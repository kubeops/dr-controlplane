/*
Copyright AppsCode Inc. and Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package agent

import (
	"context"
	"encoding/json"

	"kubeops.dev/dr-controlplane/pkg/leases"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

// Break glass reconvergence (A43(c)).
//
// A42 made break glass manual only: a human sets and clears the
// <marker>-override ConfigMap directly on the surviving primary spoke, and no
// coordinator or operator code path ever writes or deletes it (pg-coordinator's
// pkg/dc_fence.go breakGlassOverrideActive is Get only; this file matches that
// contract, see localOverrideActive).
//
// This agent closes the other half: while that ConfigMap is present on ITS OWN
// spoke, it reflects the pin onto the SHARED primary DC Lease as the
// dr.open-cluster-management.io/override-hold annotation, so every OTHER DC's
// agent can see the pin even though it has no access to another spoke's local
// ConfigMap. The agent still never writes the human's ConfigMap, only Gets it;
// the Lease annotation is a fact this agent reports about it, not a copy of it.
//
// Sequencing (the crux, see the design note in prompt-library
// dc-dr/postgres/from-agent.md N165): while hub/etcd quorum is down this agent
// cannot reach the coordination control plane at all, the same as every other
// write path during the outage, so it cannot set the annotation DURING the
// outage. Its real job is a first mover write the moment quorum returns:
// reconcileOverrides runs on the same fast ticker as the marker projector
// (MarkerRefreshInterval, 5s default) and, the instant this agent can reach the
// hub again, reasserts override-hold. This does not by itself remove the low
// level race against a returning standby's own leaderelection Acquire call:
// both merely become possible again at the same moment, quorum's return, and
// there is no server side ordering between them. What bounds the race is that
// any agent which OBSERVES override-hold set to another DC defers
// unconditionally (desiredContend in handoff.go): the exposure window is
// "before the annotation lands", not "forever". Fully closing that window is
// deferred to A43's two DC quorum rebuild live test phase with a real 3 member
// etcd; it is not attempted here.
func (a *Agent) reconcileOverrides(ctx context.Context) {
	a.mu.Lock()
	snapshot := make(map[string]markerState, len(a.holders))
	for k, v := range a.holders {
		snapshot[k] = v
	}
	a.mu.Unlock()

	for name, st := range snapshot {
		active := a.localOverrideActive(ctx, name)
		a.reconcileOverrideHold(ctx, name, st.dc, st.overrideHold, active)
	}
}

// localOverrideActive reports whether this DC's own spoke carries the break
// glass override ConfigMap for scope leaseName. Get only (through the writer
// manager's cache), matching pg-coordinator's breakGlassOverrideActive byte for
// byte in naming (leases.OverrideConfigMapSuffix): presence is the signal, any
// error (NotFound or otherwise) fails closed to "not active".
func (a *Agent) localOverrideActive(ctx context.Context, leaseName string) bool {
	name := leaseName + leases.OverrideConfigMapSuffix
	var cm core.ConfigMap
	err := a.spoke.Get(ctx, types.NamespacedName{Namespace: a.opts.MarkerNamespace, Name: name}, &cm)
	return err == nil
}

// overrideDecision is the pure decision reconcileOverrideHold applies: given
// whether this DC's local override ConfigMap is active, the scope's last
// observed Lease holder, this DC's own name, and the override-hold value
// currently on the Lease, decide the value the annotation should have and
// whether a write is needed.
//
// Gating rule: this agent only ever sets override-hold to ITS OWN name, and
// only when it is also the scope's last known Lease holder. The override
// ConfigMap is meant to live solely on the DC that was primary before quorum
// was lost, so in the intended flow that check always passes; it exists purely
// as a defense against a human placing the ConfigMap on the wrong spoke, so a
// DC that never held the Lease can never hijack it for itself.
//
// This agent also only ever CLEARS a value it could have set (current == dc):
// it never touches another DC's pin, and never writes anything when there was
// nothing to change.
func overrideDecision(active bool, lastHolder, dc, current string) (want string, changed bool) {
	if active {
		if lastHolder != dc {
			return current, false
		}
		return dc, current != dc
	}
	if current != dc {
		return current, false
	}
	return "", true
}

// reconcileOverrideHold applies overrideDecision against the OBSERVED
// override-hold annotation (carried in markerState by the same informer feed
// the holders come from), patching the Lease only on a change. Deciding from
// the observed value instead of a per scope uncached hub Get keeps this
// reconciler's read load at zero; the worst a stale observation can cause is a
// redundant idempotent merge patch, which the next observation corrects. This
// is this agent's ONLY write path to the annotation; the human's override
// ConfigMap itself is never written (see localOverrideActive, Get only).
func (a *Agent) reconcileOverrideHold(ctx context.Context, leaseName, lastHolder, have string, active bool) {
	if active && lastHolder != a.opts.DCName {
		klog.ErrorS(nil, "DC-DR BREAK GLASS override ConfigMap found on a spoke that is not this scope's last known primary DC; ignoring, refusing to pin the Lease to a DC that never held it",
			"dcdr.dc", a.opts.DCName, "dcdr.scope", leaseName, "dcdr.lastKnownHolder", lastHolder)
		return
	}

	want, changed := overrideDecision(active, lastHolder, a.opts.DCName, have)
	if !changed {
		return
	}
	if want == "" {
		klog.InfoS("break glass override ConfigMap cleared, dropping override-hold pin, normal contention resumes",
			"dcdr.dc", a.opts.DCName, "dcdr.scope", leaseName)
	} else {
		klog.ErrorS(nil, "DC-DR BREAK GLASS OVERRIDE ACTIVE: pinning primary DC Lease to this DC until a human clears the override ConfigMap",
			"dcdr.dc", a.opts.DCName, "dcdr.scope", leaseName)
	}
	a.patchOverrideHold(ctx, leaseName, want)
}

// patchOverrideHold sets the override-hold annotation to value, or removes it
// when value is empty. Uses the same coordination plane client and merge patch
// shape as clearHandoff in handoff.go.
func (a *Agent) patchOverrideHold(ctx context.Context, leaseName, value string) {
	var ann any
	if value != "" {
		ann = value
	}
	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				leases.AnnOverrideHold: ann,
			},
		},
	}
	raw, err := json.Marshal(patch)
	if err != nil {
		return
	}
	if _, err := a.cs.CoordinationV1().Leases(a.opts.Namespace).
		Patch(ctx, leaseName, types.MergePatchType, raw, metav1.PatchOptions{}); err != nil {
		klog.V(3).ErrorS(err, "failed to reconcile override-hold annotation", "dcdr.scope", leaseName, "dcdr.dc", a.opts.DCName)
	}
}
