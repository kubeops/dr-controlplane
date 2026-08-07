/*
Copyright AppsCode Inc. and Contributors

Licensed under the AppsCode Free Trial License 1.0.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://github.com/appscode/licenses/raw/1.0.0/AppsCode-Free-Trial-1.0.0.md

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
	"time"

	"open-cluster-management.io/dr-controlplane/pkg/leases"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

// desiredContend decides whether this DC should actively contend for a scope's
// primary DC Lease right now, given its membership, any in progress coordinated
// handoff, any break glass override-hold pin, and any local standby-hold veto.
// handoffTargetIsMember reports whether handoffTo names a Member data center
// (one that can actually become primary). overrideHold is
// leases.AnnOverrideHold's value, empty when unset. standbyHold reports
// whether THIS DC's own spoke currently carries its standby-hold ConfigMap
// (leases.StandbyHoldConfigMapSuffix); unlike overrideHold this is never read
// off the Lease, see standbyhold.go for why.
//
//   - Arbiter and Witness DCs (not members) never contend.
//   - Standby-hold (A44, corrected by A45) is an absolute veto on THIS DC
//     alone, UNLESS this DC is the scope's current Lease holder (holder == dc,
//     the same equality the handoff logic below already uses to mean "this DC
//     currently holds the Lease"). On a non-holder DC, while its own
//     standby-hold ConfigMap is present it never contends, full stop: this is
//     checked before everything else, including override-hold, so that if a
//     human somehow sets both markers on a DC that is not the current holder,
//     standby-hold wins, fail safe means staying a non-promoting standby, not
//     forcing active. On the CURRENT HOLDER, standby-hold is ignored (a no-op)
//     and logged loudly (see reconcileStandbyHold): it must never stop the
//     active DC from contending or let it drop the Lease, since demoting the
//     active DC without a quiesce/catch-up is unsafe, and stripping the sole
//     role=primary pod breaks the primary Service DNS every pod depends on
//     (the N172 finding). To move the primary, use a planned switchover
//     (dr.kubedb.com/switchover-to) instead. When ignored this way, evaluation
//     falls through to the override-hold and handoff checks below exactly as
//     if standby-hold were unset.
//   - A break glass override-hold pin (A43(c)) is otherwise an absolute veto:
//     every Member other than the named DC defers unconditionally, regardless
//     of handoff state or anything else. The named DC contends (this is how it
//     holds/renews the Lease through the pin), unless standby-hold above already
//     vetoed it.
//   - Normally (no pin, no hold) a Member contends.
//   - During a handoff to another Member, a Member pauses so the target can
//     acquire. If this DC currently holds the Lease, the reconcile additionally
//     releases it once via releaseForHandoff (elector stops never release on
//     their own, see election.go).
//   - The handoff target contends eagerly. Once the target holds the Lease the
//     handoff is complete and everyone resumes normal contention.
//   - A handoff whose target is not a Member (a stale annotation, or a Member
//     removed from the set mid handoff) is ignored. Otherwise every Member would
//     pause for a target that can never acquire, leaving the scope with no
//     primary at all.
func desiredContend(isMember bool, handoffTo, holder, dc string, handoffTargetIsMember bool, overrideHold string, standbyHold bool) bool {
	if !isMember {
		return false
	}
	if standbyHold && holder != dc {
		return false
	}
	if overrideHold != "" {
		return overrideHold == dc
	}
	if handoffTo == "" || !handoffTargetIsMember || handoffTo == dc {
		return true
	}
	if holder == handoffTo {
		return true
	}
	return false
}

func holderOf(l *coordinationv1.Lease) string {
	if l == nil || l.Spec.HolderIdentity == nil {
		return ""
	}
	return *l.Spec.HolderIdentity
}

// releaseForHandoff steps this DC aside for a coordinated handoff by writing the
// Lease back with no holder and a one second duration, the same record client-go's
// release writes, so the handoff target acquires on its next retry tick instead of
// waiting out the full lease duration. This is deliberately the ONLY active Lease
// move in the whole service; every elector runs with ReleaseOnCancel disabled (see
// election.go for the incident that forced this). The caller has already paused
// this DC's elector, but a renew that was in flight when the elector stopped can
// still land after this write and re-claim the Lease; that renew is itself a Lease
// event, the reconcile re-runs, sees the handoff annotation still set and this DC
// still the holder, and releases again, so the handoff converges within one event.
func (a *Agent) releaseForHandoff(ctx context.Context, name, handoffTo string) {
	cl := a.cs.CoordinationV1().Leases(a.opts.Namespace)
	cur, err := cl.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		klog.V(3).ErrorS(err, "failed to read Lease for handoff release", "dcdr.scope", name)
		return
	}
	if holderOf(cur) != a.opts.DCName {
		return
	}
	now := metav1.NewMicroTime(time.Now())
	cur.Spec.HolderIdentity = ptr.To("")
	cur.Spec.LeaseDurationSeconds = ptr.To(int32(1))
	cur.Spec.RenewTime = &now
	cur.Spec.AcquireTime = &now
	if _, err := cl.Update(ctx, cur, metav1.UpdateOptions{}); err != nil {
		klog.V(3).ErrorS(err, "failed to release Lease for handoff", "dcdr.scope", name)
		return
	}
	klog.InfoS("released primary DC Lease for coordinated handoff", "dcdr.scope", name, "dcdr.dc", a.opts.DCName, "dcdr.handoffTo", handoffTo)
}

// clearHandoff removes the handoff annotation once the target holds the Lease.
func (a *Agent) clearHandoff(ctx context.Context, name string) {
	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				leases.AnnHandoffTo: nil,
			},
		},
	}
	raw, err := json.Marshal(patch)
	if err != nil {
		return
	}
	if _, err := a.cs.CoordinationV1().Leases(a.opts.Namespace).
		Patch(ctx, name, types.MergePatchType, raw, metav1.PatchOptions{}); err != nil {
		klog.V(3).ErrorS(err, "failed to clear handoff annotation", "dcdr.scope", name)
		return
	}
	klog.InfoS("coordinated handoff complete, cleared handoff annotation", "dcdr.scope", name, "dcdr.dc", a.opts.DCName)
}
