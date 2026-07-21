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

	"open-cluster-management.io/dr-controlplane/pkg/leases"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// Standby-hold (A44), the mirror of break glass override (A43(c), override.go).
//
// A human places a <marker>-standby-hold ConfigMap on a spoke to force that DC
// to never contend for the scope's primary DC Lease, so it never promotes,
// while the marker exists. Manual only, same contract as break glass: this
// agent only ever Gets the ConfigMap, never writes or deletes it
// (localStandbyHoldActive matches localOverrideActive byte for byte in shape).
//
// The key design difference from override-hold: standby-hold's effect is
// purely local. desiredContend's overrideHold veto has to travel over the
// Lease as an annotation because every OTHER Member's contention decision
// depends on knowing which DC is pinned, and no DC can read another spoke's
// local ConfigMap. Standby-hold needs no such trip: only the held DC's own
// decision to contend changes, and a DC that stops contending is, from every
// other DC's point of view, indistinguishable from a Member that simply never
// tries. So there is no leases.Ann* constant and no Lease write for this
// feature at all, see leases.StandbyHoldConfigMapSuffix's doc comment for the
// same reasoning. The cache below (Agent.standbyHold) exists only so this
// DC's own reconcile() (agent.go, driven by Lease informer events) can read a
// fresh local answer without doing a spoke Get on every single Lease event;
// it is refreshed on the same fast ticker as the marker projector and the
// override reconciler (runProjector, MarkerRefreshInterval, 5s default).
//
// Interaction with break glass on the SAME DC: if a human sets both markers on
// one DC, they conflict (one demands active, the other demands standby).
// desiredContend resolves it deterministically: standby-hold is checked first
// and wins unconditionally (fail safe: stay a non-promoting standby). This
// file additionally detects and loudly logs the coexistence every tick it
// persists (standbyHoldConflict), the same "log every tick while the anomaly
// persists" treatment reconcileOverrideHold already gives an override
// ConfigMap placed on the wrong spoke. dr-controlplane does not own any
// Postgres (or other workload) CR status, so there is no natural Condition
// surface to set from here; a loud structured log line at the point of
// detection is the deliberate, documented minimum for this repo, matching the
// A44 instruction to use a log line when no Condition surface exists.

// localStandbyHoldActive reports whether this DC's own spoke carries the
// standby-hold ConfigMap for scope leaseName. Get only, matching
// localOverrideActive: presence is the signal, any error (NotFound or
// otherwise) fails closed to "not active".
func (a *Agent) localStandbyHoldActive(ctx context.Context, spoke kubernetes.Interface, leaseName string) bool {
	name := leaseName + leases.StandbyHoldConfigMapSuffix
	_, err := spoke.CoreV1().ConfigMaps(a.opts.MarkerNamespace).Get(ctx, name, metav1.GetOptions{})
	return err == nil
}

// standbyHoldTransitionLog is the pure decision behind reconcileStandbyHold's
// state-change logging: given whether standby-hold was active last tick, is
// active now, and whether this DC is the scope's last known Lease holder,
// decide which single state-change log (if any) to emit this tick. Mirrors
// overrideDecision's shape: no I/O, easy to table test.
//
// stepDown fires exactly once, on the tick standby-hold newly becomes active
// while this DC is the current holder: this is the controlled demotion case,
// and it must be loud and unambiguous, distinct from the elector's own
// generic "paused/stopped contending" log, since a human needs to be able to
// grep for why a DC that used to be primary is not primary anymore.
// held fires once when standby-hold newly becomes active on a DC that is not
// currently the holder, the ordinary "stays a non-promoting standby" case.
// cleared fires once when standby-hold goes away, either case.
func standbyHoldTransitionLog(wasActive, active, isHolder bool) (stepDown, held, cleared bool) {
	switch {
	case active && !wasActive && isHolder:
		return true, false, false
	case active && !wasActive:
		return false, true, false
	case !active && wasActive:
		return false, false, true
	default:
		return false, false, false
	}
}

// standbyHoldConflict reports whether both standby-hold and break glass
// override are simultaneously active on this DC for the same scope, the
// precedence case desiredContend resolves by letting standby-hold win. Pure
// function of the two observed booleans, called every tick so the conflict
// stays loud for as long as it persists (see the wrong-spoke override log in
// reconcileOverrideHold for the same "log every tick, not just on
// transition" treatment of an anomalous, human-fixable state).
func standbyHoldConflict(standbyActive, overrideActive bool) bool {
	return standbyActive && overrideActive
}

// reconcileStandbyHold refreshes this DC's cached standby-hold state for
// every scope this agent currently tracks, and emits the state-change and
// conflict logs standbyHoldTransitionLog and standbyHoldConflict decide on.
// It never writes anything, to the spoke ConfigMap or to the Lease; the
// actual behavior change (this DC stops contending, and if it was the
// holder, releases the Lease) happens the ordinary way, the next time
// reconcile (agent.go) runs off a Lease informer event and calls
// desiredContend with the cached value this function just wrote.
func (a *Agent) reconcileStandbyHold(ctx context.Context, spoke kubernetes.Interface) {
	a.mu.Lock()
	snapshot := make(map[string]markerState, len(a.holders))
	for k, v := range a.holders {
		snapshot[k] = v
	}
	a.mu.Unlock()

	for name, st := range snapshot {
		active := a.localStandbyHoldActive(ctx, spoke, name)
		overrideActive := a.localOverrideActive(ctx, spoke, name)

		a.mu.Lock()
		wasActive := a.standbyHold[name]
		a.standbyHold[name] = active
		a.mu.Unlock()

		isHolder := st.dc == a.opts.DCName
		stepDown, held, cleared := standbyHoldTransitionLog(wasActive, active, isHolder)
		switch {
		case stepDown:
			klog.ErrorS(nil, "DC-DR STANDBY-HOLD ACTIVE on the current primary DC: this DC stops contending and will release the primary DC Lease, a controlled step-down, another Member DC will be promoted",
				"dcdr.dc", a.opts.DCName, "dcdr.scope", name)
		case held:
			klog.InfoS("DC-DR standby-hold ConfigMap present, this DC will not contend for the primary DC Lease while it exists",
				"dcdr.dc", a.opts.DCName, "dcdr.scope", name)
		case cleared:
			klog.InfoS("DC-DR standby-hold ConfigMap cleared, normal contention resumes",
				"dcdr.dc", a.opts.DCName, "dcdr.scope", name)
		}

		if standbyHoldConflict(active, overrideActive) {
			klog.ErrorS(nil, "DC-DR CONFLICT: both break glass override and standby-hold ConfigMaps are present on this DC for the same scope, they demand opposite outcomes, standby-hold takes precedence (fail safe: stay standby, never promote), a human must clear one of the two markers to resolve this",
				"dcdr.dc", a.opts.DCName, "dcdr.scope", name)
		}
	}
}
