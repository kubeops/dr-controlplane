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

	"github.com/kluster-manager/dr-controlplane/pkg/leases"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

// Standby-hold (A44, corrected by A45), the mirror of break glass override
// (A43(c), override.go).
//
// A human places a <marker>-standby-hold ConfigMap on a spoke to force that DC
// to never contend for the scope's primary DC Lease, so it never promotes,
// while the marker exists. Manual only, same contract as break glass: this
// agent only ever Gets the ConfigMap, never writes or deletes it
// (localStandbyHoldActive matches localOverrideActive byte for byte in shape).
//
// A45 correction: standby-hold must never demote or fence the CURRENT ACTIVE
// DC. A44's original build treated standby-hold on the current holder as a
// controlled step-down (release the Lease, another Member promotes); live
// testing (N172) showed this is unsafe and redundant, it drops the Lease with
// no quiesce/catch-up, and it strips the sole role=primary pod, which breaks
// the primary Service DNS every pod depends on and does not self heal
// promptly. Standby-hold's only legitimate target is a NON-active DC
// (reconvergence: a returning standby that must not race for the Lease, or a
// standby parked so it will not auto-promote). Applied to the current holder,
// it is now a logged no-op: desiredContend (handoff.go) ignores it whenever
// holder == dc, and this file logs the ignored case loudly, every tick it
// persists, so a human who set it on the wrong DC notices. To move the
// primary, the runbook is a planned switchover (dr.kubedb.com/switchover-to).
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
// Interaction with break glass on a NON-holder DC: if a human sets both
// markers on the same DC and that DC is not the current holder, they conflict
// (one demands active, the other demands standby). desiredContend resolves it
// deterministically: standby-hold is checked first and wins unconditionally
// (fail safe: stay a non-promoting standby). This file additionally detects
// and loudly logs the coexistence every tick it persists (standbyHoldConflict),
// the same "log every tick while the anomaly persists" treatment
// reconcileOverrideHold already gives an override ConfigMap placed on the
// wrong spoke. On the CURRENT HOLDER there is no such conflict to resolve:
// standby-hold is ignored outright (A45), so whatever override-hold says is
// what governs; standbyHoldConflict is gated to the non-holder case so its
// "standby-hold wins" message is never logged somewhere it would be false.
// dr-controlplane does not own any Postgres (or other workload) CR status, so
// there is no natural Condition surface to set from here; a loud structured
// log line at the point of detection is the deliberate, documented minimum
// for this repo, matching the A44 instruction to use a log line when no
// Condition surface exists.

// localStandbyHoldActive reports whether this DC's own spoke carries the
// standby-hold ConfigMap for scope leaseName. Get only (through the writer
// manager's cache), matching localOverrideActive: presence is the signal, any
// error (NotFound or otherwise) fails closed to "not active".
func (a *Agent) localStandbyHoldActive(ctx context.Context, leaseName string) bool {
	name := leaseName + leases.StandbyHoldConfigMapSuffix
	var cm core.ConfigMap
	err := a.spoke.Get(ctx, types.NamespacedName{Namespace: a.opts.MarkerNamespace, Name: name}, &cm)
	return err == nil
}

// standbyHoldTransitionLog is the pure decision behind reconcileStandbyHold's
// state-change logging: given whether standby-hold was active last tick, is
// active now, and whether this DC is the scope's last known Lease holder,
// decide which single state-change log (if any) to emit this tick. Mirrors
// overrideDecision's shape: no I/O, easy to table test.
//
// held fires once when standby-hold newly becomes active on a DC that is not
// currently the holder, the ordinary "stays a non-promoting standby" case,
// the only case where standby-hold actually takes effect. On the current
// holder, standby-hold never takes effect (A45), so no transition fires here
// for that case, standbyHoldIgnoredOnActive below covers it with its own,
// persistent, log instead of a one-shot transition log.
// cleared fires once when standby-hold goes away, either case: it is safe to
// state plainly regardless of whether the DC was the (unaffected) holder or
// an actually-held non-holder.
func standbyHoldTransitionLog(wasActive, active, isHolder bool) (held, cleared bool) {
	switch {
	case active && !wasActive && !isHolder:
		return true, false
	case !active && wasActive:
		return false, true
	default:
		return false, false
	}
}

// standbyHoldIgnoredOnActive reports whether standby-hold is active on this
// DC while this DC is also the scope's current Lease holder, the A45
// corrected no-op case: desiredContend (handoff.go) ignores standby-hold
// entirely on the active DC, it never demotes or fences it. This predicate
// drives the loud log that makes the no-op visible, called every tick it
// persists (not just once on transition), the same "log every tick while the
// anomaly persists" treatment standbyHoldConflict already gets, since a human
// who set the marker on the wrong (active) DC needs to keep seeing why
// nothing is happening for as long as they leave it there.
func standbyHoldIgnoredOnActive(active, isHolder bool) bool {
	return active && isHolder
}

// standbyHoldConflict reports whether both standby-hold and break glass
// override are simultaneously active on this DC for the same scope AND this
// DC is not the current holder, the precedence case desiredContend resolves
// by letting standby-hold win. Gated to the non-holder case (A45): on the
// current holder standby-hold is ignored outright, so there is no real
// "standby-hold wins" conflict to report, standbyHoldIgnoredOnActive covers
// that case instead. Pure function of the observed booleans, called every
// tick so the conflict stays loud for as long as it persists (see the
// wrong-spoke override log in reconcileOverrideHold for the same "log every
// tick, not just on transition" treatment of an anomalous, human-fixable
// state).
func standbyHoldConflict(standbyActive, overrideActive, isHolder bool) bool {
	return standbyActive && overrideActive && !isHolder
}

// reconcileStandbyHold refreshes this DC's cached standby-hold state for
// every scope this agent currently tracks, and emits the state-change,
// ignored-on-active, and conflict logs standbyHoldTransitionLog,
// standbyHoldIgnoredOnActive, and standbyHoldConflict decide on. It never
// writes to the spoke ConfigMap or to the Lease.
//
// On a TRANSITION it re-runs the scope's contention decision immediately,
// against a freshly read Lease. Without that, the new cached value only took
// effect the next time a Lease informer event happened to arrive, which is not
// a bounded wait: a scope whose Lease is idle (nobody renewing, as right after
// a coordinated handoff released it) delivers no events until the informer's
// 10 minute resync. Observed live: clearing a standby-hold on a handoff TARGET
// left that DC not contending while the Lease sat unheld with handoff-to still
// naming it, so the handoff simply never completed and the scope had no
// primary at all. Re-poking a Lease from outside does not reliably fix it
// either, since an event that lands before this cache refresh is evaluated
// against the STALE value, which is exactly the race that made the live repro
// look intermittent. Deciding here, right where the value changes, removes the
// ordering dependency entirely.
func (a *Agent) reconcileStandbyHold(ctx context.Context) {
	a.mu.Lock()
	snapshot := make(map[string]markerState, len(a.holders))
	for k, v := range a.holders {
		snapshot[k] = v
	}
	a.mu.Unlock()

	for name, st := range snapshot {
		active := a.localStandbyHoldActive(ctx, name)
		overrideActive := a.localOverrideActive(ctx, name)

		a.mu.Lock()
		wasActive := a.standbyHold[name]
		a.standbyHold[name] = active
		a.mu.Unlock()

		isHolder := st.dc == a.opts.DCName
		held, cleared := standbyHoldTransitionLog(wasActive, active, isHolder)
		switch {
		case held:
			klog.InfoS("DC-DR standby-hold ConfigMap present, this DC will not contend for the primary DC Lease while it exists",
				"dcdr.dc", a.opts.DCName, "dcdr.scope", name)
		case cleared:
			klog.InfoS("DC-DR standby-hold ConfigMap cleared",
				"dcdr.dc", a.opts.DCName, "dcdr.scope", name)
		}

		if standbyHoldIgnoredOnActive(active, isHolder) {
			klog.ErrorS(nil, "standby-hold ignored on the active DC; to move the primary use a planned switchover (dr.kubedb.com/switchover-to)",
				"dcdr.dc", a.opts.DCName, "dcdr.scope", name)
		}

		if standbyHoldConflict(active, overrideActive, isHolder) {
			klog.ErrorS(nil, "DC-DR CONFLICT: both break glass override and standby-hold ConfigMaps are present on this DC for the same scope, they demand opposite outcomes, standby-hold takes precedence (fail safe: stay standby, never promote), a human must clear one of the two markers to resolve this",
				"dcdr.dc", a.opts.DCName, "dcdr.scope", name)
		}

		// Act on the change now, do not wait for a Lease event that may never come.
		if held || cleared {
			a.reevaluateContention(ctx, name)
		}
	}
}

// reevaluateContention re-reads the scope's Lease and re-runs the ordinary
// reconcile against it, so a locally observed change (a standby-hold appearing or
// disappearing) takes effect without depending on an inbound informer event.
//
// It deliberately reuses reconcile rather than poking the elector directly: every
// other input (membership, handoff target, override-hold, the holder itself) must
// be re-read at the same moment, and reconcile is the single place that combines
// them. A failed Get is simply skipped; the next tick retries, and the pre-existing
// event path still applies.
func (a *Agent) reevaluateContention(ctx context.Context, leaseName string) {
	l, err := a.cs.CoordinationV1().Leases(a.opts.Namespace).Get(ctx, leaseName, metav1.GetOptions{})
	if err != nil {
		klog.V(3).ErrorS(err, "standby-hold changed but the Lease could not be re-read; contention will be re-evaluated on the next event or tick",
			"dcdr.dc", a.opts.DCName, "dcdr.scope", leaseName)
		return
	}
	a.reconcile(l)
}
