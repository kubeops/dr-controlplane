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

package failovergroup

import (
	"time"

	"kubeops.dev/dr-controlplane/pkg/leases"
	appsv1 "kubeops.dev/petset/apis/apps/v1"
)

// AgreedHandoff returns the data center every member of g currently asks the
// group's primary DC Lease to move to, or "" when there is no such agreement. A
// member agrees when its report is fresh and its HandoffTo is non empty and the
// same as everyone else's. One silent, stale or disagreeing member means no
// agreement, which is what keeps an entry added to a database group (whose
// engines never set HandoffTo) from ever moving that group.
func AgreedHandoff(g *appsv1.FailoverGroup, now time.Time) string {
	if len(g.Status.Members) == 0 {
		return ""
	}
	target := g.Status.Members[0].HandoffTo
	for i := range g.Status.Members {
		m := &g.Status.Members[i]
		fresh := m.ObservedAt != nil && now.Sub(m.ObservedAt.Time) <= MemberReportTTL
		if !fresh || m.HandoffTo == "" || m.HandoffTo != target {
			return ""
		}
	}
	return target
}

// HandoffRequest returns the data center the controller may start a handoff to
// now, or "" when it must not. It is AgreedHandoff plus the conditions under
// which moving the Lease is safe to start:
//
//   - the Lease exists and its holder is alive (a dead holder is a failover, not
//     a handoff, and is handled by contention);
//   - the target is a Member DC other than the holder, and its health Lease is
//     fresh, so the Lease is never handed to a DC that cannot take it;
//   - a group with dependencies may only move to the DC where its dependencies
//     are active (dependencyTarget, Decision.Target), never anywhere else.
//
// dependencyTarget is only consulted when g has dependencies; it is "" while they
// are unsettled, which blocks every request.
func HandoffRequest(g *appsv1.FailoverGroup, lease LeaseView, dependencyTarget string, targetAlive func(dc string) bool, now time.Time) string {
	target := AgreedHandoff(g, now)
	if target == "" {
		return ""
	}
	if !lease.Exists || lease.Holder == "" || !lease.HolderAlive {
		return ""
	}
	if target == lease.Holder || !contains(lease.MemberDCs, target) || !targetAlive(target) {
		return ""
	}
	if len(g.Spec.DependsOn) > 0 && target != dependencyTarget {
		return ""
	}
	return target
}

// PlanHandoff returns the annotation patch (a nil value removes a key) that
// brings the Lease's handoff request in line with what the members ask for, or
// nil when nothing needs to change.
//
// agreed is AgreedHandoff and initiate is HandoffRequest. They differ on
// purpose. Starting a handoff needs every safety condition, but a handoff this
// controller already started must survive the moment the holder releases the
// Lease (its holder is then empty, so initiate is ""): withdrawing it there would
// let any Member race for the Lease, which is what a coordinated handoff exists
// to prevent. So a pending request of ours stays while the members still agree on
// it, and goes when they stop (abort, timeout, a stale reporter).
//
// A handoff-to is "ours" only when handoff-request names the same data center.
// Any other handoff-to (a database operator, the switchover CLI) is never
// touched. A handoff-request that does not match the handoff-to is a leftover
// (the agent cleared handoff-to when the target took the Lease) and is removed.
func PlanHandoff(lease LeaseView, agreed, initiate string) map[string]any {
	handoff, request := lease.HandoffTo, lease.HandoffRequest
	ours := request != "" && request == handoff

	patch := map[string]any{}
	switch {
	case ours:
		if agreed != handoff {
			patch[leases.AnnHandoffTo] = nil
			patch[leases.AnnHandoffRequest] = nil
		}
	case request != "":
		patch[leases.AnnHandoffRequest] = nil
	}
	if !ours && handoff == "" && initiate != "" {
		patch[leases.AnnHandoffTo] = initiate
		patch[leases.AnnHandoffRequest] = initiate
	}
	if len(patch) == 0 {
		return nil
	}
	return patch
}
