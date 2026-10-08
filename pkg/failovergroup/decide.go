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

// Package failovergroup publishes where each FailoverGroup is active and orders
// dependent groups behind their dependencies.
//
// The primary DC Lease still decides failover. This controller only:
//   - publishes status.activeDC once every member of the group reports ready on
//     the DC its Lease (or, for a dependent group, its dependencies) points at;
//   - sets leases.AnnFollowDC on a dependent group's Lease, so after a DC loss the
//     agents only acquire it once the dependencies have landed;
//   - asks the members of a dependent group to switch over (the engine's
//     dr.kubedb.com/switchover-to annotation) when its dependencies moved on a
//     planned switchover and its own, still healthy, holder has to follow.
package failovergroup

import (
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "kubeops.dev/petset/apis/apps/v1"
)

// MemberReportTTL is how long a member's readiness report stays usable. Engines
// re-report on every hub pass, so a report older than this means the reporter
// stopped observing, and it is not treated as ready.
const MemberReportTTL = 5 * time.Minute

// LeaseView is the part of a group's primary DC Lease the decision needs.
type LeaseView struct {
	// Exists is false when the Lease has not been created yet.
	Exists bool
	// Holder is spec.holderIdentity.
	Holder string
	// HolderAlive is true when the Lease is unexpired AND the holder DC's health
	// Lease is fresh, i.e. the holder is still running. Used to tell a planned
	// move (holder alive, it has to be switched over) from a DC loss (the Lease
	// is left to expire and follow-dc decides who acquires it).
	HolderAlive bool
	// MemberDCs are the primary eligible DCs of the Lease.
	MemberDCs []string
	// FollowDC is the current leases.AnnFollowDC value.
	FollowDC string
}

// Decision is the outcome for one group.
type Decision struct {
	Phase   appsv1.FailoverGroupPhase
	Message string
	// Target is the DC the group should be active on, empty when unknown.
	Target string
	// ActiveDC is the DC to publish; it only changes once the group is ready on Target.
	ActiveDC string
	// FollowDC is the desired leases.AnnFollowDC value; empty removes it. Only a
	// Blocked group, or one without dependencies, clears it: a group waiting for
	// its dependencies keeps the current value so it stays held back.
	FollowDC string
	// SwitchoverTo, when set, asks every member to switch over to this DC.
	SwitchoverTo string
}

// Decide computes the decision for group g. groups holds every FailoverGroup by
// name (for dependency lookup), lease is g's own Lease.
func Decide(g *appsv1.FailoverGroup, groups map[string]*appsv1.FailoverGroup, lease LeaseView, now time.Time) Decision {
	d := Decision{ActiveDC: g.Status.ActiveDC}

	if len(g.Spec.DependsOn) == 0 {
		if !lease.Exists || lease.Holder == "" {
			d.Phase = appsv1.FailoverGroupPhaseWaitingForMembers
			d.Message = "the group's primary DC Lease has no holder yet"
			return d
		}
		d.Target = lease.Holder
	} else {
		if cycle := findCycle(g.Name, groups); cycle != "" {
			d.Phase = appsv1.FailoverGroupPhaseBlocked
			d.Message = "dependency cycle: " + cycle
			return d
		}
		target := ""
		for _, dep := range g.Spec.DependsOn {
			dg, ok := groups[dep]
			if !ok {
				d.Phase = appsv1.FailoverGroupPhaseBlocked
				d.Message = fmt.Sprintf("dependency %q does not exist", dep)
				return d
			}
			if dg.Status.ActiveDC == "" {
				d.Phase = appsv1.FailoverGroupPhaseWaitingForDependency
				d.Message = fmt.Sprintf("dependency %q is not active on any data center yet", dep)
				d.FollowDC = lease.FollowDC
				return d
			}
			if target != "" && dg.Status.ActiveDC != target {
				d.Phase = appsv1.FailoverGroupPhaseWaitingForDependency
				d.Message = fmt.Sprintf("dependencies are active on different data centers (%s and %s)", target, dg.Status.ActiveDC)
				d.FollowDC = lease.FollowDC
				return d
			}
			target = dg.Status.ActiveDC
		}
		if lease.Exists && !contains(lease.MemberDCs, target) {
			d.Phase = appsv1.FailoverGroupPhaseBlocked
			d.Message = fmt.Sprintf("dependencies are active on %s, which is not a Member data center of this group (members: %s)", target, strings.Join(lease.MemberDCs, ","))
			return d
		}
		d.Target = target
		d.FollowDC = target
		// Only a group that is serving on its live holder is switched over. A group
		// that is still provisioning, or not ready on the holder, is left to become
		// ready first; follow-dc already keeps other DCs from acquiring its Lease.
		if lease.Exists && lease.Holder != "" && lease.Holder != target && lease.HolderAlive &&
			len(g.Status.Members) > 0 && len(membersNotReady(g.Status.Members, lease.Holder, now)) == 0 {
			d.SwitchoverTo = target
		}
	}

	if lease.Holder != d.Target {
		d.Phase = appsv1.FailoverGroupPhaseWaitingForMembers
		d.Message = fmt.Sprintf("waiting for the group's Lease to move from %q to %s", lease.Holder, d.Target)
		return d
	}
	// A group whose members have not reported yet is not ready: the engine
	// operator may simply not have observed them yet, and publishing activeDC then
	// would release dependent groups before anything is serving.
	if len(g.Status.Members) == 0 {
		d.Phase = appsv1.FailoverGroupPhaseWaitingForMembers
		d.Message = fmt.Sprintf("no member has reported readiness on %s yet", d.Target)
		return d
	}
	if notReady := membersNotReady(g.Status.Members, d.Target, now); len(notReady) > 0 {
		d.Phase = appsv1.FailoverGroupPhaseWaitingForMembers
		d.Message = fmt.Sprintf("members not yet ready on %s: %s", d.Target, strings.Join(notReady, ", "))
		return d
	}
	d.ActiveDC = d.Target
	d.Phase = appsv1.FailoverGroupPhaseSteady
	d.Message = fmt.Sprintf("every member is ready on %s", d.Target)
	return d
}

// membersNotReady lists the members that have not freshly reported ready on dc.
func membersNotReady(members []appsv1.FailoverGroupMember, dc string, now time.Time) []string {
	var out []string
	for i := range members {
		m := &members[i]
		fresh := m.ObservedAt != nil && now.Sub(m.ObservedAt.Time) <= MemberReportTTL
		if m.DC != dc || !m.Ready || !fresh {
			out = append(out, fmt.Sprintf("%s/%s/%s", m.Kind, m.Namespace, m.Name))
		}
	}
	sort.Strings(out)
	return out
}

// findCycle returns a rendered dependency cycle through start, or "".
func findCycle(start string, groups map[string]*appsv1.FailoverGroup) string {
	var path []string
	onPath := map[string]bool{}
	var visit func(name string) string
	visit = func(name string) string {
		if onPath[name] {
			return strings.Join(append(path, name), " -> ")
		}
		g, ok := groups[name]
		if !ok {
			return ""
		}
		onPath[name] = true
		path = append(path, name)
		for _, dep := range g.Spec.DependsOn {
			if c := visit(dep); c != "" {
				return c
			}
		}
		path = path[:len(path)-1]
		onPath[name] = false
		return ""
	}
	return visit(start)
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
