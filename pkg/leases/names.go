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

// Package leases holds the naming and annotation conventions for the Leases the
// common DC failover service publishes. These names form the contract between
// the topology controller (which ensures the Leases), the DC agent (which
// contends for them), and the client library (which reads them).
package leases

import "strings"

const (
	// DefaultNamespace is where all coordination Leases live.
	DefaultNamespace = "dc-failover"

	// GlobalPrimaryLease is the single primary DC Lease that globally scoped
	// workloads follow. Its spec.holderIdentity is the current primary data center.
	GlobalPrimaryLease = "primary-dc"

	// GroupPrimaryPrefix is prepended to a group name to form its primary DC Lease,
	// for example primary-dc-orders.
	GroupPrimaryPrefix = "primary-dc-"

	// HealthPrefix is prepended to a data center name to form its health Lease,
	// for example dc-health-a, renewed continuously by that DC's agent.
	HealthPrefix = "dc-health-"
)

const (
	// AnnMemberDCs lists the Member (primary eligible) data centers for a primary
	// DC Lease, comma separated. The topology controller sets it; agents read it to
	// decide whether their DC contends. Arbiter and Witness DCs are not listed.
	AnnMemberDCs = "dr.open-cluster-management.io/member-dcs"

	// AnnHandoffTo names the data center a coordinated failback should move the
	// Lease to. The current holder releases; non target candidates pause so the
	// target acquires. Cleared once the target holds the Lease.
	AnnHandoffTo = "dr.open-cluster-management.io/handoff-to"

	// AnnScope records the trigger scope (Global or the group name) for observability.
	AnnScope = "dr.open-cluster-management.io/scope"

	// AnnQuiesce names the data center whose primary must hold read only for a
	// planned switchover, so the target can replay to the active primary's frozen
	// LSN for zero RPO. It is the current holder. The agent projects it into the
	// marker as data.quiesce; the active DC's coordinator honors it. Cleared once
	// the switchover completes (the Lease moves and the old DC self fences anyway).
	AnnQuiesce = "dr.open-cluster-management.io/quiesce"

	// AnnOverrideHold names the data center a human has pinned via that DC's local
	// break glass override ConfigMap (see OverrideConfigMapSuffix). While set,
	// every Member DC other than the named one defers from contending for this
	// scope's primary DC Lease; the named DC's own agent holds/renews it. Written
	// and cleared ONLY by the named DC's own agent, and only ever to that agent's
	// own DC name, mirroring (never writing) the human owned override ConfigMap.
	// See the DC-DR handoff notes (prompt-library dc-dr/postgres), A42 and
	// A43(c): break glass itself stays manual only, this annotation is how the
	// pin becomes visible to DCs that cannot read another spoke's local state.
	AnnOverrideHold = "dr.open-cluster-management.io/override-hold"

	// AnnFollowDC orders a dependent FailoverGroup behind its dependencies. The
	// FailoverGroup controller sets it on the group's primary DC Lease to the data
	// center where every dependency is active and ready. A Member that is neither
	// the named DC nor the current holder does not contend, so after a DC loss the
	// group's Lease is only acquired once its dependencies have landed. The holder
	// is never forced out by it; a planned move still goes through a switchover.
	// Unset (the default, and for every group without dependencies) leaves
	// contention unchanged.
	AnnFollowDC = "dr.open-cluster-management.io/follow-dc"
)

// OverrideConfigMapSuffix names the human owned break glass override ConfigMap
// relative to a scope's marker/primary Lease name: <name>+OverrideConfigMapSuffix,
// in the same coordination namespace on the spoke that carries the marker (for
// example primary-dc-override for the global scope). It must match the
// pg-coordinator fence's dcBreakGlassOverrideSuffix byte for byte; the two
// repos read the same human managed object by convention, not shared code, so
// keep them in sync by hand if either changes.
const OverrideConfigMapSuffix = "-override"

// StandbyHoldConfigMapSuffix names the human owned standby-hold ConfigMap
// relative to a scope's marker/primary Lease name, the mirror of
// OverrideConfigMapSuffix (A44): <name>+StandbyHoldConfigMapSuffix, in the same
// coordination namespace on the spoke that carries the marker (for example
// primary-dc-standby-hold for the global scope). Presence forces that DC to
// never contend for the scope's primary DC Lease, so it never promotes, UNLESS
// that DC is the current holder (A45 correction): standby-hold is ignored,
// logged loudly, and has no effect on the active DC, since demoting it without
// a quiesce/catch-up is unsafe and the safe way to move the primary is a
// planned switchover. Manual only, same contract as break glass: a human sets
// and clears it directly on the spoke, this agent and pg-coordinator only ever
// Get it.
//
// Unlike OverrideConfigMapSuffix, standby-hold does not get a companion
// dr.open-cluster-management.io/* Lease annotation. Break glass needs one
// because every OTHER DC's contention decision depends on knowing which DC is
// pinned, and only the pinned DC's own agent can see its own spoke's local
// ConfigMap. Standby-hold's effect is purely local: only the held DC's own
// decision to contend changes, and a DC that stops contending is invisible to
// the rest of the system in exactly the same way a Member that was never
// eligible is, no other DC's desiredContend logic needs to special case it. So
// this agent reads the ConfigMap straight into its own contention decision and
// never writes anything to the shared Lease for it. It must match the
// pg-coordinator fence's mirror constant byte for byte, same convention as
// OverrideConfigMapSuffix above.
const StandbyHoldConfigMapSuffix = "-standby-hold"

const (
	// LabelManagedBy marks the Leases this service owns.
	LabelManagedBy = "app.kubernetes.io/managed-by"
	// ValueManagedBy is the LabelManagedBy value for this service.
	ValueManagedBy = "dr-controlplane"
)

// Scope selects a primary DC Lease. An empty Group means the global scope.
type Scope struct {
	Group string
}

// GlobalScope is the global trigger scope.
var GlobalScope = Scope{}

// GroupScope returns the scope for a named workload group.
func GroupScope(group string) Scope {
	return Scope{Group: group}
}

// IsGlobal reports whether s is the global scope.
func (s Scope) IsGlobal() bool {
	return s.Group == ""
}

// PrimaryLeaseName returns the Lease name that backs this scope.
func (s Scope) PrimaryLeaseName() string {
	if s.IsGlobal() {
		return GlobalPrimaryLease
	}
	return GroupPrimaryPrefix + s.Group
}

// String renders the scope for logs and the CLI.
func (s Scope) String() string {
	if s.IsGlobal() {
		return "global"
	}
	return "group/" + s.Group
}

// HealthLeaseName returns the health Lease name for a data center.
func HealthLeaseName(dc string) string {
	return HealthPrefix + dc
}

// ScopeFromPrimaryLeaseName recovers the Scope from a primary DC Lease name.
// It returns false for names that are not primary DC Leases (for example health Leases).
func ScopeFromPrimaryLeaseName(name string) (Scope, bool) {
	if name == GlobalPrimaryLease {
		return GlobalScope, true
	}
	if strings.HasPrefix(name, GroupPrimaryPrefix) {
		return GroupScope(strings.TrimPrefix(name, GroupPrimaryPrefix)), true
	}
	return Scope{}, false
}

// IsPrimaryLeaseName reports whether name is a primary DC Lease (global or group),
// as opposed to a health Lease.
func IsPrimaryLeaseName(name string) bool {
	_, ok := ScopeFromPrimaryLeaseName(name)
	return ok
}
