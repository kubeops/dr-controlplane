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

// Package topology derives the DC failover topology from the workloads'
// PlacementPolicy objects: for each trigger scope (the global one plus each named
// group) it computes which data centers are Members (primary DC Lease candidates)
// and which are Arbiters or Witnesses.
package topology

import (
	"fmt"
	"sort"

	"github.com/kluster-manager/dr-controlplane/pkg/leases"

	appsv1 "kubeops.dev/petset/apis/apps/v1"
)

// ScopeTopology is the resolved set of data centers for one trigger scope.
type ScopeTopology struct {
	Scope     leases.Scope
	Members   []string // primary eligible, the Lease candidates
	Arbiters  []string // vote only, no data, never primary
	Witnesses []string // data bearing, never primary
}

// AllClusters returns every data center participating in the scope.
func (st *ScopeTopology) AllClusters() []string {
	out := append([]string{}, st.Members...)
	out = append(out, st.Arbiters...)
	out = append(out, st.Witnesses...)
	return out
}

// ValidateSpread checks that the scope's data centers span at least three
// distinct failure domains (regions), so a single domain loss leaves a quorum.
// regionOf maps a cluster name to its region; when nil, each cluster is treated
// as its own region.
func (st *ScopeTopology) ValidateSpread(regionOf func(string) string) error {
	regions := map[string]struct{}{}
	for _, c := range st.AllClusters() {
		r := c
		if regionOf != nil {
			r = regionOf(c)
		}
		regions[r] = struct{}{}
	}
	if len(regions) < 3 {
		return fmt.Errorf("scope %s spans %d failure domain(s), need at least 3 (for example two Members plus one Arbiter, or three Members)", st.Scope, len(regions))
	}
	return nil
}

// Topology is the resolved set of scopes, keyed by their primary DC Lease name.
type Topology struct {
	Scopes map[string]*ScopeTopology
}

// NewTopology returns an empty Topology.
func NewTopology() *Topology {
	return &Topology{Scopes: map[string]*ScopeTopology{}}
}

func (t *Topology) add(pp *appsv1.PlacementPolicy) error {
	csc := pp.Spec.ClusterSpreadConstraint
	if csc == nil || csc.FailoverPolicy == nil {
		return nil
	}
	if err := csc.Validate(); err != nil {
		return fmt.Errorf("PlacementPolicy %q: %w", pp.Name, err)
	}

	var scope leases.Scope
	switch csc.FailoverPolicy.Trigger.Scope {
	case appsv1.FailoverScopeGroup:
		scope = leases.GroupScope(csc.FailoverPolicy.Trigger.Group)
	default:
		scope = leases.GlobalScope
	}

	name := scope.PrimaryLeaseName()
	st := t.Scopes[name]
	if st == nil {
		st = &ScopeTopology{Scope: scope}
		t.Scopes[name] = st
	}
	for i := range csc.DistributionRules {
		r := &csc.DistributionRules[i]
		switch r.Role {
		case appsv1.DCRoleArbiter:
			st.Arbiters = appendUnique(st.Arbiters, r.ClusterName)
		case appsv1.DCRoleWitness:
			st.Witnesses = appendUnique(st.Witnesses, r.ClusterName)
		default: // "" or Member
			st.Members = appendUnique(st.Members, r.ClusterName)
		}
	}
	return nil
}

// Derive builds a Topology from a set of PlacementPolicies. It returns the
// topology plus any per policy validation errors (a bad policy is skipped, not fatal).
func Derive(pps []*appsv1.PlacementPolicy) (*Topology, []error) {
	t := NewTopology()
	var errs []error
	for _, pp := range pps {
		if err := t.add(pp); err != nil {
			errs = append(errs, err)
		}
	}
	for _, st := range t.Scopes {
		sort.Strings(st.Members)
		sort.Strings(st.Arbiters)
		sort.Strings(st.Witnesses)
	}
	return t, errs
}

// ScopeAnnotationValue renders a scope for the AnnScope annotation.
func ScopeAnnotationValue(s leases.Scope) string {
	if s.IsGlobal() {
		return "global"
	}
	return s.Group
}

func appendUnique(s []string, v string) []string {
	for _, x := range s {
		if x == v {
			return s
		}
	}
	return append(s, v)
}
