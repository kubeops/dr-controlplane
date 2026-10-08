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

package topology

import (
	"strings"
	"testing"

	appsv1 "kubeops.dev/petset/apis/apps/v1"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func pp(name string, fp *appsv1.FailoverPolicy, rules ...appsv1.DistributionRule) *appsv1.PlacementPolicy {
	return &appsv1.PlacementPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: appsv1.PlacementPolicySpec{
			ClusterSpreadConstraint: &appsv1.ClusterSpreadConstraint{
				DistributionRules: rules,
				FailoverPolicy:    fp,
			},
		},
	}
}

func member(c string) appsv1.DistributionRule {
	return appsv1.DistributionRule{ClusterName: c, ReplicaIndices: []int32{0}, Role: appsv1.DCRoleMember}
}

func arbiter(c string) appsv1.DistributionRule {
	return appsv1.DistributionRule{ClusterName: c, Role: appsv1.DCRoleArbiter}
}

func witness(c string) appsv1.DistributionRule {
	return appsv1.DistributionRule{ClusterName: c, ReplicaIndices: []int32{0}, Role: appsv1.DCRoleWitness}
}

func TestDeriveTwoDCWithArbiter(t *testing.T) {
	fp := &appsv1.FailoverPolicy{Mode: appsv1.FailoverModeTwoDC}
	topo, errs := Derive([]*appsv1.PlacementPolicy{pp("pg", fp, member("dc-a"), member("dc-b"), arbiter("dc-c"))})
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	st := topo.Scopes["primary-dc"]
	if st == nil {
		t.Fatalf("expected a global scope keyed primary-dc, got %v", topo.Scopes)
	}
	if strings.Join(st.Members, ",") != "dc-a,dc-b" {
		t.Fatalf("members = %v, want [dc-a dc-b]", st.Members)
	}
	if strings.Join(st.Arbiters, ",") != "dc-c" {
		t.Fatalf("arbiters = %v, want [dc-c]", st.Arbiters)
	}
	if err := st.ValidateSpread(nil); err != nil {
		t.Fatalf("spread should be valid across 3 clusters: %v", err)
	}
}

func TestDeriveGroupWitness(t *testing.T) {
	// MongoDB style: two data Members plus a data bearing Witness, group scoped.
	fp := &appsv1.FailoverPolicy{Mode: appsv1.FailoverModeTwoDC, FailoverGroupRef: &core.LocalObjectReference{Name: "orders"}}
	topo, errs := Derive([]*appsv1.PlacementPolicy{pp("mongo", fp, member("dc-a"), member("dc-b"), witness("dc-c"))})
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	st := topo.Scopes["primary-dc-orders"]
	if st == nil {
		t.Fatalf("expected group scope keyed primary-dc-orders, got %v", topo.Scopes)
	}
	if strings.Join(st.Witnesses, ",") != "dc-c" {
		t.Fatalf("witnesses = %v, want [dc-c]", st.Witnesses)
	}
	if len(st.Members) != 2 {
		t.Fatalf("members = %v, want 2", st.Members)
	}
}

func TestDeriveRejectsSingleMember(t *testing.T) {
	fp := &appsv1.FailoverPolicy{}
	_, errs := Derive([]*appsv1.PlacementPolicy{pp("bad", fp, member("dc-a"), arbiter("dc-c"))})
	if len(errs) == 0 {
		t.Fatalf("expected a validation error for a single Member DC/DR config")
	}
}

func TestValidateSpreadRejectsTwoRegions(t *testing.T) {
	st := &ScopeTopology{Members: []string{"dc-a", "dc-b"}, Arbiters: []string{"dc-c"}}
	regionOf := func(c string) string {
		if c == "dc-c" {
			return "us-east" // same region as dc-a
		}
		if c == "dc-a" {
			return "us-east"
		}
		return "us-west"
	}
	if err := st.ValidateSpread(regionOf); err == nil {
		t.Fatalf("expected spread validation to fail with only two regions")
	}
}
