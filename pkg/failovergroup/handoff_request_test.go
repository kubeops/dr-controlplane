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
	"context"
	"reflect"
	"testing"
	"time"

	"kubeops.dev/dr-controlplane/pkg/leases"
	appsv1 "kubeops.dev/petset/apis/apps/v1"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func appMember(name, dc string, age time.Duration, to string) appsv1.FailoverGroupMember {
	m := member(name, dc, true, age)
	m.APIGroup, m.Kind = "apps.k8s.appscode.com", "CattleSet"
	m.HandoffTo = to
	return m
}

func alive(dcs ...string) func(string) bool {
	set := map[string]bool{}
	for _, d := range dcs {
		set[d] = true
	}
	return func(dc string) bool { return set[dc] }
}

func TestHandoffRequest(t *testing.T) {
	appOn := func(members ...appsv1.FailoverGroupMember) *appsv1.FailoverGroup {
		return group("orders", "dc-a", []string{"data"}, members...)
	}
	standalone := func(members ...appsv1.FailoverGroupMember) *appsv1.FailoverGroup {
		return group("static", "dc-a", nil, members...)
	}
	leaseA := lease("dc-a", true, "dc-b")
	deadHolder := lease("dc-a", false, "dc-b")
	noLease := LeaseView{}

	cases := []struct {
		name string
		g    *appsv1.FailoverGroup
		l    LeaseView
		dep  string
		up   func(string) bool
		want string
	}{
		{"all members agree, target valid, dependency is there", appOn(appMember("o1", "dc-a", time.Second, "dc-b"), appMember("o2", "dc-a", time.Second, "dc-b")), leaseA, "dc-b", alive("dc-a", "dc-b"), "dc-b"},
		{"a group without dependencies may move anywhere valid", standalone(appMember("s1", "dc-a", time.Second, "dc-b")), leaseA, "", alive("dc-b"), "dc-b"},

		{"nobody asks", appOn(appMember("o1", "dc-a", time.Second, "")), leaseA, "dc-b", alive("dc-b"), ""},
		{"no members", appOn(), leaseA, "dc-b", alive("dc-b"), ""},
		{"one member silent", appOn(appMember("o1", "dc-a", time.Second, "dc-b"), appMember("o2", "dc-a", time.Second, "")), leaseA, "dc-b", alive("dc-b"), ""},
		{"one member stale", appOn(appMember("o1", "dc-a", time.Second, "dc-b"), appMember("o2", "dc-a", MemberReportTTL+time.Second, "dc-b")), leaseA, "dc-b", alive("dc-b"), ""},
		{"members disagree", appOn(appMember("o1", "dc-a", time.Second, "dc-b"), appMember("o2", "dc-a", time.Second, "dc-a")), leaseA, "dc-b", alive("dc-a", "dc-b"), ""},

		{"target is not a Member DC", standalone(appMember("s1", "dc-a", time.Second, "dc-w")), leaseA, "", alive("dc-w"), ""},
		{"target already holds the Lease", standalone(appMember("s1", "dc-b", time.Second, "dc-b")), lease("dc-b", true, ""), "", alive("dc-b"), ""},
		{"holder is dead: a failover, not a handoff", standalone(appMember("s1", "dc-a", time.Second, "dc-b")), deadHolder, "", alive("dc-b"), ""},
		{"holder has released", standalone(appMember("s1", "dc-a", time.Second, "dc-b")), lease("", false, ""), "", alive("dc-b"), ""},
		{"target's health Lease is stale", standalone(appMember("s1", "dc-a", time.Second, "dc-b")), leaseA, "", alive("dc-a"), ""},
		{"the Lease does not exist", standalone(appMember("s1", "dc-a", time.Second, "dc-b")), noLease, "", alive("dc-b"), ""},

		{"dependent group asks for a DC other than its dependencies'", appOn(appMember("o1", "dc-a", time.Second, "dc-a")), lease("dc-b", true, ""), "dc-b", alive("dc-a", "dc-b"), ""},
		{"dependencies are not settled", appOn(appMember("o1", "dc-a", time.Second, "dc-b")), leaseA, "", alive("dc-b"), ""},

		{"database member never sets handoffTo, a rogue entry does", appOn(member("pg2", "dc-a", true, time.Second), appMember("rogue", "dc-a", time.Second, "dc-b")), leaseA, "dc-b", alive("dc-b"), ""},
		{"database member alone is untouched", group("data2", "dc-a", nil, member("pg", "dc-a", true, time.Second)), leaseA, "", alive("dc-b"), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := HandoffRequest(tc.g, tc.l, tc.dep, tc.up, now); got != tc.want {
				t.Fatalf("HandoffRequest() = %q, want %q", got, tc.want)
			}
		})
	}
}

// The dependency target the controller passes is Decision.Target. Check it is
// what a real Decide produces for the planned-switchover trace, so the two
// functions cannot drift apart.
func TestHandoffRequestUsesDecideTarget(t *testing.T) {
	root := group("data", "dc-b", nil)
	app := group("orders", "dc-a", []string{"data"}, appMember("o1", "dc-a", time.Second, "dc-b"))
	l := lease("dc-a", true, "dc-b")
	d := Decide(app, index(root, app), l, now)
	if d.Target != "dc-b" {
		t.Fatalf("Decide target = %q", d.Target)
	}
	if got := HandoffRequest(app, l, d.Target, alive("dc-b"), now); got != "dc-b" {
		t.Fatalf("HandoffRequest() = %q, want dc-b", got)
	}

	// Dependencies disagree: Decide has no target, so nothing is requested.
	other := group("other", "dc-a", nil)
	app = group("orders", "dc-a", []string{"data", "other"}, appMember("o1", "dc-a", time.Second, "dc-b"))
	d = Decide(app, index(root, other, app), l, now)
	if got := HandoffRequest(app, l, d.Target, alive("dc-b"), now); got != "" {
		t.Fatalf("unsettled dependencies must block the request, got %q", got)
	}
}

func TestPlanHandoff(t *testing.T) {
	rm := map[string]any{leases.AnnHandoffTo: nil, leases.AnnHandoffRequest: nil}
	cases := []struct {
		name            string
		l               LeaseView
		agreed, initiat string
		want            map[string]any
	}{
		{"start a handoff", LeaseView{}, "dc-b", "dc-b", map[string]any{leases.AnnHandoffTo: "dc-b", leases.AnnHandoffRequest: "dc-b"}},
		{"nothing asked, nothing there", LeaseView{}, "", "", nil},
		{"agreed but not safe to start", LeaseView{}, "dc-b", "", nil},

		{"our pending handoff stays while the holder has released it (initiate is empty)", LeaseView{HandoffTo: "dc-b", HandoffRequest: "dc-b"}, "dc-b", "", nil},
		{"our pending handoff stays while still safe", LeaseView{HandoffTo: "dc-b", HandoffRequest: "dc-b"}, "dc-b", "dc-b", nil},
		{"members stop asking: abort or timeout withdraws ours", LeaseView{HandoffTo: "dc-b", HandoffRequest: "dc-b"}, "", "", rm},
		{"members now ask for another DC: ours is withdrawn first", LeaseView{HandoffTo: "dc-b", HandoffRequest: "dc-b"}, "dc-a", "dc-a", rm},

		{"a database operator's handoff-to is never touched", LeaseView{HandoffTo: "dc-b"}, "", "", nil},
		{"nor when the members ask for something else", LeaseView{HandoffTo: "dc-b"}, "dc-a", "dc-a", nil},
		{"nor when the members ask for the same DC", LeaseView{HandoffTo: "dc-b"}, "dc-b", "dc-b", nil},
		{"a companion for another target does not make an operator's handoff ours", LeaseView{HandoffTo: "dc-b", HandoffRequest: "dc-a"}, "", "", map[string]any{leases.AnnHandoffRequest: nil}},

		{"leftover companion after the agent cleared handoff-to", LeaseView{HandoffRequest: "dc-b"}, "", "", map[string]any{leases.AnnHandoffRequest: nil}},
		{"leftover companion is replaced when a new request starts", LeaseView{HandoffRequest: "dc-b"}, "dc-a", "dc-a", map[string]any{leases.AnnHandoffTo: "dc-a", leases.AnnHandoffRequest: "dc-a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := PlanHandoff(tc.l, tc.agreed, tc.initiat)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("PlanHandoff() = %v, want %v", got, tc.want)
			}
		})
	}
}

func primaryLease(name string, ann map[string]string) *coordinationv1.Lease {
	return &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: leases.DefaultNamespace, Annotations: ann, ResourceVersion: "1"}}
}

// A deleted group must not leave a handoff we requested pinned, and a handoff
// somebody else requested on a Lease with no group must be left alone.
func TestClearStaleHandoff(t *testing.T) {
	ours := primaryLease("primary-dc-gone", map[string]string{leases.AnnHandoffTo: "dc-b", leases.AnnHandoffRequest: "dc-b"})
	foreign := primaryLease("primary-dc-other", map[string]string{leases.AnnHandoffTo: "dc-b"})
	live := primaryLease("primary-dc-orders", map[string]string{leases.AnnHandoffTo: "dc-b", leases.AnnHandoffRequest: "dc-b"})
	cs := fake.NewSimpleClientset(ours, foreign, live)
	c := &Controller{coord: cs, ns: leases.DefaultNamespace}

	c.clearStaleHandoff(context.Background(), map[string]*appsv1.FailoverGroup{"orders": group("orders", "", nil)})

	get := func(name string) map[string]string {
		l, err := cs.CoordinationV1().Leases(leases.DefaultNamespace).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return l.Annotations
	}
	if a := get("primary-dc-gone"); a[leases.AnnHandoffTo] != "" || a[leases.AnnHandoffRequest] != "" {
		t.Fatalf("a deleted group's handoff must be withdrawn: %v", a)
	}
	if a := get("primary-dc-other"); a[leases.AnnHandoffTo] != "dc-b" {
		t.Fatalf("a handoff we did not request must be left alone: %v", a)
	}
	if a := get("primary-dc-orders"); a[leases.AnnHandoffTo] != "dc-b" || a[leases.AnnHandoffRequest] != "dc-b" {
		t.Fatalf("a live group's handoff must be left alone: %v", a)
	}
}

// End to end through the controller: members ask, the Lease gets the request;
// the holder releases; the request survives; members stop; it is withdrawn.
func TestSyncHandoffLifecycle(t *testing.T) {
	l := primaryLease("primary-dc-static", map[string]string{leases.AnnMemberDCs: "dc-a,dc-b"})
	hold := "dc-a"
	l.Spec.HolderIdentity = &hold
	cs := fake.NewSimpleClientset(l)
	c := &Controller{coord: cs, ns: leases.DefaultNamespace}
	ctx := context.Background()

	renew := metav1.NewMicroTime(now)
	dur := int32(45)
	for _, name := range []string{"dc-health-dc-b"} {
		_, err := cs.CoordinationV1().Leases(leases.DefaultNamespace).Create(ctx, &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: leases.DefaultNamespace},
			Spec:       coordinationv1.LeaseSpec{RenewTime: &renew, LeaseDurationSeconds: &dur},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}

	step := func(g *appsv1.FailoverGroup, holder string, holderAlive bool) map[string]string {
		t.Helper()
		cur, err := cs.CoordinationV1().Leases(leases.DefaultNamespace).Get(ctx, "primary-dc-static", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		view := LeaseView{
			Exists: true, Holder: holder, HolderAlive: holderAlive, MemberDCs: []string{"dc-a", "dc-b"},
			HandoffTo: cur.Annotations[leases.AnnHandoffTo], HandoffRequest: cur.Annotations[leases.AnnHandoffRequest],
		}
		c.syncHandoff(ctx, g, cur, view, Decision{}, now)
		after, err := cs.CoordinationV1().Leases(leases.DefaultNamespace).Get(ctx, "primary-dc-static", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return after.Annotations
	}

	asking := group("static", "dc-a", nil, appMember("s1", "dc-a", time.Second, "dc-b"))
	if a := step(asking, "dc-a", true); a[leases.AnnHandoffTo] != "dc-b" || a[leases.AnnHandoffRequest] != "dc-b" {
		t.Fatalf("the handoff must be requested: %v", a)
	}
	// The holder released the Lease: empty holder, the request must stay.
	if a := step(asking, "", false); a[leases.AnnHandoffTo] != "dc-b" {
		t.Fatalf("a pending handoff must survive the holder releasing the Lease: %v", a)
	}
	// The agent clears handoff-to when dc-b holds the Lease; the leftover goes.
	cur, _ := cs.CoordinationV1().Leases(leases.DefaultNamespace).Get(ctx, "primary-dc-static", metav1.GetOptions{})
	delete(cur.Annotations, leases.AnnHandoffTo)
	if _, err := cs.CoordinationV1().Leases(leases.DefaultNamespace).Update(ctx, cur, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	done := group("static", "dc-b", nil, appMember("s1", "dc-b", time.Second, ""))
	if a := step(done, "dc-b", true); a[leases.AnnHandoffRequest] != "" || a[leases.AnnHandoffTo] != "" {
		t.Fatalf("the leftover request must be removed: %v", a)
	}

	// A second request, then the members withdraw it (abort).
	if a := step(asking, "dc-a", true); a[leases.AnnHandoffTo] != "dc-b" {
		t.Fatalf("second request: %v", a)
	}
	withdrawn := group("static", "dc-a", nil, appMember("s1", "dc-a", time.Second, ""))
	if a := step(withdrawn, "dc-a", true); a[leases.AnnHandoffTo] != "" || a[leases.AnnHandoffRequest] != "" {
		t.Fatalf("an aborted request must be withdrawn: %v", a)
	}

	// A database operator's own handoff-to is never touched by this controller.
	cur, _ = cs.CoordinationV1().Leases(leases.DefaultNamespace).Get(ctx, "primary-dc-static", metav1.GetOptions{})
	cur.Annotations[leases.AnnHandoffTo] = "dc-b"
	if _, err := cs.CoordinationV1().Leases(leases.DefaultNamespace).Update(ctx, cur, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if a := step(withdrawn, "dc-a", true); a[leases.AnnHandoffTo] != "dc-b" {
		t.Fatalf("a handoff-to this controller did not write must survive: %v", a)
	}
}
