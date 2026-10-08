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

package failovergroup

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	appsv1 "kubeops.dev/petset/apis/apps/v1"
)

var now = time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)

func group(name, activeDC string, deps []string, members ...appsv1.FailoverGroupMember) *appsv1.FailoverGroup {
	return &appsv1.FailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       appsv1.FailoverGroupSpec{DependsOn: deps},
		Status:     appsv1.FailoverGroupStatus{ActiveDC: activeDC, Members: members},
	}
}

func member(name, dc string, ready bool, age time.Duration) appsv1.FailoverGroupMember {
	t := metav1.NewTime(now.Add(-age))
	return appsv1.FailoverGroupMember{APIGroup: "kubedb.com", Kind: "Postgres", Namespace: "demo", Name: name, DC: dc, Ready: ready, ObservedAt: &t}
}

func lease(holder string, alive bool, follow string) LeaseView {
	return LeaseView{Exists: true, Holder: holder, HolderAlive: alive, MemberDCs: []string{"dc-a", "dc-b"}, FollowDC: follow}
}

func index(gs ...*appsv1.FailoverGroup) map[string]*appsv1.FailoverGroup {
	m := map[string]*appsv1.FailoverGroup{}
	for _, g := range gs {
		m[g.Name] = g
	}
	return m
}

func TestDecideRootGroup(t *testing.T) {
	// Lease moved to dc-b, member not promoted yet: activeDC stays on dc-a.
	g := group("data", "dc-a", nil, member("pg", "dc-b", false, time.Second))
	d := Decide(g, index(g), lease("dc-b", true, ""), now)
	if d.Phase != appsv1.FailoverGroupPhaseWaitingForMembers || d.ActiveDC != "dc-a" || d.Target != "dc-b" || d.FollowDC != "" || d.SwitchoverTo != "" {
		t.Fatalf("unexpected decision %+v", d)
	}

	// Member confirmed writable on dc-b: activeDC moves.
	g = group("data", "dc-a", nil, member("pg", "dc-b", true, time.Second))
	d = Decide(g, index(g), lease("dc-b", true, ""), now)
	if d.Phase != appsv1.FailoverGroupPhaseSteady || d.ActiveDC != "dc-b" {
		t.Fatalf("unexpected decision %+v", d)
	}

	// A stale ready report is not ready.
	g = group("data", "dc-a", nil, member("pg", "dc-b", true, MemberReportTTL+time.Second))
	d = Decide(g, index(g), lease("dc-b", true, ""), now)
	if d.Phase != appsv1.FailoverGroupPhaseWaitingForMembers || d.ActiveDC != "dc-a" {
		t.Fatalf("stale report must not be ready: %+v", d)
	}

	// No member has reported yet: not ready, even though the Lease has a holder.
	g = group("data", "", nil)
	d = Decide(g, index(g), lease("dc-b", true, ""), now)
	if d.Phase != appsv1.FailoverGroupPhaseWaitingForMembers || d.ActiveDC != "" {
		t.Fatalf("a group with no reported members must not become active: %+v", d)
	}

	// No Lease holder yet.
	g = group("data", "", nil)
	d = Decide(g, index(g), LeaseView{}, now)
	if d.Phase != appsv1.FailoverGroupPhaseWaitingForMembers || d.ActiveDC != "" {
		t.Fatalf("unexpected decision %+v", d)
	}
}

func TestDecideDependentUnplannedFailover(t *testing.T) {
	// DC a lost. The root's Lease moved but its database is not writable on dc-b
	// yet, so the root is still published on dc-a and the dependent is held on dc-a.
	root := group("data", "dc-a", nil)
	app := group("billing", "dc-a", []string{"data"}, member("pg2", "dc-a", false, time.Second))
	d := Decide(app, index(root, app), lease("dc-a", false, "dc-a"), now)
	if d.FollowDC != "dc-a" || d.SwitchoverTo != "" || d.ActiveDC != "dc-a" || d.Phase != appsv1.FailoverGroupPhaseWaitingForMembers {
		t.Fatalf("dependent must stay held on dc-a: %+v", d)
	}

	// Root ready on dc-b: follow-dc moves to dc-b. The dead holder is not asked
	// to switch over; dc-b acquires the expired Lease through follow-dc.
	root = group("data", "dc-b", nil)
	d = Decide(app, index(root, app), lease("dc-a", false, "dc-a"), now)
	if d.FollowDC != "dc-b" || d.SwitchoverTo != "" || d.ActiveDC != "dc-a" {
		t.Fatalf("dependent must follow to dc-b without a switchover: %+v", d)
	}

	// dc-b acquired and the dependent's database is writable there.
	app = group("billing", "dc-a", []string{"data"}, member("pg2", "dc-b", true, time.Second))
	d = Decide(app, index(root, app), lease("dc-b", true, "dc-b"), now)
	if d.Phase != appsv1.FailoverGroupPhaseSteady || d.ActiveDC != "dc-b" {
		t.Fatalf("unexpected decision %+v", d)
	}
}

func TestDecideDependentPlannedSwitchover(t *testing.T) {
	// The root switched over to dc-b; the dependent's holder dc-a is alive, so it
	// must be asked to switch over.
	root := group("data", "dc-b", nil)
	app := group("billing", "dc-a", []string{"data"}, member("pg2", "dc-a", true, time.Second))
	d := Decide(app, index(root, app), lease("dc-a", true, "dc-a"), now)
	if d.FollowDC != "dc-b" || d.SwitchoverTo != "dc-b" || d.ActiveDC != "dc-a" {
		t.Fatalf("dependent must be switched over to dc-b: %+v", d)
	}
}

func TestDecideDependentNotSwitchedOverUntilReady(t *testing.T) {
	// Bootstrap: the dependent's Lease was won by dc-a while its database is still
	// provisioning; the dependency is active on dc-b. Follow dc-b, but do not switch
	// a database over that is not serving yet.
	root := group("data", "dc-b", nil)
	app := group("billing", "", []string{"data"}, member("pg2", "dc-a", false, time.Second))
	d := Decide(app, index(root, app), lease("dc-a", true, ""), now)
	if d.FollowDC != "dc-b" || d.SwitchoverTo != "" || d.ActiveDC != "" {
		t.Fatalf("a not-ready dependent must not be switched over: %+v", d)
	}

	// No member reported at all: same.
	app = group("billing", "", []string{"data"})
	if d := Decide(app, index(root, app), lease("dc-a", true, ""), now); d.SwitchoverTo != "" {
		t.Fatalf("a dependent with no reported members must not be switched over: %+v", d)
	}

	// Once ready on dc-a, it is switched over to follow dc-b.
	app = group("billing", "", []string{"data"}, member("pg2", "dc-a", true, time.Second))
	if d := Decide(app, index(root, app), lease("dc-a", true, "dc-b"), now); d.SwitchoverTo != "dc-b" {
		t.Fatalf("a ready dependent must be switched over: %+v", d)
	}
}

func TestDecideDependencyNotSettled(t *testing.T) {
	a := group("a", "dc-a", nil)
	b := group("b", "dc-b", nil)
	app := group("app", "dc-a", []string{"a", "b"})
	d := Decide(app, index(a, b, app), lease("dc-a", true, "dc-a"), now)
	if d.Phase != appsv1.FailoverGroupPhaseWaitingForDependency || d.FollowDC != "dc-a" {
		t.Fatalf("disagreeing dependencies must wait and keep follow-dc: %+v", d)
	}

	none := group("none", "", nil)
	app = group("app", "dc-a", []string{"none"})
	d = Decide(app, index(none, app), lease("dc-a", true, "dc-a"), now)
	if d.Phase != appsv1.FailoverGroupPhaseWaitingForDependency || d.FollowDC != "dc-a" {
		t.Fatalf("inactive dependency must wait and keep follow-dc: %+v", d)
	}
}

func TestDecideBlocked(t *testing.T) {
	x := group("x", "", []string{"y"})
	y := group("y", "", []string{"x"})
	if d := Decide(x, index(x, y), lease("dc-a", true, "dc-a"), now); d.Phase != appsv1.FailoverGroupPhaseBlocked || d.FollowDC != "" {
		t.Fatalf("cycle must block and clear follow-dc: %+v", d)
	}

	app := group("app", "", []string{"missing"})
	if d := Decide(app, index(app), lease("dc-a", true, "dc-a"), now); d.Phase != appsv1.FailoverGroupPhaseBlocked || d.FollowDC != "" {
		t.Fatalf("missing dependency must block and clear follow-dc: %+v", d)
	}

	root := group("data", "dc-c", nil)
	app = group("app", "dc-a", []string{"data"})
	if d := Decide(app, index(root, app), lease("dc-a", true, "dc-a"), now); d.Phase != appsv1.FailoverGroupPhaseBlocked {
		t.Fatalf("dependency on a non-member DC must block: %+v", d)
	}
}
