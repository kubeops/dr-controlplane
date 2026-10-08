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

package agent

import (
	"context"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

func primaryLease(name, holder string, renew time.Time) *coordinationv1.Lease {
	rt := metav1.NewMicroTime(renew)
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "dc-failover"},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: ptr.To(holder),
			RenewTime:      &rt,
		},
	}
}

// TestObservationCheckRepairsStaleView pins the fix for the 2026-08-07 live
// failure: both agents' Lease informers stopped delivering events while the
// Leases kept renewing, the observed renewTime froze 13+ minutes behind the
// actual one, every marker went stale, and the fence held every DC (including
// the quorum-trusted active one) read only. The watchdog must prove the drift
// with a direct List and repair the observed state through the ordinary
// reconcile path.
func TestObservationCheckRepairsStaleView(t *testing.T) {
	now := time.Now()
	actual := now.Add(-2 * time.Second)
	frozen := now.Add(-13 * time.Minute)

	cs := fake.NewSimpleClientset(primaryLease("primary-dc-d1drill", "dc-a", actual))
	a := New(Options{DCName: "dc-b", Namespace: "dc-failover"}, cs, cs, nil)
	a.rootCtx = context.Background()

	// The informer's last delivery, long ago.
	a.holders["primary-dc-d1drill"] = markerState{dc: "dc-a", renew: frozen}

	stale, err := a.checkObservation(context.Background(), false)
	if err != nil {
		t.Fatalf("checkObservation: %v", err)
	}
	if !stale {
		t.Fatal("a 13 minute drift between actual and observed renewTime must be reported stale")
	}
	a.mu.Lock()
	got := a.holders["primary-dc-d1drill"]
	a.mu.Unlock()
	if !got.renew.Equal(actual) {
		t.Fatalf("observed renewTime not repaired: got %v want %v", got.renew, actual)
	}
	if got.dc != "dc-a" {
		t.Fatalf("holder must survive the repair: got %q", got.dc)
	}
}

// TestObservationCheckQuietViewIsNotStale pins that silence is not staleness: a
// Lease whose actual renewTime equals the observed one (an idle or expired
// scope) must not trigger repairs or informer rebuilds.
func TestObservationCheckQuietViewIsNotStale(t *testing.T) {
	old := time.Now().Add(-30 * time.Minute)

	cs := fake.NewSimpleClientset(primaryLease("primary-dc-idle", "dc-a", old))
	a := New(Options{DCName: "dc-b", Namespace: "dc-failover"}, cs, cs, nil)
	a.rootCtx = context.Background()
	a.holders["primary-dc-idle"] = markerState{dc: "dc-a", renew: old}

	stale, err := a.checkObservation(context.Background(), false)
	if err != nil {
		t.Fatalf("checkObservation: %v", err)
	}
	if stale {
		t.Fatal("identical actual and observed renewTime must not be reported stale")
	}
}

// TestObservationCheckSmallDriftTolerated pins that ordinary informer latency
// (well under ObservationStaleAfter) is not treated as a dead watch.
func TestObservationCheckSmallDriftTolerated(t *testing.T) {
	now := time.Now()

	cs := fake.NewSimpleClientset(primaryLease("primary-dc-d1drill", "dc-a", now))
	a := New(Options{DCName: "dc-b", Namespace: "dc-failover"}, cs, cs, nil)
	a.rootCtx = context.Background()
	a.holders["primary-dc-d1drill"] = markerState{dc: "dc-a", renew: now.Add(-5 * time.Second)}

	stale, err := a.checkObservation(context.Background(), false)
	if err != nil {
		t.Fatalf("checkObservation: %v", err)
	}
	if stale {
		t.Fatalf("%s of drift is ordinary latency and must be tolerated", 5*time.Second)
	}
}

// TestForcedObservationStartsElectorsForInSyncLeases pins the writer cold-start
// path: with force, every listed Lease goes through reconcile even when the
// observed state is in perfect sync. Without this, a freshly promoted writer
// starts zero electors for self-held scopes (their Leases emit no events once
// the previous renewer is gone) and they freeze until the fence trips; observed
// live on the bank pair, 2026-08-07. The fake here has no electors to observe
// directly, so the test pins the observable contract: the holders entry is
// rewritten even with zero drift, proving reconcile ran for the in-sync Lease.
func TestForcedObservationStartsElectorsForInSyncLeases(t *testing.T) {
	now := time.Now()
	cs := fake.NewSimpleClientset(primaryLease("primary-dc-bankpg", "dr", now))
	a := New(Options{DCName: "dr", Namespace: "dc-failover"}, cs, cs, nil)
	a.rootCtx = context.Background()
	// Perfectly in-sync observation, but with a sentinel quiesce value that only
	// reconcile (reading the actual Lease) would clear.
	a.holders["primary-dc-bankpg"] = markerState{dc: "dr", renew: now, quiesce: "sentinel"}

	if _, err := a.checkObservation(context.Background(), false); err != nil {
		t.Fatalf("checkObservation: %v", err)
	}
	if a.holders["primary-dc-bankpg"].quiesce != "sentinel" {
		t.Fatal("without force, an in-sync lease must be skipped (this is the drift gate working)")
	}

	if _, err := a.checkObservation(context.Background(), true); err != nil {
		t.Fatalf("checkObservation force: %v", err)
	}
	if a.holders["primary-dc-bankpg"].quiesce != "" {
		t.Fatal("with force, reconcile must run for an in-sync lease (writer cold start depends on it)")
	}
}
