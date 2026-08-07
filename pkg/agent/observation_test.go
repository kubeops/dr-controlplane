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

	stale, err := a.checkObservation(context.Background())
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

	stale, err := a.checkObservation(context.Background())
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

	stale, err := a.checkObservation(context.Background())
	if err != nil {
		t.Fatalf("checkObservation: %v", err)
	}
	if stale {
		t.Fatalf("%s of drift is ordinary latency and must be tolerated", 5*time.Second)
	}
}
