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
	"errors"
	"testing"
	"time"

	"kubeops.dev/dr-controlplane/pkg/leases"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/utils/ptr"
)

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

func priorityAgent(window time.Duration) *Agent {
	return New(Options{DCName: "dc-b", HealthRenewInterval: 5 * time.Second, HolderPriorityWindow: window}, nil, nil, nil)
}

// healthyFor makes the agent's health streak look unbroken for d, ending now.
func healthyFor(a *Agent, now time.Time, d time.Duration) {
	a.healthStreakStart.Store(now.Add(-d).UnixNano())
	a.lastRenewOK.Store(now.UnixNano())
}

func TestMayTakeOver(t *testing.T) {
	now := time.Now()

	a := priorityAgent(0)
	if !a.mayTakeOver(now) {
		t.Fatal("a zero window disables the guard")
	}

	a = priorityAgent(20 * time.Second)
	if a.mayTakeOver(now) {
		t.Fatal("no health renewal yet means no evidence, so no takeover")
	}
	healthyFor(a, now, 10*time.Second)
	if a.mayTakeOver(now) {
		t.Fatal("a streak shorter than the window must defer")
	}
	healthyFor(a, now, 45*time.Second)
	if !a.mayTakeOver(now) {
		t.Fatal("a streak longer than the window (a real DC loss) must not be delayed")
	}
	// The streak is long but the last success is older than the break gap:
	// this DC is failing right now.
	a.lastRenewOK.Store(now.Add(-16 * time.Second).UnixNano())
	if a.mayTakeOver(now) {
		t.Fatal("a DC whose own hub writes are failing now must defer")
	}
}

func TestHealthStreak(t *testing.T) {
	a := priorityAgent(20 * time.Second)
	t0 := time.Now().Add(-time.Minute)

	a.noteHealthRenewed(t0)
	// A blip: one missed renewal (10s gap) keeps the streak.
	a.noteHealthRenewed(t0.Add(10 * time.Second))
	a.noteHealthRenewed(t0.Add(15 * time.Second))
	if got := time.Unix(0, a.healthStreakStart.Load()); !got.Equal(t0) {
		t.Fatalf("a one-renewal blip must not break the streak, start moved to %v", got)
	}
	// An outage: a 50s gap breaks it.
	a.noteHealthRenewed(t0.Add(65 * time.Second))
	if got := time.Unix(0, a.healthStreakStart.Load()); !got.Equal(t0.Add(65 * time.Second)) {
		t.Fatalf("a long gap must restart the streak, start is %v", got)
	}
	a.resetHealthStreak()
	if a.lastRenewOK.Load() != 0 || a.healthStreakStart.Load() != 0 {
		t.Fatal("reset must forget the streak")
	}
}

// fakeLock is an in-memory resourcelock.Interface.
type fakeLock struct {
	rec     resourcelock.LeaderElectionRecord
	updates int
}

func (f *fakeLock) Get(context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	r := f.rec
	return &r, []byte(r.HolderIdentity), nil
}

func (f *fakeLock) Create(_ context.Context, ler resourcelock.LeaderElectionRecord) error {
	f.rec = ler
	return nil
}

func (f *fakeLock) Update(_ context.Context, ler resourcelock.LeaderElectionRecord) error {
	f.rec = ler
	f.updates++
	return nil
}
func (f *fakeLock) RecordEvent(string) {}
func (f *fakeLock) Identity() string   { return "dc-b" }
func (f *fakeLock) Describe() string   { return "fake" }
func (f *fakeLock) holder() string     { return f.rec.HolderIdentity }
func (f *fakeLock) setHolder(h string) { f.rec.HolderIdentity = h }

// dcBRecord is the record dc-b writes when it renews or acquires.
var dcBRecord = resourcelock.LeaderElectionRecord{HolderIdentity: "dc-b"}

func TestHolderPriorityLock(t *testing.T) {
	ctx := context.Background()
	a := priorityAgent(20 * time.Second)
	a.metrics = NewMetrics(prometheus.NewRegistry(), "dc-b")
	inner := &fakeLock{}
	l := &holderPriorityLock{Interface: inner, a: a, scope: "default"}

	// Takeover from another DC with no health streak: deferred, nothing written.
	inner.setHolder("dc-a")
	if _, _, err := l.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if err := l.Update(ctx, dcBRecord); !errors.Is(err, errTakeoverDeferred) {
		t.Fatalf("want errTakeoverDeferred, got %v", err)
	}
	if inner.updates != 0 || inner.holder() != "dc-a" {
		t.Fatal("a deferred takeover must not write the Lease")
	}
	if got := counterValue(t, a.metrics.TakeoversDeferred.WithLabelValues("default")); got != 1 {
		t.Fatalf("takeovers_deferred_total = %v, want 1", got)
	}

	// The same takeover once the streak is long enough: allowed.
	healthyFor(a, time.Now(), time.Minute)
	if err := l.Update(ctx, dcBRecord); err != nil {
		t.Fatalf("takeover after a healthy streak must pass: %v", err)
	}
	if inner.holder() != "dc-b" {
		t.Fatal("takeover was not written")
	}

	// The holder renewing its own Lease is never gated, even with no streak
	// (the fast path calls Update without a Get: lastHolder comes from the
	// previous successful Update).
	a.resetHealthStreak()
	if err := l.Update(ctx, dcBRecord); err != nil {
		t.Fatalf("the holder's renewal must never be gated: %v", err)
	}

	// Acquiring a Lease with no holder (a fresh Lease or a handoff release)
	// is never gated.
	inner.setHolder("")
	if _, _, err := l.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if err := l.Update(ctx, dcBRecord); err != nil {
		t.Fatalf("acquiring a released Lease must not be gated: %v", err)
	}
}

// expiredLease is a primary DC Lease held by holder whose last renewal is an
// hour old: what every DC sees right after a long hub outage.
func expiredLease(holder string) *coordinationv1.Lease {
	old := metav1.NewMicroTime(time.Now().Add(-time.Hour))
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name: "primary-dc", Namespace: "dc-failover",
			Annotations: map[string]string{leases.AnnMemberDCs: "dc-a,dc-b"},
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptr.To(holder),
			LeaseDurationSeconds: ptr.To[int32](1),
			AcquireTime:          &old,
			RenewTime:            &old,
		},
	}
}

func electionAgent(dc string, cs *fake.Clientset) *Agent {
	a := New(Options{
		DCName: dc, Namespace: "dc-failover",
		HealthRenewInterval:  5 * time.Second,
		HolderPriorityWindow: 20 * time.Second,
		Election:             ElectionConfig{LeaseDuration: time.Second, RenewDeadline: 800 * time.Millisecond, RetryPeriod: 100 * time.Millisecond},
	}, cs, cs, NewMetrics(prometheus.NewRegistry(), dc))
	return a
}

func holderOfLease(t *testing.T, cs *fake.Clientset) string {
	t.Helper()
	l, err := cs.CoordinationV1().Leases("dc-failover").Get(context.Background(), "primary-dc", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return ptr.Deref(l.Spec.HolderIdentity, "")
}

func startElector(t *testing.T, a *Agent) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	a.mu.Lock()
	a.writerCtx = ctx
	a.mu.Unlock()
	a.isWriter.Store(true)
	scope, ok := leases.ScopeFromPrimaryLeaseName("primary-dc")
	if !ok {
		t.Fatal("bad scope")
	}
	a.electorFor(scope, "primary-dc").setDesired(true)
	return cancel
}

// TestSharedOutageNonHolderDefers is the brownout case end to end through the
// real client-go elector: the Lease expired for everyone, the non-holder's own
// hub writes have only just come back, so it must not take the Lease; once its
// streak is long enough (a real DC loss) it does.
func TestSharedOutageNonHolderDefers(t *testing.T) {
	cs := fake.NewClientset(expiredLease("dc-a"))
	b := electionAgent("dc-b", cs)
	cancel := startElector(t, b)
	defer cancel()

	time.Sleep(2500 * time.Millisecond)
	if h := holderOfLease(t, cs); h != "dc-a" {
		t.Fatalf("dc-b took the Lease right after a shared outage, holder %q", h)
	}
	if counterValue(t, b.metrics.TakeoversDeferred.WithLabelValues("global")) == 0 {
		t.Fatal("expected deferred takeovers to be counted")
	}

	healthyFor(b, time.Now(), time.Minute)
	deadline := time.Now().Add(3 * time.Second)
	for holderOfLease(t, cs) != "dc-b" {
		if time.Now().After(deadline) {
			t.Fatal("dc-b never took over although its hub writes were healthy (a real DC loss)")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestSharedOutageHolderRenewsAtOnce: the holder is never gated, so right after
// the outage it renews its own expired Lease with no health streak at all.
func TestSharedOutageHolderRenewsAtOnce(t *testing.T) {
	cs := fake.NewClientset(expiredLease("dc-a"))
	a := electionAgent("dc-a", cs)
	cancel := startElector(t, a)
	defer cancel()

	deadline := time.Now().Add(2 * time.Second)
	for {
		l, err := cs.CoordinationV1().Leases("dc-failover").Get(context.Background(), "primary-dc", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if ptr.Deref(l.Spec.HolderIdentity, "") == "dc-a" && time.Since(l.Spec.RenewTime.Time) < time.Minute {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the holder did not renew its own expired Lease")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
