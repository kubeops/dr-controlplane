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
	"sync/atomic"
	"testing"
	"time"

	"kubeops.dev/dr-controlplane/pkg/leases"

	"github.com/prometheus/client_golang/prometheus"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func writerAgent(identity string) *Agent {
	hub := fake.NewClientset()
	return New(Options{
		DCName: "dc-a", Namespace: "dc-failover", MarkerNamespace: "dc-failover",
		WriterIdentity:        identity,
		HealthLeaseDuration:   15 * time.Second,
		HealthRenewInterval:   5 * time.Second,
		MarkerRefreshInterval: time.Hour,
		Election:              ElectionConfig{LeaseDuration: 45 * time.Second, RenewDeadline: 30 * time.Second, RetryPeriod: 2 * time.Second},
		WriterElection:        ElectionConfig{LeaseDuration: 3 * time.Second, RenewDeadline: 600 * time.Millisecond, RetryPeriod: 100 * time.Millisecond},
	}, hub, hub, NewMetrics(prometheus.NewRegistry(), "dc-a"))
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", d, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func writerLeaseHolder(t *testing.T, spoke *fake.Clientset) string {
	t.Helper()
	l, err := spoke.CoordinationV1().Leases("dc-failover").Get(context.Background(), writerLeaseName("dc-a"), metav1.GetOptions{})
	if err != nil {
		return ""
	}
	return ptr.Deref(l.Spec.HolderIdentity, "")
}

// TestWriterStepsDownInProcess pins fix E: losing the writer Lease (here, the
// spoke API failing every Lease update past the renew deadline) steps the
// replica down without ending the process, and it becomes the writer again as
// soon as the spoke API recovers. It used to exit the process and pay a pod
// restart plus a full writer Lease expiry, about 30s of marker gap.
func TestWriterStepsDownInProcess(t *testing.T) {
	a := writerAgent("agent-pod-0")
	spoke := fake.NewClientset()
	var stall atomic.Bool
	spoke.PrependReactor("update", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
		if stall.Load() {
			return true, nil, errors.New("context deadline exceeded")
		}
		return false, nil, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- writerElection{a: a, cs: spoke}.Start(ctx) }()

	waitFor(t, 3*time.Second, "first writer term", a.isWriter.Load)
	if a.currentWriterCtx() == nil {
		t.Fatal("a writer term must expose its context")
	}

	stall.Store(true)
	waitFor(t, 3*time.Second, "step-down after the spoke stall", func() bool { return !a.isWriter.Load() })
	if a.currentWriterCtx() != nil {
		t.Fatal("after step-down no writer context may remain")
	}
	select {
	case err := <-done:
		t.Fatalf("the writer election ended (%v); it must keep running and contend again", err)
	default:
	}

	stall.Store(false)
	// Same identity, still recorded in the Lease: re-acquired at once, well
	// inside the 3s writer LeaseDuration.
	start := time.Now()
	waitFor(t, 2*time.Second, "writer role re-acquired in process", a.isWriter.Load)
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("re-acquire took %v, it must not wait for the Lease to expire", took)
	}
	if n := counterValue(t, a.metrics.WriterStepDowns); n < 1 {
		t.Fatalf("writer_step_downs_total = %v, want >= 1", n)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("clean shutdown returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("writer election did not stop on shutdown")
	}
	// Clean shutdown hands the role over.
	if h := writerLeaseHolder(t, spoke); h != "" {
		t.Fatalf("writer Lease still held by %q after a clean shutdown", h)
	}
}

// writerLease is a writer Lease held by holder and renewed just now.
func writerLease(holder string) *coordinationv1.Lease {
	now := metav1.NewMicroTime(time.Now())
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: writerLeaseName("dc-a"), Namespace: "dc-failover"},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptr.To(holder),
			LeaseDurationSeconds: ptr.To[int32](60),
			AcquireTime:          &now,
			RenewTime:            &now,
		},
	}
}

// TestRestartedContainerReclaimsWriterAtOnce pins fix F: a restarted container
// keeps its pod name, so it finds its own identity in a fresh writer Lease and
// renews it at once, instead of waiting out the Lease (15s live).
func TestRestartedContainerReclaimsWriterAtOnce(t *testing.T) {
	a := writerAgent("agent-pod-0")
	spoke := fake.NewClientset(writerLease("agent-pod-0"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = writerElection{a: a, cs: spoke}.Start(ctx) }()
	waitFor(t, time.Second, "the restarted container to reclaim its writer Lease", a.isWriter.Load)
}

// TestOtherPodWaitsForWriterLease: the single writer guarantee still holds, a
// different pod cannot take a writer Lease that is being held.
func TestOtherPodWaitsForWriterLease(t *testing.T) {
	a := writerAgent("agent-pod-1")
	spoke := fake.NewClientset(writerLease("agent-pod-0"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = writerElection{a: a, cs: spoke}.Start(ctx) }()
	time.Sleep(time.Second)
	if a.isWriter.Load() {
		t.Fatal("a second pod became writer while another pod holds a fresh writer Lease")
	}
}

// TestStepDownStopsElectorsAndBlocksNewOnes: after a step-down no elector is
// left and none can be created until the next writer term.
func TestStepDownStopsElectorsAndBlocksNewOnes(t *testing.T) {
	a := writerAgent("agent-pod-0")
	ctx, cancel := context.WithCancel(context.Background())
	a.mu.Lock()
	a.writerCtx = ctx
	a.mu.Unlock()
	a.isWriter.Store(true)

	scope, _ := leases.ScopeFromPrimaryLeaseName("primary-dc")
	e := a.electorFor(scope, "primary-dc")
	if e == nil {
		t.Fatal("a writer term must be able to create electors")
	}
	e.setDesired(true)

	cancel()
	a.stepDown()
	if a.isWriter.Load() {
		t.Fatal("still writer after step-down")
	}
	e.mu.Lock()
	running := e.running
	e.mu.Unlock()
	if running {
		t.Fatal("step-down must stop every elector")
	}
	a.mu.Lock()
	n := len(a.electors)
	a.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d electors left after step-down", n)
	}
	if a.electorFor(scope, "primary-dc") != nil {
		t.Fatal("no elector may be created outside a writer term")
	}
	// Idempotent: client-go calls OnStoppedLeading on every election exit.
	a.stepDown()
}

// TestLateWriterTermDoesNotResurrect: if a term ends before its goroutine got to
// run, that goroutine must not flip the replica back to writer.
func TestLateWriterTermDoesNotResurrect(t *testing.T) {
	a := writerAgent("agent-pod-0")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a.stepDown()
	a.runWriterTerm(ctx)
	if a.isWriter.Load() || a.currentWriterCtx() != nil {
		t.Fatal("a term that already ended made this replica the writer")
	}
}
