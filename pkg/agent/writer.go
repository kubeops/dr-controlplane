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
	"fmt"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// The writer role. With more than one agent replica per DC, exactly ONE pod may
// write: the marker ConfigMap (a stale replica restamping an older renewTime
// would age or flap the fence on a healthy DC), the hub scope Leases, the DC
// health Lease, and the break glass override annotations. The role is decided
// by a client-go leader election against the SPOKE cluster: same failure domain
// as the writes it guards, so a coordination plane outage can never change which
// local pod holds the pen. Non-writer replicas stay hot: they run the hub Lease
// informer, the holders cache, and the observation watchdog, so a takeover
// starts from a warm view and only has to win the local election (bounded by
// WriterElection.LeaseDuration, which is kept well inside the 30s fence TTL).
//
// Losing the writer role steps the replica down IN PROCESS: it stops every write
// path (the writer term context is canceled, which stops the electors, the
// health renewer, the projector and any in-flight hub write), keeps its hub
// observation warm, and contends for the writer role again. It used to exit the
// process instead, and that turned every short spoke API stall (a renew of the
// writer Lease missing its 10s deadline) into a full pod restart: up to 10s of
// stall, a few seconds of start up, then 15s waiting for its own old writer
// Lease to expire, close to 30s with no marker restamp. That is the fence TTL,
// so a healthy DC went read only and came within seconds of a false failover.
// Observed live on spoke-2 and on the hub agent in the 2026-11 lab.
//
// The writer identity is the pod name (Options.WriterIdentity), stable across
// container restarts, so a restarted container renews the writer Lease it still
// holds at once instead of waiting for it to expire. The Lease is released only
// on a clean shutdown, never on a lost renewal: a release from a replica whose
// renewals were timing out could race a successor that already took over.

// writerLeaseName is the spoke-local election Lease, one per DC.
func writerLeaseName(dc string) string {
	return "dr-agent-writer-" + dc
}

var writerScheme = runtime.NewScheme()

func init() {
	_ = clientgoscheme.AddToScheme(writerScheme)
}

// newSpokeManager builds the controller-runtime manager that owns the
// spoke-side cached client. Reads of spoke objects (marker ConfigMaps,
// standby-hold and override ConfigMaps) go through mgr.GetClient(), which is
// informer-cache backed; writes go through the same client and hit the API
// server directly. The manager does no leader election of its own: the writer
// election runs as one of its runnables (writerElection), so it starts only
// after the cache has synced.
func (a *Agent) newSpokeManager() (manager.Manager, error) {
	cfg, err := a.opts.SpokeRESTConfig()
	if err != nil {
		return nil, fmt.Errorf("spoke rest config: %w", err)
	}
	mgr, err := ctrl.NewManager(cfg, manager.Options{
		Scheme: writerScheme,
		// The agent keeps its own prometheus registry and /metrics mux.
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "",
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{a.opts.MarkerNamespace: {}},
		},
		LeaderElection: false,
	})
	if err != nil {
		return nil, fmt.Errorf("spoke manager: %w", err)
	}
	if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
		return nil, err
	}
	return mgr, nil
}

// writerElection is the manager runnable that runs the spoke-local writer
// election for the life of the process.
type writerElection struct {
	a  *Agent
	cs kubernetes.Interface
}

// NeedLeaderElection is false: this runnable IS the election.
func (w writerElection) NeedLeaderElection() bool { return false }

// Start contends for the writer role until ctx ends. Each lost term steps the
// replica down and the loop contends again; the process never exits for it.
func (w writerElection) Start(ctx context.Context) error {
	a := w.a
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      writerLeaseName(a.opts.DCName),
			Namespace: a.opts.MarkerNamespace,
		},
		Client:     w.cs.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: a.opts.WriterIdentity},
	}
	for {
		le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
			Lock:          lock,
			LeaseDuration: a.opts.WriterElection.LeaseDuration,
			RenewDeadline: a.opts.WriterElection.RenewDeadline,
			RetryPeriod:   a.opts.WriterElection.RetryPeriod,
			// Released by releaseWriterLease on a clean shutdown only, see the
			// comment at the top of this file.
			ReleaseOnCancel: false,
			Name:            writerLeaseName(a.opts.DCName),
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: a.runWriterTerm,
				OnStoppedLeading: a.stepDown,
			},
		})
		if err != nil {
			return fmt.Errorf("writer election: %w", err)
		}
		le.Run(ctx)
		if ctx.Err() != nil {
			w.releaseWriterLease()
			return nil
		}
		klog.InfoS("lost the DC writer role; stepped down and contending again", "dcdr.dc", a.opts.DCName, "identity", a.opts.WriterIdentity)
		if a.metrics != nil {
			a.metrics.WriterStepDowns.Inc()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait.Jitter(a.opts.WriterElection.RetryPeriod, 0.5)):
		}
	}
}

// releaseWriterLease hands the writer role over on a clean shutdown (a rollout,
// a drain), so the standby takes over in one retry period instead of a full
// LeaseDuration. Only a Lease this replica still holds is released.
func (w writerElection) releaseWriterLease() {
	a := w.a
	ctx, cancel := context.WithTimeout(context.Background(), a.opts.WriterElection.RenewDeadline)
	defer cancel()
	cl := w.cs.CoordinationV1().Leases(a.opts.MarkerNamespace)
	l, err := cl.Get(ctx, writerLeaseName(a.opts.DCName), metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			klog.ErrorS(err, "could not read the writer Lease to release it on shutdown")
		}
		return
	}
	if l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity != a.opts.WriterIdentity {
		return
	}
	now := metav1.NewMicroTime(time.Now())
	l.Spec.HolderIdentity = nil
	l.Spec.LeaseDurationSeconds = ptr.To[int32](1)
	l.Spec.RenewTime = &now
	l.Spec.AcquireTime = &now
	if _, err := cl.Update(ctx, l, metav1.UpdateOptions{}); err != nil {
		klog.ErrorS(err, "could not release the writer Lease on shutdown; the standby takes over when it expires")
		return
	}
	klog.InfoS("released the DC writer role on shutdown", "dcdr.dc", a.opts.DCName)
}

// runWriterTerm is the leader-gated half of the agent: everything that writes.
// It runs for one writer term; ctx ends when the term ends.
func (a *Agent) runWriterTerm(ctx context.Context) {
	// Re-seed the hub-write health window: this replica may have been a
	// non-writer for hours, and Healthy() switches to hub-write freshness the
	// instant isWriter flips. Without the re-seed the liveness probe would
	// judge a brand-new writer by a clock that started before it was allowed
	// to write. The holder priority streak is NOT seeded: it starts with this
	// term's first real health renewal.
	a.noteHealthOK()
	a.resetHealthStreak()

	// Enter the term under mu, the same lock stepDown takes, so a term that
	// ended before this goroutine got to run can never flip isWriter back on.
	done := make(chan struct{})
	defer close(done)
	a.mu.Lock()
	if ctx.Err() != nil {
		a.mu.Unlock()
		return
	}
	a.writerCtx = ctx
	a.writerDone = done
	a.isWriter.Store(true)
	a.mu.Unlock()
	if a.metrics != nil {
		a.metrics.IsWriter.Set(1)
	}
	klog.InfoS("this replica is now the DC writer", "dcdr.dc", a.opts.DCName, "identity", a.opts.WriterIdentity)

	// Populate the standby-hold cache from the local ConfigMaps BEFORE any
	// elector starts: a fresh writer whose predecessor was holding a scope
	// standby must not contend for it during its own first seconds.
	if a.spoke != nil {
		a.reconcileStandbyHold(ctx)
	}

	// The health Lease renewals and the projector run only on the writer.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); a.runHealthRenewer(ctx) }()
	go func() { defer wg.Done(); a.runProjector(ctx) }()

	// Elector cold start. While this pod was a non-writer, reconcile kept the
	// holders cache warm but deliberately started no electors. Now that it may
	// write, run the ordinary reconcile path over EVERY listed Lease (force):
	// a Lease this DC holds gets no informer events once its previous renewer
	// is gone, and the holders cache being in perfect sync is exactly why a
	// drift-gated pass would skip it, so only an unconditional pass revives
	// contention for self-held scopes. Observed live on the bank pair: without
	// force, a clean roll started zero electors and every self-held Lease
	// froze until poked by hand.
	if _, err := a.checkObservation(ctx, true); err != nil {
		klog.ErrorS(err, "writer cold start: could not list Leases; electors resume on informer events")
	}

	<-ctx.Done()
	wg.Wait()
}

// stepDownWait bounds how long a step-down waits for the writer term's
// goroutines. Their requests are already canceled, so this is a backstop.
const stepDownWait = 5 * time.Second

// stepDown ends a writer term in process: writes stop before anything else, the
// electors are dropped (their contexts are canceled, and with ReleaseOnCancel
// false the hub Leases stay with this DC, so the next writer, this replica or
// another, resumes renewing them), and the term's goroutines are waited for.
// The hub observation (informer, holders cache, watchdog) keeps running. Safe to
// call when this replica never was the writer: client-go calls it on every
// election exit.
func (a *Agent) stepDown() {
	a.mu.Lock()
	wasWriter := a.isWriter.Load()
	a.isWriter.Store(false)
	a.writerCtx = nil
	done := a.writerDone
	a.writerDone = nil
	electors := make([]*scopeElector, 0, len(a.electors))
	for _, e := range a.electors {
		electors = append(electors, e)
	}
	a.electors = map[string]*scopeElector{}
	a.mu.Unlock()

	for _, e := range electors {
		e.stop()
	}
	a.resetHealthStreak()
	if a.metrics != nil {
		a.metrics.IsWriter.Set(0)
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(stepDownWait):
			klog.InfoS("writer term goroutines still finishing after step-down; their requests are canceled", "dcdr.dc", a.opts.DCName)
		}
	}
	if wasWriter {
		klog.InfoS("this replica is no longer the DC writer", "dcdr.dc", a.opts.DCName)
	}
}
