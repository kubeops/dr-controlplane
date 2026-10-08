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

// Package agent is the per data center DC agent. One runs in each data center.
// It renews the DC's health Lease, and for the scopes where its DC is a Member it
// contends for the primary DC Lease. Arbiter and Witness DCs never contend.
package agent

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kluster-manager/dr-controlplane/pkg/leases"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Agent orchestrates the health Lease renewal and the per scope leader election.
type Agent struct {
	opts    Options
	cs      kubernetes.Interface
	metrics *Metrics

	rootCtx context.Context

	mu          sync.Mutex
	electors    map[string]*scopeElector // keyed by primary Lease name
	holders     map[string]markerState   // keyed by primary Lease name
	standbyHold map[string]bool          // keyed by primary Lease name, see standbyhold.go

	// lastHealthOK is the UnixNano of the last SUCCESSFUL health Lease renewal, i.e. the
	// last time this agent could actually reach the coordination control plane. Seeded at
	// construction so process startup is covered by the same staleness window.
	lastHealthOK atomic.Int64

	// aux is a second clientset for the liveness-critical paths (health Lease renewal
	// and the observation watchdog's List). It shares the server but NOT the rate
	// limiter with cs, so a flood of elector traffic can never starve the health
	// signal or the watchdog. Falls back to cs when nil (tests).
	aux kubernetes.Interface

	// spoke is the controller-runtime cached client for the LOCAL spoke cluster,
	// from the writer manager (see writer.go). All spoke reads (markers, override
	// and standby-hold ConfigMaps) go through its informer cache; writes hit the
	// API server directly.
	spoke client.Client

	// isWriter is true only on the replica that won the spoke-local writer
	// election. Every write path (electors, health renewals, marker projection,
	// override reconciliation) is gated on it; non-writers only observe.
	isWriter atomic.Bool

	// observationSynced is set once the hub Lease informer has synced. Kept for
	// introspection; deliberately NOT a liveness input (see Healthy).
	observationSynced atomic.Bool

	// lastAuthFailure is the UnixNano of the last hub write that failed with a
	// credential-class error (x509, unauthorized, invalid token). Only these
	// failures make the liveness probe restart the pod, because a restart
	// re-reads the mounted kubeconfig Secret and that is the ONLY failure a
	// restart can fix. Observed live 2026-08-07: failing the probe on plain
	// unreachability liveness-killed the writer every ~90s for the whole hub
	// outage, each SIGTERM released the writer lease, the role bounced between
	// replicas, and when the hub returned the marker path took minutes longer
	// to settle than the outage itself.
	lastAuthFailure atomic.Int64

	// informerCancel stops the current Lease informer; the observation watchdog uses
	// it to rebuild a wedged informer without restarting the process.
	informerMu     sync.Mutex
	informerCancel context.CancelFunc
}

// HealthStaleAfter is how long an agent may go without a successful coordination-plane
// write before /healthz starts failing.
//
// Deliberately generous: this drives a liveness probe, so it must ignore transient API
// blips and fire only on a SUSTAINED inability to reach the control plane. That is the
// case worth restarting for, because the credential is mounted from a Secret and a
// restart is what picks up a rotated one.
const HealthStaleAfter = 90 * time.Second

// Healthy reports whether restarting this replica could make anything better.
// That is the ONLY question a liveness probe answers; "the hub is down" is not a
// pod defect and a restart does not bring the hub back.
//
// The WRITER fails the probe only when BOTH hold: its hub writes have not
// succeeded within HealthStaleAfter, AND the recent failures are
// credential-class (x509, unauthorized, invalid token). Then a restart genuinely
// helps: it re-reads the mounted kubeconfig Secret. This preserves the fix for
// the live incident where a control-plane rebuild minted a new CA and all three
// agents failed every call with "x509: certificate signed by unknown authority"
// for ~35 minutes without anything restarting them or reloading the credential.
// Plain unreachability (connection refused, timeouts) keeps the pod alive: the
// fence already protects the databases, the elector and watchdog keep retrying,
// and restarting the writer on every probe window was observed live to bounce
// the writer role between replicas for the whole outage and delay marker
// recovery past the outage itself.
//
// A NON-WRITER never writes to the hub by design, so it is always healthy while
// the process runs. If its credentials are rotten it will restart on its first
// promotion to writer, which is exactly when the credential starts mattering.
func (a *Agent) Healthy() bool {
	if !a.isWriter.Load() {
		return true
	}
	last := a.lastHealthOK.Load()
	if last != 0 && time.Since(time.Unix(0, last)) <= HealthStaleAfter {
		return true
	}
	authFail := a.lastAuthFailure.Load()
	return authFail == 0 || time.Since(time.Unix(0, authFail)) > HealthStaleAfter
}

// noteAuthFailure records a credential-class hub write failure.
func (a *Agent) noteAuthFailure() { a.lastAuthFailure.Store(time.Now().UnixNano()) }

// credentialClassError reports whether err smells like a credential problem that a
// pod restart (re-reading the mounted kubeconfig) could fix, as opposed to plain
// unreachability, which it cannot.
func credentialClassError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, marker := range []string{
		"x509:", "certificate", "Unauthorized", "unauthorized",
		"invalid bearer token", "credentials", "Forbidden", "forbidden",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

// noteHealthOK records a successful coordination-plane write.
func (a *Agent) noteHealthOK() { a.lastHealthOK.Store(time.Now().UnixNano()) }

// markerState is the per scope value the projector writes to the spoke: which DC
// the quorum trusts, the Lease renewTime that proves the view is current, and the
// DC (if any) the hub has asked to quiesce for a planned switchover. It also
// carries the observed override-hold annotation so the override reconciler can
// decide from the informer view instead of an uncached hub Get per scope per tick.
type markerState struct {
	dc           string
	renew        time.Time
	quiesce      string
	overrideHold string
}

// New builds an Agent. aux is an optional second clientset for the
// liveness-critical paths (see the field comment); nil falls back to cs.
func New(opts Options, cs, aux kubernetes.Interface, metrics *Metrics) *Agent {
	if aux == nil {
		aux = cs
	}
	a := &Agent{
		opts:        opts,
		cs:          cs,
		aux:         aux,
		metrics:     metrics,
		electors:    map[string]*scopeElector{},
		holders:     map[string]markerState{},
		standbyHold: map[string]bool{},
	}
	a.noteHealthOK()
	return a
}

// Run starts the observation half on every replica (hub Lease informer plus the
// watchdog), then hands control to the spoke writer manager. The write half
// (health renewals, marker projection, electors, override reconciliation) runs
// inside writerRunnable, which the manager starts only on the replica that wins
// the spoke-local writer election. Losing the writer role stops the manager and
// Run returns its error; the process exits and rejoins as a non-writer.
func (a *Agent) Run(ctx context.Context) error {
	a.rootCtx = ctx

	mgr, err := a.newSpokeManager()
	if err != nil {
		return err
	}
	a.spoke = mgr.GetClient()

	if err := a.startInformer(ctx); err != nil {
		return err
	}
	a.observationSynced.Store(true)
	klog.InfoS("DC agent running", "dcdr.dc", a.opts.DCName, "namespace", a.opts.Namespace)

	go a.runObservationWatchdog(ctx)

	if err := mgr.Add(writerRunnable{a}); err != nil {
		return err
	}
	if err := mgr.Start(ctx); err != nil {
		a.stopAll()
		return fmt.Errorf("spoke writer manager stopped: %w", err)
	}
	a.stopAll()
	return ctx.Err()
}

// startInformer builds and syncs a fresh Lease informer. The previous informer, if
// any, is stopped first. The observation watchdog calls this again when it proves
// the informer's view has fallen behind the server (a hung watch); everything else
// about the agent keeps running across the swap.
func (a *Agent) startInformer(ctx context.Context) error {
	a.informerMu.Lock()
	if a.informerCancel != nil {
		a.informerCancel()
	}
	ictx, cancel := context.WithCancel(ctx)
	a.informerCancel = cancel
	a.informerMu.Unlock()

	factory := informers.NewSharedInformerFactoryWithOptions(a.cs, 10*time.Minute, informers.WithNamespace(a.opts.Namespace))
	informer := factory.Coordination().V1().Leases().Informer()
	_, _ = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { a.onLease(obj) },
		UpdateFunc: func(_, obj any) { a.onLease(obj) },
		DeleteFunc: func(obj any) { a.onDelete(obj) },
	})

	factory.Start(ictx.Done())
	if !cache.WaitForCacheSync(ictx.Done(), informer.HasSynced) {
		return ctx.Err()
	}
	return nil
}

func (a *Agent) onLease(obj any) {
	l, ok := obj.(*coordinationv1.Lease)
	if !ok || !leases.IsPrimaryLeaseName(l.Name) {
		return
	}
	a.reconcile(l)
}

func (a *Agent) onDelete(obj any) {
	l, ok := obj.(*coordinationv1.Lease)
	if !ok {
		tomb, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		l, ok = tomb.Obj.(*coordinationv1.Lease)
		if !ok {
			return
		}
	}
	if !leases.IsPrimaryLeaseName(l.Name) {
		return
	}
	a.mu.Lock()
	e := a.electors[l.Name]
	delete(a.electors, l.Name)
	delete(a.holders, l.Name)
	a.mu.Unlock()
	if e != nil {
		e.stop()
	}
}

// reconcile drives this DC's contention for one primary DC Lease.
//
// Cold-start safety (A43(c)): electorFor only ever creates and starts a scope's
// elector from inside this function, and this function only ever runs with a
// Lease actually read from the control plane: either the informer delivered an
// event for it (cache.WaitForCacheSync guarantees that happens for every
// pre-existing Lease before startInformer returns) or the observation watchdog
// listed it directly after proving the informer's view had fallen behind. So a
// freshly started or restarted agent has zero electors, hence contends for
// nothing, until it has read the current Lease state at least once; there is no
// code path that can start contention from a nil or never-observed Lease. This
// is a structural property of the call graph (electorFor has exactly one call
// site, right here), not a runtime flag, so keep it that way: do not add a
// path into electorFor that bypasses an observed Lease.
func (a *Agent) reconcile(l *coordinationv1.Lease) {
	scope, ok := leases.ScopeFromPrimaryLeaseName(l.Name)
	if !ok {
		return
	}
	members := l.Annotations[leases.AnnMemberDCs]
	handoffTo := l.Annotations[leases.AnnHandoffTo]
	overrideHold := l.Annotations[leases.AnnOverrideHold]
	holder := holderOf(l)
	isMember := leases.ContainsMember(members, a.opts.DCName)
	handoffTargetIsMember := handoffTo != "" && leases.ContainsMember(members, handoffTo)
	followDC := l.Annotations[leases.AnnFollowDC]
	followIsMember := followDC != "" && leases.ContainsMember(members, followDC)

	// standbyHold is never on the Lease (see standbyhold.go); it is this DC's
	// own cached read of its local standby-hold ConfigMap, refreshed on the
	// same ticker as the marker projector and override reconciler.
	a.mu.Lock()
	standbyHold := a.standbyHold[l.Name]
	a.mu.Unlock()

	// Contention and the handoff release are write paths: only the replica
	// holding the spoke-local writer role runs them (see writer.go). Non-writer
	// replicas fall through to the holders update below so their observation
	// stays warm for takeover.
	if a.isWriter.Load() {
		contend := desiredContend(isMember, handoffTo, holder, a.opts.DCName, handoffTargetIsMember, overrideHold, standbyHold, followDC, followIsMember)
		a.electorFor(scope, l.Name).setDesired(contend)

		// A coordinated handoff is the only path that actively moves the Lease: the
		// holder steps aside by releasing it exactly once, after its elector has been
		// paused just above (desiredContend is false for the holder while a handoff
		// to another Member is pending). Nothing else releases, ever; on every other
		// path (agent restarts, member changes, override or standby holds) the Lease
		// moves only by expiring after the holder stops renewing it.
		if handoffTo != "" && handoffTargetIsMember && handoffTo != a.opts.DCName && holder == a.opts.DCName {
			a.releaseForHandoff(a.rootCtx, l.Name, handoffTo)
		}
	}

	// Record the holder and the Lease renewTime for the projector. Tying the marker
	// freshness to the Lease renewTime makes the fence fail closed: if this agent is
	// partitioned from the coordination plane the renewTime stops advancing, the
	// marker goes stale, and the local leader demotes itself.
	var renew time.Time
	if l.Spec.RenewTime != nil {
		renew = l.Spec.RenewTime.Time
	}
	// Carry a planned switchover quiesce request through to the marker so the active
	// DC's coordinator can hold its primary read only while the target catches up.
	quiesce := l.Annotations[leases.AnnQuiesce]
	a.mu.Lock()
	a.holders[l.Name] = markerState{dc: holder, renew: renew, quiesce: quiesce, overrideHold: overrideHold}
	a.mu.Unlock()

	// Once the target holds the Lease, the holder clears the handoff annotation
	// so the system returns to normal contention. A hub write, so writer only.
	if a.isWriter.Load() && handoffTo != "" && holder == handoffTo && holder == a.opts.DCName {
		a.clearHandoff(a.rootCtx, l.Name)
	}
}

func (a *Agent) electorFor(scope leases.Scope, name string) *scopeElector {
	a.mu.Lock()
	defer a.mu.Unlock()
	e, ok := a.electors[name]
	if !ok {
		e = newScopeElector(a.rootCtx, a, scope)
		a.electors[name] = e
	}
	return e
}

func (a *Agent) stopAll() {
	a.mu.Lock()
	electors := make([]*scopeElector, 0, len(a.electors))
	for _, e := range a.electors {
		electors = append(electors, e)
	}
	a.mu.Unlock()
	for _, e := range electors {
		e.stop()
	}
}
