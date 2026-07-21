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
	"sync"
	"time"

	"open-cluster-management.io/dr-controlplane/pkg/leases"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// Agent orchestrates the health Lease renewal and the per scope leader election.
type Agent struct {
	opts    Options
	cs      kubernetes.Interface
	metrics *Metrics

	rootCtx context.Context

	mu       sync.Mutex
	electors map[string]*scopeElector // keyed by primary Lease name
	holders  map[string]markerState   // keyed by primary Lease name
}

// markerState is the per scope value the projector writes to the spoke: which DC
// the quorum trusts, the Lease renewTime that proves the view is current, and the
// DC (if any) the hub has asked to quiesce for a planned switchover.
type markerState struct {
	dc      string
	renew   time.Time
	quiesce string
}

// New builds an Agent.
func New(opts Options, cs kubernetes.Interface, metrics *Metrics) *Agent {
	return &Agent{
		opts:     opts,
		cs:       cs,
		metrics:  metrics,
		electors: map[string]*scopeElector{},
		holders:  map[string]markerState{},
	}
}

// Run starts the health renewer and the Lease informer, then blocks until ctx is done.
func (a *Agent) Run(ctx context.Context) error {
	a.rootCtx = ctx

	go a.runHealthRenewer(ctx)
	go a.runProjector(ctx)

	factory := informers.NewSharedInformerFactoryWithOptions(a.cs, 10*time.Minute, informers.WithNamespace(a.opts.Namespace))
	informer := factory.Coordination().V1().Leases().Informer()
	_, _ = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { a.onLease(obj) },
		UpdateFunc: func(_, obj any) { a.onLease(obj) },
		DeleteFunc: func(obj any) { a.onDelete(obj) },
	})

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return ctx.Err()
	}
	klog.InfoS("DC agent running", "dcdr.dc", a.opts.DCName, "namespace", a.opts.Namespace)

	<-ctx.Done()
	a.stopAll()
	return ctx.Err()
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
// elector from inside this function, and this function only ever runs when the
// Lease informer delivers an event for that Lease, which cache.WaitForCacheSync
// guarantees happens for every pre-existing Lease before Run returns. So a
// freshly started or restarted agent has zero electors, hence contends for
// nothing, until it has read the current Lease state at least once; there is no
// code path that can start contention from a nil or never-observed Lease. This
// is a structural property of the call graph (electorFor has exactly one call
// site, right here), not a runtime flag, so keep it that way: do not add a
// second path into electorFor that bypasses an observed Lease.
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

	contend := desiredContend(isMember, handoffTo, holder, a.opts.DCName, handoffTargetIsMember, overrideHold)
	a.electorFor(scope, l.Name).setDesired(contend)

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
	a.holders[l.Name] = markerState{dc: holder, renew: renew, quiesce: quiesce}
	a.mu.Unlock()

	// Once the target holds the Lease, the holder clears the handoff annotation
	// so the system returns to normal contention.
	if handoffTo != "" && holder == handoffTo && holder == a.opts.DCName {
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
