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
// contends for the primary DC Lease. Arbiter DCs never contend.
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
}

// New builds an Agent.
func New(opts Options, cs kubernetes.Interface, metrics *Metrics) *Agent {
	return &Agent{
		opts:     opts,
		cs:       cs,
		metrics:  metrics,
		electors: map[string]*scopeElector{},
	}
}

// Run starts the health renewer and the Lease informer, then blocks until ctx is done.
func (a *Agent) Run(ctx context.Context) error {
	a.rootCtx = ctx

	go a.runHealthRenewer(ctx)

	factory := informers.NewSharedInformerFactoryWithOptions(a.cs, 10*time.Minute, informers.WithNamespace(a.opts.Namespace))
	informer := factory.Coordination().V1().Leases().Informer()
	_, _ = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { a.onLease(obj) },
		UpdateFunc: func(_, obj interface{}) { a.onLease(obj) },
		DeleteFunc: func(obj interface{}) { a.onDelete(obj) },
	})

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return ctx.Err()
	}
	klog.InfoS("DC agent running", "dc", a.opts.DCName, "namespace", a.opts.Namespace)

	<-ctx.Done()
	a.stopAll()
	return ctx.Err()
}

func (a *Agent) onLease(obj interface{}) {
	l, ok := obj.(*coordinationv1.Lease)
	if !ok || !leases.IsPrimaryLeaseName(l.Name) {
		return
	}
	a.reconcile(l)
}

func (a *Agent) onDelete(obj interface{}) {
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
	a.mu.Unlock()
	if e != nil {
		e.stop()
	}
}

// reconcile drives this DC's contention for one primary DC Lease.
func (a *Agent) reconcile(l *coordinationv1.Lease) {
	scope, ok := leases.ScopeFromPrimaryLeaseName(l.Name)
	if !ok {
		return
	}
	members := l.Annotations[leases.AnnMemberDCs]
	handoffTo := l.Annotations[leases.AnnHandoffTo]
	holder := holderOf(l)
	isMember := leases.ContainsMember(members, a.opts.DCName)
	handoffTargetIsMember := handoffTo != "" && leases.ContainsMember(members, handoffTo)

	contend := desiredContend(isMember, handoffTo, holder, a.opts.DCName, handoffTargetIsMember)
	a.electorFor(scope, l.Name).setDesired(contend)

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
