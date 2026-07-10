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

// Package client is the small library that consumers (the KubeDB DR driver and
// any other DC aware application) import to read the failover signal.
//
// Contract: the primary DC reported here is a DC ownership signal, the data
// center the quorum currently trusts. It is NOT a per database data currency
// signal. A consumer must still apply its own lag guard before promoting in the
// new primary DC, and surface its own Degraded state if its data there is not
// safely promotable. This library never promotes or demotes anything.
package client

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

// ChangeFunc is invoked when a primary DC Lease changes hands. A DC failover.
type ChangeFunc func(oldDC, newDC string)

// Coordinator watches the primary DC Leases and exposes the current holder per scope.
type Coordinator struct {
	localDC string
	ns      string

	factory  informers.SharedInformerFactory
	informer cache.SharedIndexInformer

	mu      sync.RWMutex
	holders map[string]string       // primary lease name -> holderIdentity (the primary DC)
	subs    map[string][]ChangeFunc // primary lease name -> change callbacks
}

// New builds a Coordinator for the given namespace and the local data center name.
func New(cs kubernetes.Interface, namespace, localDC string) *Coordinator {
	if namespace == "" {
		namespace = leases.DefaultNamespace
	}
	factory := informers.NewSharedInformerFactoryWithOptions(cs, 10*time.Minute, informers.WithNamespace(namespace))
	c := &Coordinator{
		localDC:  localDC,
		ns:       namespace,
		factory:  factory,
		informer: factory.Coordination().V1().Leases().Informer(),
		holders:  map[string]string{},
		subs:     map[string][]ChangeFunc{},
	}
	_, _ = c.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { c.onLease(obj) },
		UpdateFunc: func(_, obj any) { c.onLease(obj) },
		DeleteFunc: func(obj any) { c.onDelete(obj) },
	})
	return c
}

// LocalDC returns the configured local data center name.
func (c *Coordinator) LocalDC() string { return c.localDC }

// Run starts the informer and blocks until the cache is synced or ctx is done.
func (c *Coordinator) Run(ctx context.Context) error {
	c.factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), c.informer.HasSynced) {
		return ctx.Err()
	}
	klog.InfoS("coordinator synced", "namespace", c.ns, "localDC", c.localDC)
	<-ctx.Done()
	return ctx.Err()
}

// PrimaryDC returns the current primary data center for a scope and whether it is known.
func (c *Coordinator) PrimaryDC(s leases.Scope) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	dc, ok := c.holders[s.PrimaryLeaseName()]
	return dc, ok && dc != ""
}

// IsLocalDCPrimary reports whether the local data center holds the primary DC Lease for a scope.
func (c *Coordinator) IsLocalDCPrimary(s leases.Scope) bool {
	dc, ok := c.PrimaryDC(s)
	return ok && dc == c.localDC
}

// OnPrimaryDCChange registers a callback fired on every holder change for a scope.
// Safe to call before Run; callbacks fire on subsequent changes.
func (c *Coordinator) OnPrimaryDCChange(s leases.Scope, fn ChangeFunc) {
	name := s.PrimaryLeaseName()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subs[name] = append(c.subs[name], fn)
}

func holderOf(l *coordinationv1.Lease) string {
	if l == nil || l.Spec.HolderIdentity == nil {
		return ""
	}
	return *l.Spec.HolderIdentity
}

func (c *Coordinator) onLease(obj any) {
	l, ok := obj.(*coordinationv1.Lease)
	if !ok || !leases.IsPrimaryLeaseName(l.Name) {
		return
	}
	c.apply(l.Name, holderOf(l))
}

func (c *Coordinator) onDelete(obj any) {
	l, ok := obj.(*coordinationv1.Lease)
	if !ok {
		if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			l, ok = tomb.Obj.(*coordinationv1.Lease)
			if !ok {
				return
			}
		} else {
			return
		}
	}
	if !leases.IsPrimaryLeaseName(l.Name) {
		return
	}
	c.apply(l.Name, "")
}

// apply records a new holder for a lease and fires callbacks if it changed.
func (c *Coordinator) apply(name, newDC string) {
	c.mu.Lock()
	oldDC := c.holders[name]
	if oldDC == newDC {
		c.mu.Unlock()
		return
	}
	c.holders[name] = newDC
	subs := append([]ChangeFunc(nil), c.subs[name]...)
	c.mu.Unlock()

	klog.InfoS("primary DC changed", "lease", name, "from", oldDC, "to", newDC)
	for _, fn := range subs {
		fn(oldDC, newDC)
	}
}
