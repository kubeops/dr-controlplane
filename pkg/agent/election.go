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
	"sync"
	"time"

	"open-cluster-management.io/dr-controlplane/pkg/leases"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
)

// scopeElector runs (or pauses) this DC's leader election participant for one
// primary DC Lease. Only Member data centers ever run one. Pausing is how the
// coordinated failback releases the Lease for a preferred DC: stopping the
// elector with ReleaseOnCancel releases the Lease if this DC held it, and a non
// target candidate that pauses stops racing the target.
type scopeElector struct {
	scope  leases.Scope
	a      *Agent
	parent context.Context

	mu      sync.Mutex
	cancel  context.CancelFunc
	running bool
}

func newScopeElector(parent context.Context, a *Agent, scope leases.Scope) *scopeElector {
	return &scopeElector{scope: scope, a: a, parent: parent}
}

// setDesired starts or stops the elector to match run.
func (e *scopeElector) setDesired(run bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if run == e.running {
		return
	}
	label := e.scope.String()
	if run {
		ctx, cancel := context.WithCancel(e.parent)
		e.cancel = cancel
		e.running = true
		e.a.metrics.Contending.WithLabelValues(label).Set(1)
		klog.InfoS("contending for primary DC Lease", "scope", label, "dc", e.a.opts.DCName)
		go e.run(ctx)
	} else {
		if e.cancel != nil {
			e.cancel()
			e.cancel = nil
		}
		e.running = false
		e.a.metrics.Contending.WithLabelValues(label).Set(0)
		e.a.metrics.IsPrimary.WithLabelValues(label).Set(0)
		klog.InfoS("paused/stopped contending for primary DC Lease", "scope", label, "dc", e.a.opts.DCName)
	}
}

func (e *scopeElector) stop() { e.setDesired(false) }

// run keeps a leader election participant alive until the elector is stopped.
// client-go's election returns after losing leadership, so it is wrapped in a
// re-contend loop.
func (e *scopeElector) run(ctx context.Context) {
	label := e.scope.String()
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      e.scope.PrimaryLeaseName(),
			Namespace: e.a.opts.Namespace,
		},
		Client:     e.a.cs.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: e.a.opts.DCName},
	}
	cfg := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   e.a.opts.Election.LeaseDuration,
		RenewDeadline:   e.a.opts.Election.RenewDeadline,
		RetryPeriod:     e.a.opts.Election.RetryPeriod,
		Name:            e.scope.PrimaryLeaseName(),
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(context.Context) {
				e.a.metrics.IsPrimary.WithLabelValues(label).Set(1)
				e.a.metrics.ElectionTransitions.WithLabelValues(label, "acquired").Inc()
				klog.InfoS("this DC is now primary", "scope", label, "dc", e.a.opts.DCName)
			},
			OnStoppedLeading: func() {
				e.a.metrics.IsPrimary.WithLabelValues(label).Set(0)
				e.a.metrics.ElectionTransitions.WithLabelValues(label, "lost").Inc()
				klog.InfoS("this DC is no longer primary", "scope", label, "dc", e.a.opts.DCName)
			},
			OnNewLeader: func(id string) {
				if id != e.a.opts.DCName && id != "" {
					klog.InfoS("observed primary DC", "scope", label, "holder", id)
				}
			},
		},
	}
	for {
		if ctx.Err() != nil {
			return
		}
		// Returns when ctx is canceled or after this DC loses leadership.
		leaderelection.RunOrDie(ctx, cfg)
		select {
		case <-ctx.Done():
			return
		case <-time.After(e.a.opts.Election.RetryPeriod):
		}
	}
}
