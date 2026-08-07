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
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
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
// by a controller-runtime manager doing leader election against the SPOKE
// cluster: same failure domain as the writes it guards, so a coordination
// plane outage can never change which local pod holds the pen. Non-writer
// replicas stay hot: they run the hub Lease informer, the holders cache, and
// the observation watchdog, so a takeover starts from a warm view and only has
// to win the local election (bounded by WriterElection.LeaseDuration, which is
// kept well inside the 30s fence TTL).
//
// Losing the writer role stops the manager, which ends Run with an error and
// exits the process; the kubelet restarts the pod and it rejoins as a
// non-writer. That is deliberate: an in-place demotion would have to unwind
// every write path mid-flight, and a clean restart is both simpler and exactly
// what the hub Leases are built to survive (holder identity is the DC name, so
// a successor pod resumes renewing the same Leases without any transition).

// writerLeaseName is the spoke-local election Lease, one per DC.
func writerLeaseName(dc string) string {
	return "dr-agent-writer-" + dc
}

var writerScheme = runtime.NewScheme()

func init() {
	_ = clientgoscheme.AddToScheme(writerScheme)
}

// newSpokeManager builds the controller-runtime manager that owns the writer
// election and the spoke-side cached client. Reads of spoke objects (marker
// ConfigMaps, standby-hold and override ConfigMaps, the writer Lease) go
// through mgr.GetClient(), which is informer-cache backed; writes go through
// the same client and hit the API server directly.
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
		LeaderElection:             true,
		LeaderElectionID:           writerLeaseName(a.opts.DCName),
		LeaderElectionNamespace:    a.opts.MarkerNamespace,
		LeaderElectionResourceLock: "leases",
		LeaseDuration:              &a.opts.WriterElection.LeaseDuration,
		RenewDeadline:              &a.opts.WriterElection.RenewDeadline,
		RetryPeriod:                &a.opts.WriterElection.RetryPeriod,
		// Release on shutdown so a planned roll hands the pen over in one retry
		// period instead of a full LeaseDuration. Safe for THIS lease because it
		// is pod-scoped and local; the hub scope Leases (DC-scoped identity)
		// must never do this, see election.go.
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		return nil, fmt.Errorf("spoke manager: %w", err)
	}
	if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
		return nil, err
	}
	return mgr, nil
}

// writerRunnable is the leader-gated half of the agent: everything that writes.
// controller-runtime starts it only after this pod wins the writer election.
type writerRunnable struct{ a *Agent }

// NeedLeaderElection gates this runnable behind the writer election.
func (w writerRunnable) NeedLeaderElection() bool { return true }

func (w writerRunnable) Start(ctx context.Context) error {
	a := w.a
	// Re-seed the hub-write health window: this replica may have been a
	// non-writer for hours, and Healthy() switches to hub-write freshness the
	// instant isWriter flips. Without the re-seed the liveness probe would
	// judge a brand-new writer by a clock that started before it was allowed
	// to write.
	a.noteHealthOK()
	a.isWriter.Store(true)
	if a.metrics != nil {
		a.metrics.IsWriter.Set(1)
	}
	klog.InfoS("this replica is now the DC writer", "dcdr.dc", a.opts.DCName)

	// Populate the standby-hold cache from the local ConfigMaps BEFORE any
	// elector starts: a fresh writer whose predecessor was holding a scope
	// standby must not contend for it during its own first seconds.
	a.reconcileStandbyHold(ctx)

	// The health Lease renewals and the projector run only on the writer.
	go a.runHealthRenewer(ctx)
	go a.runProjector(ctx)

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
	return nil
}
