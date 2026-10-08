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
	"fmt"
	"time"

	"kubeops.dev/dr-controlplane/pkg/leases"

	"github.com/spf13/pflag"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// ElectionConfig tunes the primary DC leader election. Every renewal is an etcd
// write that commits on a Raft majority, so it pays one inter DC round trip. Set
// the durations comfortably above the inter DC RTT.
type ElectionConfig struct {
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

// Options configures the DC agent.
type Options struct {
	// DCName is this data center's name. It is the Lease holderIdentity this agent uses.
	DCName string
	// Namespace holds the coordination Leases.
	Namespace string
	// Kubeconfig points at the coordination control plane. Empty means in cluster.
	Kubeconfig string
	// MetricsAddr is the address for the /metrics endpoint.
	MetricsAddr string

	HealthLeaseDuration time.Duration
	HealthRenewInterval time.Duration

	Election ElectionConfig

	// SpokeKubeconfig points at the local data center cluster where the database
	// pods run, the cluster the active DC marker is projected into. Empty means in
	// cluster, which is the common case: the agent runs in its own DC.
	SpokeKubeconfig string
	// MarkerNamespace is where the active DC marker ConfigMap is written on the
	// spoke. The pg-coordinator fence reads it from the same namespace.
	MarkerNamespace string
	// MarkerRefreshInterval is how often the marker renewTime is restamped. It must
	// be well under the fence TTL so a healthy marker always reads fresh.
	MarkerRefreshInterval time.Duration

	// WriterElection tunes the spoke-local election that picks THE ONE agent
	// replica allowed to write: the marker ConfigMap, the hub scope Leases, the
	// DC health Lease, and the break glass override annotations. The election
	// Lease lives on the SPOKE (in MarkerNamespace), deliberately not on the
	// coordination plane: a hub outage must never change which local pod holds
	// the writer role, and holding the role must have the same failure domain
	// as the writes it guards. Non-writer replicas keep their hub observation
	// warm (informer, holders, watchdog) so takeover starts from a fresh view.
	// Takeover after a writer crash is bounded by LeaseDuration, which must stay
	// comfortably inside the pg-coordinator fence TTL (30s) so an agent pod
	// death never fences a healthy DC.
	WriterElection ElectionConfig

	// ClientQPS and ClientBurst size the rate limiter of BOTH the coordination-plane
	// clients (RESTConfig: electors, informer relists, handoff writes, and the aux
	// health/watchdog client) and the spoke client (SpokeRESTConfig: writer election,
	// marker projection). Every scope elector shares one client, so the limiter must
	// be sized for the whole fleet of scopes, not a single controller. Observed live
	// (2026-08-07): the client-go default of 5 QPS under ~19 scopes queued requests
	// faster than they drained; renewals missed deadlines (130 leadership transitions
	// on one Lease), the reflector starved (markers froze 13+ minutes, fencing every
	// DC read only), and health renewals starved (liveness restarted the agents 180+
	// times in one night). Observed again 2026-09-11: a conflict storm (forked etcd +
	// orphan scopes) exhausted even 100 QPS ("client rate limiter Wait returned an
	// error"). The default is now effectively unthrottled; the API server's
	// priority-and-fairness is the real backpressure.
	ClientQPS   float32
	ClientBurst int
}

// DefaultOptions returns conservative WAN friendly defaults.
func DefaultOptions() Options {
	return Options{
		Namespace:             leases.DefaultNamespace,
		MetricsAddr:           ":8080",
		HealthLeaseDuration:   15 * time.Second,
		HealthRenewInterval:   5 * time.Second,
		MarkerNamespace:       leases.DefaultNamespace,
		MarkerRefreshInterval: 5 * time.Second,
		// Safety invariant: the consumer's marker fence TTL (pg-coordinator
		// dcMarkerTTL, 30s) plus cross-DC clock skew must be strictly LESS than
		// LeaseDuration. The marker renewTime tracks this Lease's renewTime, so a
		// partitioned active DC self-fences at lastRenew + fence TTL; a survivor can
		// only acquire the expired Lease at lastRenew + LeaseDuration. Keeping the
		// fence TTL inside LeaseDuration guarantees the old active DC goes read-only
		// before any new DC becomes writable, so there is no split-brain window.
		// (RetryPeriod stays small so the holder restamps renewTime well inside the
		// fence TTL during normal operation.)
		Election: ElectionConfig{
			LeaseDuration: 45 * time.Second,
			RenewDeadline: 30 * time.Second,
			RetryPeriod:   2 * time.Second,
		},
		WriterElection: ElectionConfig{
			LeaseDuration: 15 * time.Second,
			RenewDeadline: 10 * time.Second,
			RetryPeriod:   2 * time.Second,
		},
		ClientQPS:   50000,
		ClientBurst: 50000,
	}
}

// AddFlags binds the options to a flag set.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.DCName, "dc-name", o.DCName, "Name of this data center (the Lease holderIdentity).")
	fs.StringVar(&o.Namespace, "namespace", o.Namespace, "Namespace holding the coordination Leases.")
	fs.StringVar(&o.Kubeconfig, "kubeconfig", o.Kubeconfig, "Path to the coordination control plane kubeconfig. Empty means in cluster.")
	fs.StringVar(&o.MetricsAddr, "metrics-addr", o.MetricsAddr, "Address for the Prometheus /metrics endpoint.")
	fs.DurationVar(&o.HealthLeaseDuration, "health-lease-duration", o.HealthLeaseDuration, "leaseDurationSeconds for this DC's health Lease.")
	fs.DurationVar(&o.HealthRenewInterval, "health-renew-interval", o.HealthRenewInterval, "How often to renew the health Lease.")
	fs.DurationVar(&o.Election.LeaseDuration, "election-lease-duration", o.Election.LeaseDuration, "Primary DC Lease duration (the failover signal RTO floor).")
	fs.DurationVar(&o.Election.RenewDeadline, "election-renew-deadline", o.Election.RenewDeadline, "Primary DC Lease renew deadline.")
	fs.DurationVar(&o.Election.RetryPeriod, "election-retry-period", o.Election.RetryPeriod, "Primary DC Lease retry period.")
	fs.StringVar(&o.SpokeKubeconfig, "spoke-kubeconfig", o.SpokeKubeconfig, "Path to the local DC cluster kubeconfig where the active DC marker is projected. Empty means in cluster.")
	fs.StringVar(&o.MarkerNamespace, "marker-namespace", o.MarkerNamespace, "Namespace on the spoke where the active DC marker ConfigMap is written.")
	fs.DurationVar(&o.MarkerRefreshInterval, "marker-refresh-interval", o.MarkerRefreshInterval, "How often the active DC marker renewTime is restamped (must be well under the fence TTL).")
	fs.Float32Var(&o.ClientQPS, "client-qps", o.ClientQPS, "QPS for the coordination control plane client. Size for the whole scope fleet: electors, informer relists and handoffs share this budget.")
	fs.IntVar(&o.ClientBurst, "client-burst", o.ClientBurst, "Burst for the coordination control plane client.")
	fs.DurationVar(&o.WriterElection.LeaseDuration, "writer-lease-duration", o.WriterElection.LeaseDuration, "Spoke-local writer election Lease duration (bounds takeover after a writer pod crash; keep well under the 30s fence TTL).")
	fs.DurationVar(&o.WriterElection.RenewDeadline, "writer-renew-deadline", o.WriterElection.RenewDeadline, "Spoke-local writer election renew deadline.")
	fs.DurationVar(&o.WriterElection.RetryPeriod, "writer-retry-period", o.WriterElection.RetryPeriod, "Spoke-local writer election retry period.")
}

// Validate checks the options.
func (o Options) Validate() error {
	if o.DCName == "" {
		return fmt.Errorf("--dc-name is required")
	}
	if o.HealthRenewInterval <= 0 || o.HealthRenewInterval >= o.HealthLeaseDuration {
		return fmt.Errorf("health-renew-interval must be positive and less than health-lease-duration")
	}
	if o.Election.RenewDeadline >= o.Election.LeaseDuration {
		return fmt.Errorf("election-renew-deadline must be less than election-lease-duration")
	}
	if o.Election.RetryPeriod <= 0 || o.Election.RetryPeriod >= o.Election.RenewDeadline {
		return fmt.Errorf("election-retry-period must be positive and less than election-renew-deadline")
	}
	if o.ClientQPS <= 0 || o.ClientBurst <= 0 {
		return fmt.Errorf("client-qps and client-burst must be positive")
	}
	if o.WriterElection.RenewDeadline >= o.WriterElection.LeaseDuration {
		return fmt.Errorf("writer-renew-deadline must be less than writer-lease-duration")
	}
	if o.WriterElection.RetryPeriod <= 0 || o.WriterElection.RetryPeriod >= o.WriterElection.RenewDeadline {
		return fmt.Errorf("writer-retry-period must be positive and less than writer-renew-deadline")
	}
	return nil
}

// RESTConfig builds the client config for the coordination control plane.
func (o Options) RESTConfig() (*rest.Config, error) {
	var cfg *rest.Config
	var err error
	if o.Kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", o.Kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, err
	}
	cfg.QPS = o.ClientQPS
	cfg.Burst = o.ClientBurst
	return cfg, nil
}

// SpokeRESTConfig builds the client config for the local DC cluster where the
// active DC marker is projected. Empty SpokeKubeconfig means in cluster. It gets
// the same generous limiter as the coordination clients: marker projection writes
// scale with the scope count (one restamp per scope per refresh tick) and at the
// client-go default of 5 QPS ~15 scopes already brush the ceiling, which would
// throttle marker restamps — a fence-staleness input.
func (o Options) SpokeRESTConfig() (*rest.Config, error) {
	var cfg *rest.Config
	var err error
	if o.SpokeKubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", o.SpokeKubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, err
	}
	cfg.QPS = o.ClientQPS
	cfg.Burst = o.ClientBurst
	return cfg, nil
}
