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

	"open-cluster-management.io/dr-controlplane/pkg/leases"

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
	return nil
}

// RESTConfig builds the client config for the coordination control plane.
func (o Options) RESTConfig() (*rest.Config, error) {
	if o.Kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", o.Kubeconfig)
	}
	return rest.InClusterConfig()
}

// SpokeRESTConfig builds the client config for the local DC cluster where the
// active DC marker is projected. Empty SpokeKubeconfig means in cluster.
func (o Options) SpokeRESTConfig() (*rest.Config, error) {
	if o.SpokeKubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", o.SpokeKubeconfig)
	}
	return rest.InClusterConfig()
}
