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
}

// DefaultOptions returns conservative WAN friendly defaults.
func DefaultOptions() Options {
	return Options{
		Namespace:           leases.DefaultNamespace,
		MetricsAddr:         ":8080",
		HealthLeaseDuration: 15 * time.Second,
		HealthRenewInterval: 5 * time.Second,
		Election: ElectionConfig{
			LeaseDuration: 15 * time.Second,
			RenewDeadline: 10 * time.Second,
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
