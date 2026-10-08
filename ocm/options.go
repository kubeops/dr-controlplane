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

package ocm

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/pflag"
)

const (
	// defaultCoordKubeconfigSecret is the Secret the agent mounts for coordination
	// apiserver auth. It must already exist in the agent install namespace on every
	// spoke; the addon does not create it.
	defaultCoordKubeconfigSecret = "coord-kubeconfig"

	// defaultAgentInstallNamespace is where the agent is installed on each spoke.
	// It is NOT the addon-framework default (open-cluster-management-agent-addon):
	// the coordination credential Secret lives in dc-failover, so the agent must
	// run there or its pods never get past ContainerCreating.
	defaultAgentInstallNamespace = "dc-failover"

	// defaultAgentImageRepository is the image the agent Deployment runs.
	defaultAgentImageRepository = "ghcr.io/appscode/dr-controlplane"

	// markerFenceTTL is the TTL the database data plane (pg-coordinator) applies to
	// the active DC marker ConfigMap this agent projects. The data plane fails
	// closed once the marker is older than this, so it bounds how long a partitioned
	// primary DC can still accept writes. The election Lease duration MUST stay
	// strictly above it: a survivor may only acquire the expired primary DC Lease at
	// lastRenew+LeaseDuration, and that must land after the old primary has already
	// self fenced at lastRenew+markerFenceTTL. Keep the two in sync with
	// pg-coordinator's dcMarkerTTL.
	markerFenceTTL = 30 * time.Second
)

// AgentOptions are the spoke agent tunables the addon manager injects into the
// embedded dr-controlplane-agent chart. Everything the per DC agent needs that
// used to be baked into the chart defaults travels this one path: flags on the
// addon-manager subcommand (with env fallbacks) -> AgentOptions -> getValues ->
// chart values -> the rendered agent Deployment.
type AgentOptions struct {
	// InstallNamespace is the namespace the agent is installed into on each spoke.
	InstallNamespace string
	// CreateNamespace makes the rendered ManifestWork own the target Namespace.
	// Off by default: the agent shares the pre existing dc-failover namespace with
	// the coordination credential, the markers, and (on some clusters) etcd, so an
	// addon deletion must never garbage collect it.
	CreateNamespace bool
	// CoordKubeconfigSecret is the Secret (key: kubeconfig) the agent mounts to
	// reach the coordination control plane. Empty disables the mount and the agent
	// falls back to its in cluster config.
	CoordKubeconfigSecret string

	// CoordKubeconfigSourceSecret is the Secret ON THE HUB holding the coordination
	// control plane kubeconfig that the addon copies to every managed cluster, and
	// CoordExternalEndpoint is the address it is rewritten to point at.
	//
	// Without this the coordination credential has to be extracted and applied to every
	// spoke by hand, and re-done every time the control plane mints a new CA (any rebuild
	// or data-dir loss). That was five manual steps and the single biggest cause of a
	// silently dead DR service: agents that cannot authenticate log x509 forever.
	// Empty CoordExternalEndpoint disables the copy and keeps the manual behaviour.
	CoordKubeconfigSourceSecret string
	CoordExternalEndpoint       string

	// CoordKubeconfigMirrorNamespaces are additional namespaces ON THE HUB that get a copy
	// of the coordination kubeconfig. The KubeDB operator mounts it from its own namespace
	// rather than the addon's, so without this an operator credential has to be created by
	// hand and re-created every time the control plane mints a new CA.
	CoordKubeconfigMirrorNamespaces []string

	// Replicas is how many agent pods each spoke runs. With more than one, a
	// spoke-local writer election picks the single replica allowed to write
	// (markers, hub Leases, override annotations); the rest stay hot standbys
	// with a warm hub observation and take over within the writer election
	// LeaseDuration. Extra replicas add no quorum weight; only etcd members vote.
	Replicas int
	// ImageRepository, ImageTag and ImagePullSecrets describe where the spoke pulls
	// the dr-controlplane image from. ImagePullSecrets is a list of Secret names
	// that must exist in InstallNamespace on each spoke.
	ImageRepository  string
	ImageTag         string
	ImagePullSecrets []string

	// Health Lease tunables (the per DC liveness signal).
	HealthLeaseDuration time.Duration
	HealthRenewInterval time.Duration

	// Election tunables for the primary DC Lease. See markerFenceTTL: lowering
	// ElectionLeaseDuration below it opens a two writable DC window.
	ElectionLeaseDuration time.Duration
	ElectionRenewDeadline time.Duration
	ElectionRetryPeriod   time.Duration
}

// NewAgentOptions returns the defaults, seeded from the environment where an env
// var is set. Flags, when passed, win over both.
func NewAgentOptions() AgentOptions {
	return AgentOptions{
		InstallNamespace:                envOr("AGENT_INSTALL_NAMESPACE", defaultAgentInstallNamespace),
		CreateNamespace:                 envBoolOr("AGENT_CREATE_NAMESPACE", false),
		CoordKubeconfigSecret:           envOr("COORD_KUBECONFIG_SECRET", defaultCoordKubeconfigSecret),
		CoordKubeconfigSourceSecret:     envOr("COORD_KUBECONFIG_SOURCE_SECRET", "multicluster-controlplane-kubeconfig"),
		CoordExternalEndpoint:           os.Getenv("COORD_EXTERNAL_ENDPOINT"),
		CoordKubeconfigMirrorNamespaces: envListOr("COORD_KUBECONFIG_MIRROR_NAMESPACES", nil),
		ImageRepository:                 envOr("AGENT_IMAGE_REPOSITORY", defaultAgentImageRepository),
		ImageTag:                        os.Getenv("AGENT_IMAGE_TAG"),
		Replicas:                        envIntOr("AGENT_REPLICAS", 1),
		ImagePullSecrets:                envListOr("AGENT_IMAGE_PULL_SECRETS", nil),
		HealthLeaseDuration:             15 * time.Second,
		HealthRenewInterval:             5 * time.Second,
		// Matches the live production agents: 45s/40s/5s. Do not lower
		// ElectionLeaseDuration below markerFenceTTL, Validate rejects it.
		ElectionLeaseDuration: 45 * time.Second,
		ElectionRenewDeadline: 40 * time.Second,
		ElectionRetryPeriod:   5 * time.Second,
	}
}

// AddFlags binds the agent tunables to a flag set.
func (o *AgentOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.InstallNamespace, "agent-install-namespace", o.InstallNamespace,
		"Namespace on each managed cluster the agent is installed into. Must be where the coordination kubeconfig Secret lives.")
	fs.BoolVar(&o.CreateNamespace, "agent-create-namespace", o.CreateNamespace,
		"Let the addon create and own the agent install namespace. Leave false for a shared namespace: deleting the addon would garbage collect everything in it.")
	fs.StringVar(&o.CoordKubeconfigSourceSecret, "agent-coord-kubeconfig-source-secret", o.CoordKubeconfigSourceSecret,
		"Secret on the hub (in the addon namespace) holding the coordination control plane kubeconfig to copy to every managed cluster.")
	fs.StringSliceVar(&o.CoordKubeconfigMirrorNamespaces, "coord-kubeconfig-mirror-namespaces", o.CoordKubeconfigMirrorNamespaces,
		"Additional hub namespaces to copy the coordination kubeconfig Secret into, e.g. kubedb for the KubeDB operator.")
	fs.StringVar(&o.CoordExternalEndpoint, "agent-coord-external-endpoint", o.CoordExternalEndpoint,
		"External URL agents must reach the coordination control plane on, e.g. https://10.0.0.1:9443. Empty disables copying the credential.")
	fs.StringVar(&o.CoordKubeconfigSecret, "agent-coord-kubeconfig-secret", o.CoordKubeconfigSecret,
		"Secret (key: kubeconfig) in the agent install namespace holding the coordination control plane credential. Empty uses the spoke in cluster config.")
	fs.StringVar(&o.ImageRepository, "agent-image-repository", o.ImageRepository,
		"Image repository for the agent Deployment on managed clusters.")
	fs.IntVar(&o.Replicas, "agent-replicas", o.Replicas,
		"Agent pod replicas per spoke. Extra replicas are pod crash insurance only; they share the DC identity and add no quorum weight.")
	fs.StringVar(&o.ImageTag, "agent-image-tag", o.ImageTag,
		"Image tag for the agent Deployment. Empty falls back to the agent chart appVersion.")
	fs.StringSliceVar(&o.ImagePullSecrets, "agent-image-pull-secrets", o.ImagePullSecrets,
		"Comma separated imagePullSecret names for the agent Deployment. The Secrets must exist in the agent install namespace on each managed cluster.")
	fs.DurationVar(&o.HealthLeaseDuration, "agent-health-lease-duration", o.HealthLeaseDuration,
		"leaseDurationSeconds for each DC's health Lease.")
	fs.DurationVar(&o.HealthRenewInterval, "agent-health-renew-interval", o.HealthRenewInterval,
		"How often the agent renews its health Lease.")
	fs.DurationVar(&o.ElectionLeaseDuration, "agent-election-lease-duration", o.ElectionLeaseDuration,
		fmt.Sprintf("Primary DC Lease duration. Must stay strictly above the %s data plane marker fence TTL or two DCs can be writable at once.", markerFenceTTL))
	fs.DurationVar(&o.ElectionRenewDeadline, "agent-election-renew-deadline", o.ElectionRenewDeadline,
		"Primary DC Lease renew deadline. Must be less than the election lease duration.")
	fs.DurationVar(&o.ElectionRetryPeriod, "agent-election-retry-period", o.ElectionRetryPeriod,
		"Primary DC Lease retry period. Must be less than the election renew deadline.")
}

// Validate rejects an agent configuration that would be unsafe or unrenderable.
func (o AgentOptions) Validate() error {
	if o.InstallNamespace == "" {
		return fmt.Errorf("--agent-install-namespace is required")
	}
	if o.ImageRepository == "" {
		return fmt.Errorf("--agent-image-repository is required")
	}
	if o.Replicas < 1 {
		return fmt.Errorf("--agent-replicas must be at least 1, got %d", o.Replicas)
	}
	if o.HealthRenewInterval <= 0 || o.HealthRenewInterval >= o.HealthLeaseDuration {
		return fmt.Errorf("--agent-health-renew-interval must be positive and less than --agent-health-lease-duration")
	}
	// The split-brain guard. A shorter primary DC Lease than the data plane fence
	// TTL lets a survivor acquire the Lease while the old primary still reads its
	// marker as fresh, so both accept writes.
	if o.ElectionLeaseDuration <= markerFenceTTL {
		return fmt.Errorf("--agent-election-lease-duration must be greater than the %s data plane marker fence TTL, got %s: "+
			"a shorter lease lets a second data center take the primary DC Lease before the old primary has fenced itself, "+
			"leaving two writable primaries", markerFenceTTL, o.ElectionLeaseDuration)
	}
	if o.ElectionRenewDeadline >= o.ElectionLeaseDuration {
		return fmt.Errorf("--agent-election-renew-deadline must be less than --agent-election-lease-duration")
	}
	if o.ElectionRetryPeriod <= 0 || o.ElectionRetryPeriod >= o.ElectionRenewDeadline {
		return fmt.Errorf("--agent-election-retry-period must be positive and less than --agent-election-renew-deadline")
	}
	return nil
}

func envIntOr(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBoolOr(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envListOr(key string, def []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var out []string
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}
