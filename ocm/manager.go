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

// Package ocm runs the dr-controlplane OCM addon manager. It builds a hub side
// controller-runtime manager plus an addon-framework AgentAddon that ships the
// dr-controlplane agent (Deployment, ServiceAccount, Role, RoleBinding) as an
// embedded Helm chart, so the agent is installed onto every ManagedCluster that
// carries a ManagedClusterAddOn for this addon instead of by a manual per DC
// helm install.
package ocm

import (
	"context"
	"embed"
	"fmt"
	"os"
	"time"

	"open-cluster-management.io/addon-framework/pkg/addonfactory"
	"open-cluster-management.io/addon-framework/pkg/addonmanager"
	"open-cluster-management.io/addon-framework/pkg/agent"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
	"open-cluster-management.io/dr-controlplane/ocm/secretfs"

	cert "gomodules.xyz/cert"
	"gomodules.xyz/cert/certstore"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	restclient "k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	cu "kmodules.xyz/client-go/client"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

//go:embed all:manifests
var FS embed.FS

const (
	// AddonName is the OCM addon name. It matches the ManagedClusterAddOn name
	// that must exist on a ManagedCluster for the agent to be installed there.
	AddonName = "dr-controlplane"

	// Duration10Yrs is the validity used for the bootstrap CA.
	Duration10Yrs = 10 * 365 * 24 * time.Hour

	// CACertName is the certstore key for the CA material.
	CACertName = "ca"
	// ServerCertName is the certstore key for the serving cert pair.
	ServerCertName = "tls"

	// CASecretName is the Secret that backs the addon CA on the hub.
	CASecretName = "dr-controlplane-addon-ca"

	// defaultControlPlaneNamespace is used when POD_NAMESPACE is unset.
	defaultControlPlaneNamespace = "dc-failover"
)

var scheme = runtime.NewScheme()

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
	_ = clusterv1.Install(scheme)
}

// controlPlaneNamespace is the namespace the addon manager runs in and where the
// bootstrap CA Secret is stored.
func controlPlaneNamespace() string {
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	return defaultControlPlaneNamespace
}

// RunManagerController starts the hub manager and the OCM addon manager. It runs
// in cluster on the OCM hub. The addon manager reconciles a HelmAgentAddon that
// renders the embedded dr-controlplane-agent chart per ManagedCluster, using the
// agent tunables in opts.
func RunManagerController(opts AgentOptions) error {
	kubeConfig, err := restclient.InClusterConfig()
	if err != nil {
		return err
	}

	resyncPeriod := 1 * time.Hour
	hubManager, err := ctrl.NewManager(kubeConfig, manager.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "",
		LeaderElection:         false,
		NewClient:              cu.NewClient,
		Cache: cache.Options{
			SyncPeriod: &resyncPeriod,
		},
	})
	if err != nil {
		return err
	}

	addonMgr, err := addonmanager.New(kubeConfig)
	if err != nil {
		return fmt.Errorf("unable to create addon manager: %w", err)
	}

	// Bootstrap CA scaffolding. The CA is persisted in a Secret on the hub via
	// the secretfs backed certstore. This mirrors the kubeslice approach and is
	// the foundation for a later CSR based agent identity. It does not yet issue
	// or distribute agent client certs; the agent keeps using coord-kubeconfig.
	caSecretFS := secretfs.New(hubManager.GetClient(), types.NamespacedName{
		Name:      CASecretName,
		Namespace: controlPlaneNamespace(),
	})
	cs := certstore.New(caSecretFS, "", Duration10Yrs)
	if err := hubManager.Add(manager.RunnableFunc(func(_ context.Context) error {
		if err := cs.InitCA(); err != nil {
			return err
		}
		// Generate a serving pair off the bootstrap CA so the material exists
		// for future coordination endpoints. No webhook specific SANs here.
		_, _, err := cs.GetServerCertPair(ServerCertName, cert.AltNames{
			DNSNames: []string{
				fmt.Sprintf("%s.%s.svc", AddonName, controlPlaneNamespace()),
				fmt.Sprintf("%s.%s.svc.cluster.local", AddonName, controlPlaneNamespace()),
			},
		})
		return err
	})); err != nil {
		klog.Error(err, "unable to initialize cert store")
		os.Exit(1)
	}

	klog.InfoS(
		"starting addon manager",
		"addon", AddonName,
		"agentInstallNamespace", opts.InstallNamespace,
		"agentImage", opts.ImageRepository,
		"agentImageTag", opts.ImageTag,
		"agentImagePullSecrets", opts.ImagePullSecrets,
		"electionLeaseDuration", opts.ElectionLeaseDuration,
		"markerFenceTTL", markerFenceTTL,
	)

	agentAddon, err := addonfactory.NewAgentAddonFactory(AddonName, FS, "manifests/dr-controlplane-agent").
		WithScheme(scheme).
		// Pin the agent install namespace. Without this the addon-framework uses
		// ManagedClusterAddOn.Spec.InstallNamespace, which the CRD always defaults
		// to open-cluster-management-agent-addon, where the coordination
		// credential Secret does not exist.
		//
		// NOTE, known cosmetic discrepancy: `kubectl get managedclusteraddon
		// dr-controlplane -n <dc> -o yaml` reports
		// spec.installNamespace: open-cluster-management-agent-addon even though the
		// agent correctly runs in dc-failover. That field is just the CRD default and
		// nothing writes it back; the namespace used at render time is the one returned
		// here, and it wins. Only the CR field is wrong, never the placement.
		//
		// Making the field agree would mean shipping an AddOnDeploymentConfig with
		// spec.agentInstallNamespace and referencing it from the ClusterManagementAddOn
		// (supportedConfigs + defaultConfigs), then sourcing the namespace via
		// utils.AgentInstallNamespaceFromDeploymentConfigFunc. That is deliberately NOT
		// done: that helper returns an empty namespace both when no config is attached
		// and when the config exists but has not yet landed in the ManagedClusterAddOn
		// status config references, and the framework cannot tell those apart (its own
		// TODO). During the first reconcile after an upgrade that empty value decides
		// where the agent is rendered, and an agent that lands anywhere other than
		// dc-failover cannot mount coord-kubeconfig, never starts, and stops refreshing
		// the active DC marker, which fails the databases closed to read only within
		// the 30s marker TTL. Trading a live write outage for a cosmetic field is a bad
		// deal, so the discrepancy stays documented instead.
		WithAgentInstallNamespace(agentInstallNamespace(opts)).
		// Report addon health from the agent Deployment's own availability instead of
		// the framework default, which is a Lease prober: it looks for a Lease named
		// after the addon, written by the agent into its install namespace on the
		// spoke. This agent never writes that Lease (it maintains dc-health-<dc> in the
		// coordination apiserver, which OCM knows nothing about), so every
		// ManagedClusterAddOn sat at Available=Unknown /
		// ManagedClusterAddOnLeaseNotFound forever even with the agent running 1/1.
		//
		// DeploymentAvailability is the right variant here because the agent Deployment
		// is named per cluster (dr-controlplane-agent-<managedcluster>): the framework
		// renders this addon's manifests for each ManagedCluster and derives the probe
		// identifier from the rendered Deployment, so it resolves the real name and
		// namespace per cluster. A static ResourceIdentifier list could not express
		// that, and the wildcard alternative (utils.NewAllDeploymentsProber) is doubly
		// unusable: its health check hard fails on len(results) < 2 while this addon
		// ships exactly one Deployment, and it would depend on wildcard manifestConfig
		// support in whatever work agent version each spoke happens to run.
		WithAgentHealthProber(&agent.HealthProber{
			Type: agent.HealthProberTypeDeploymentAvailability,
		}).
		WithGetValuesFuncs(getValues(opts, kubeConfig, cs)).
		BuildHelmAgentAddon()
	if err != nil {
		return fmt.Errorf("unable to build agent addon: %w", err)
	}
	if err := addonMgr.AddAgent(agentAddon); err != nil {
		return fmt.Errorf("unable to add agent to addon manager: %w", err)
	}

	go func() {
		if err := addonMgr.Start(context.Background()); err != nil {
			klog.ErrorS(err, "OCM addon manager exited")
		}
	}()
	// Keep the operator's own copy of the coordination credential current. See
	// runCoordKubeconfigMirror: the addon reaches every managed cluster, but not the hub
	// namespace the KubeDB operator mounts from.
	go runCoordKubeconfigMirror(context.Background(), opts, kubeConfig)
	return hubManager.Start(context.Background())
}
