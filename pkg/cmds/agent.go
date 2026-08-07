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

package cmds

import (
	"context"
	"errors"
	"net/http"

	"open-cluster-management.io/dr-controlplane/pkg/agent"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

func newCmdAgent() *cobra.Command {
	opts := agent.DefaultOptions()
	cmd := &cobra.Command{
		Use:               "agent",
		Short:             "Run the per data center DC agent",
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := opts.Validate(); err != nil {
				return err
			}
			return runAgent(cmd.Context(), opts)
		},
	}
	opts.AddFlags(cmd.Flags())
	return cmd
}

func runAgent(ctx context.Context, opts agent.Options) error {
	cfg, err := opts.RESTConfig()
	if err != nil {
		return err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	// A second clientset over the same config: rest.Config is copied by value into
	// each clientset, so this one carries its OWN rate limiter. The health Lease
	// renewals and the observation watchdog run on it, out of reach of elector
	// bursts (see agent.Options.ClientQPS for the incident this prevents).
	auxCfg, err := opts.RESTConfig()
	if err != nil {
		return err
	}
	aux, err := kubernetes.NewForConfig(auxCfg)
	if err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	m := agent.NewMetrics(reg, opts.DCName)

	a := agent.New(opts, cs, aux, m)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	// /healthz reports whether this agent can actually reach the coordination control
	// plane, not merely whether the process is up. It used to answer "ok"
	// unconditionally, which made an agent that could not talk to the control plane at
	// all look perfectly healthy to the addon prober. See Agent.Healthy.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if !a.Healthy() {
			http.Error(w, "coordination control plane unreachable", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: opts.MetricsAddr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			klog.ErrorS(err, "metrics server stopped")
		}
	}()
	defer func() { _ = srv.Shutdown(context.Background()) }()

	klog.InfoS("starting agent", "dc", opts.DCName, "namespace", opts.Namespace, "metricsAddr", opts.MetricsAddr)
	if err := a.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	klog.Info("agent stopped")
	return nil
}
