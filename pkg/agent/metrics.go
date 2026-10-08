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

package agent

import "github.com/prometheus/client_golang/prometheus"

// Metrics are the agent's Prometheus signals.
type Metrics struct {
	IsPrimary           *prometheus.GaugeVec
	Contending          *prometheus.GaugeVec
	ElectionTransitions *prometheus.CounterVec
	HealthRenewals      prometheus.Counter
	HealthRenewErrors   prometheus.Counter
	ObservationRepairs  prometheus.Counter
	InformerRebuilds    prometheus.Counter
	IsWriter            prometheus.Gauge
}

// NewMetrics registers and returns the agent metrics.
func NewMetrics(reg prometheus.Registerer, dc string) *Metrics {
	cl := prometheus.Labels{"dc": dc}
	m := &Metrics{
		IsPrimary: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "dr", Subsystem: "agent", Name: "is_primary",
			Help:        "1 when this data center holds the primary DC Lease for a scope.",
			ConstLabels: cl,
		}, []string{"scope"}),
		Contending: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "dr", Subsystem: "agent", Name: "contending",
			Help:        "1 when this data center is contending for a scope's primary DC Lease (Member only).",
			ConstLabels: cl,
		}, []string{"scope"}),
		ElectionTransitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "dr", Subsystem: "agent", Name: "election_transitions_total",
			Help:        "Count of acquire/lose transitions observed for a scope.",
			ConstLabels: cl,
		}, []string{"scope", "kind"}),
		HealthRenewals: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "dr", Subsystem: "agent", Name: "health_renewals_total",
			Help:        "Count of successful health Lease renewals.",
			ConstLabels: cl,
		}),
		HealthRenewErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "dr", Subsystem: "agent", Name: "health_renew_errors_total",
			Help:        "Count of failed health Lease renewals (loss of etcd majority shows up here).",
			ConstLabels: cl,
		}),
		ObservationRepairs: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "dr", Subsystem: "agent", Name: "observation_repairs_total",
			Help:        "Count of watchdog checks that proved the Lease informer stale and repaired the observed state from a direct List. Nonzero means a watch silently died.",
			ConstLabels: cl,
		}),
		InformerRebuilds: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "dr", Subsystem: "agent", Name: "informer_rebuilds_total",
			Help:        "Count of Lease informer rebuilds after staleness was proven on consecutive watchdog checks.",
			ConstLabels: cl,
		}),
		IsWriter: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "dr", Subsystem: "agent", Name: "is_writer",
			Help:        "1 on the replica holding the spoke-local writer role (the only replica allowed to write markers, hub Leases, and override annotations).",
			ConstLabels: cl,
		}),
	}
	reg.MustRegister(m.IsPrimary, m.Contending, m.ElectionTransitions, m.HealthRenewals, m.HealthRenewErrors, m.ObservationRepairs, m.InformerRebuilds, m.IsWriter)
	return m
}
