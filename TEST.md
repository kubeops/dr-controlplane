# Testing

How to test the service, from unit tests to a full failover walkthrough. For architecture see `DESIGN.md`, for development see `DEV.md`.

## Unit tests

The pure logic is unit tested: the handoff decision, the topology derivation, and the lease naming contract.

```
go test ./pkg/...                                              # all unit tests
go test ./pkg/agent/... -run TestDesiredContend -v             # the handoff decision table
go test ./pkg/topology/... -run TestDeriveTwoDCWithArbiter -v  # Member/Arbiter derivation
go test ./pkg/topology/... -run TestDeriveGroupArbiter -v      # group scope with a vote-only Arbiter
go test ./pkg/leases/... -v                                    # lease names and member parsing
```

The containerized equivalent (runs in the AppsCode build image, the same way CI does):

```
make test
```

What the unit tests pin down:

- `TestDesiredContend` proves an Arbiter never contends, a Member contends normally, the handoff target contends eagerly, a non target Member pauses during a handoff, and contention resumes once the target holds the Lease.
- `TestDeriveTwoDCWithArbiter` and `TestDeriveGroupArbiter` prove a PlacementPolicy maps to the right scope (`primary-dc` or `primary-dc-<group>`) with the right Member and Arbiter sets, and `TestValidateSpreadRejectsTwoRegions` proves the failure domain spread check.

## Local end to end on one cluster

The agents and the controller only need a Kubernetes apiserver that serves the Lease API, so a single Kind cluster is enough to exercise leader election, failover, the coordinated handoff, status, and switchover without standing up three etcd members. (A true three data center, partition, and quorum loss test needs three clusters with one external etcd member each; the single cluster flow covers everything else.)

Build the binary and create the namespace:

```
go build -o bin/dr-controlplane .
kubectl create namespace dc-failover
```

Create a global primary DC Lease with two Member candidates (this is what the controller would create from PlacementPolicies):

```
kubectl -n dc-failover create -f - <<'EOF'
apiVersion: coordination.k8s.io/v1
kind: Lease
metadata:
  name: primary-dc
  annotations:
    dr.open-cluster-management.io/member-dcs: "dc-a,dc-b"
    dr.open-cluster-management.io/scope: global
spec: {}
EOF
```

Run two agents, one per data center, in separate terminals (each uses your kubeconfig as the coordination control plane):

```
./bin/dr-controlplane agent --dc-name=dc-a --kubeconfig=$HOME/.kube/config --v=2
./bin/dr-controlplane agent --dc-name=dc-b --kubeconfig=$HOME/.kube/config --v=2
```

Inspect the result:

```
./bin/dr-controlplane status --kubeconfig=$HOME/.kube/config
```

You should see one of `dc-a` or `dc-b` as PRIMARY for scope `global`, and two health Leases (`dc-health-dc-a`, `dc-health-dc-b`) renewing.

### Feature: single holder (leader election)

With both agents running, `status` must show exactly one primary, never two. Confirm directly:

```
kubectl -n dc-failover get lease primary-dc -o jsonpath='{.spec.holderIdentity}{"\n"}'
```

### Feature: automatic failover on primary loss

Stop the agent that currently holds the Lease (Ctrl-C). After `LeaseDuration` (default 45s) the other agent acquires it; `status` shows the new primary and the `election_transitions_total` counter increments.

### Feature: Arbiter never primary

Start a third agent whose data center is not in the `member-dcs` annotation:

```
./bin/dr-controlplane agent --dc-name=dc-c --kubeconfig=$HOME/.kube/config --v=2
```

It renews `dc-health-dc-c` but never holds `primary-dc`, no matter how many times you stop the others. This is the Arbiter behavior.

### Feature: coordinated failback handoff

With `dc-a` primary, request a planned move to `dc-b`:

```
./bin/dr-controlplane switchover --to dc-b --kubeconfig=$HOME/.kube/config
```

`switchover` refuses a target that is not in `member-dcs`. On success the holder releases, the non target candidates pause, and `dc-b` acquires the Lease deterministically (not a race). `status` then shows `dc-b` primary and the `handoff-to` annotation cleared.

### Feature: health and staleness

Stop an agent and watch its health Lease stop renewing:

```
kubectl -n dc-failover get lease dc-health-dc-a -o jsonpath='{.spec.renewTime}{"\n"}'
```

The `RENEWED` column in `dr-controlplane status` shows the age; a growing age is how observers detect a data center that has lost the etcd majority.

### Feature: active DC marker projection

Each agent mirrors its scope's primary DC into a marker ConfigMap on its local spoke, named after the primary Lease. Running the agent off cluster like this, add `--spoke-kubeconfig=$HOME/.kube/config` so the projector can reach the spoke (in cluster that is automatic). On the single cluster flow the spoke is the same cluster, so read the marker directly:

```
kubectl -n dc-failover get configmap primary-dc -o yaml   # see data.activeDC, data.renewTime, data.quiesce
```

You should see `activeDC` (the current primary), a `renewTime` that keeps advancing while the agent can reach the control plane, and an empty `quiesce`. Stop the holding agent and the `renewTime` stops advancing: that frozen timestamp is the fail closed signal a consumer fence trips on. During a `switchover`, `quiesce` briefly names the outgoing holder.

### Feature: topology controller from PlacementPolicies

If the petset PlacementPolicy CRD is installed on the cluster, run the controller and apply the samples instead of hand creating Leases:

```
./bin/dr-controlplane controller --hub-kubeconfig=$HOME/.kube/config --require-spread=false --v=2
kubectl apply -f config/samples/postgres-placementpolicy.yaml
kubectl apply -f config/samples/mongodb-placementpolicy.yaml
```

The controller creates `primary-dc` (from the Postgres global policy) and `primary-dc-orders` (from the MongoDB group policy), each annotated with its `member-dcs`. Use `--require-spread=false` for a single cluster test, since one cluster is one failure domain.

### Feature: metrics

The agent serves Prometheus metrics on `:8080` by default:

```
curl -s localhost:8080/metrics | grep '^dr_agent_'
```

Key series: `dr_agent_is_primary{dc,scope}`, `dr_agent_contending{dc,scope}`, `dr_agent_election_transitions_total{dc,scope,kind}`, `dr_agent_health_renewals_total{dc}`, `dr_agent_health_renew_errors_total{dc}`. Renew errors rising is how a loss of etcd majority shows up.

## Self fencing without three clusters

To see a minority self fence on one machine, point an agent at an apiserver it cannot reach (simulating the partition):

```
./bin/dr-controlplane agent --dc-name=dc-a --kubeconfig=/path/to/unreachable.kubeconfig --v=2
```

It cannot renew, so it never holds (or loses) the Lease. On a real three data center deployment this is exactly what happens to the side without the etcd majority.

## Helm

```
helm lint charts/dr-controlplane --set agent.dcName=dc-a
helm template dr charts/dr-controlplane --set agent.dcName=dc-a   # inspect rendered manifests
```

Install into Kind for a fuller test:

```
kind create cluster --config hack/kubernetes/kind.yaml
make push-to-kind
make install        # installs the chart into namespace dc-failover
```
