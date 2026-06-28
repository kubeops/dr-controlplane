# dr-controlplane

Common DC failover service. It runs a three data center etcd quorum and an OCM
control plane that exposes the standard Kubernetes Lease API as the cross data
center failover signal. Other applications, starting with the KubeDB DC/DR driver, read that Lease
to decide which data center is primary.

The split brain guarantee is the etcd majority: moving a Lease is an etcd write
that needs a majority, so a partitioned minority cannot renew and self fences. The
service never promotes or demotes a workload itself; it only publishes which data
center the quorum trusts.

## One binary, subcommands

```
dr-controlplane agent       run the per data center DC agent (one per DC)
dr-controlplane controller  run the topology controller (one)
dr-controlplane status      show scopes, primaries, and per DC health
dr-controlplane switchover  request a planned coordinated handoff
dr-controlplane version
```

## The failover signal

The decision is a normal `coordination.k8s.io` Lease in the `dc-failover`
namespace, so any client, controller, or operator can read it with no custom
protocol:

- one primary DC Lease per trigger scope (`primary-dc`, or `primary-dc-<group>`);
  its `holderIdentity` is the current primary data center, and a change is a failover.
- one health Lease per data center (`dc-health-<dc>`), renewed by that DC's agent.

The primary DC Lease is a DC ownership signal, not a per database data currency
signal. A consumer still applies its own lag guard before promoting, and surfaces
its own Degraded state if its data in the new primary DC is not safely promotable.

## Data center roles

Set per data center on the workload's PlacementPolicy:

- `Member`: data bearing and primary eligible, a candidate for the primary DC Lease.
- `Arbiter`: votes in etcd, holds no data, never primary.
- `Witness`: data bearing but never primary (for engines like MongoDB whose witness
  must carry data to satisfy majority writes, yet must not be elected).

Two modes, same etcd topology (three voting members): TwoDC (two Members plus one
Arbiter or Witness) and ThreeDC (three Members, any can be primary).

## Install

Install etcd, the control plane, and the topology controller once:

```
helm install dr charts/dr-controlplane \
  --set agent.enabled=false \
  --set controlplane.image=<your-ocm-control-plane-image> \
  --set controlplane.externalHostname=dc-failover.example.com
```

The control plane is an OCM apiserver that serves `coordination.k8s.io` on the
external etcd quorum; supply its image with `controlplane.image`.

Expose the control plane behind one stable endpoint (a LoadBalancer or GSLB) and
build a kubeconfig whose single server URL is that endpoint. A kubeconfig has one
server URL, so cross replica failover happens at the load balancer.

Install the agent once per data center:

```
helm install dr-dc-a charts/dr-controlplane \
  --set etcd.deploy=false --set controlplane.enabled=false --set topology.enabled=false \
  --set agent.dcName=dc-a --set agent.coordKubeconfigSecret=coord-kubeconfig
```

For real multi data center production, set `etcd.deploy=false` and run one external
etcd member per data center. See `charts/dr-controlplane/values.yaml` for all
settings.

## Mark a workload cross DC

Add `failoverPolicy` and per rule `role` to the workload's PlacementPolicy
`clusterSpreadConstraint`:

```yaml
clusterSpreadConstraint:
  failoverPolicy:
    mode: TwoDC
    trigger:
      scope: Global          # or Group, with a group name
  distributionRules:
    - clusterName: dc-a
      replicaIndices: [0]
      role: Member
    - clusterName: dc-b
      replicaIndices: [1]
      role: Member
    - clusterName: dc-c
      replicaIndices: []
      role: Arbiter
```

The controller turns this into the matching primary DC Lease. See
`config/samples` for Postgres (Arbiter) and MongoDB (Witness) examples.

## Operate

```
dr-controlplane status                              # scopes, members, primary, health
dr-controlplane switchover --to dc-b                # planned coordinated handoff (global)
dr-controlplane switchover --group orders --to dc-b # per group handoff
```

`switchover` requests a coordinated handoff: the current holder releases, non
target candidates pause, and the named data center (which must be a Member)
acquires the Lease without a race.

## Consume the signal

```go
import (
    drclient "open-cluster-management.io/dr-controlplane/pkg/client"
    "open-cluster-management.io/dr-controlplane/pkg/leases"
)

c := drclient.New(clientset, leases.DefaultNamespace, "dc-a")
go c.Run(ctx)

c.OnPrimaryDCChange(leases.GlobalScope, func(oldDC, newDC string) {
    // A DC failover. Apply your own lag guard, then promote in newDC or demote here.
})
if c.IsLocalDCPrimary(leases.GlobalScope) {
    // run the workload's primary here
}
```

## Documentation

- `DESIGN.md` the architecture, the Lease model, the coordinated handoff, and the failure model.
- `TEST.md` how to exercise each feature, from unit tests to a local failover walkthrough.
- `DEV.md` the development workflow, the petset dependency, and the build harness.
- `AGENTS.md` a short guide for coding agents.

## License

Source code in this repository, Binaries, Docker images and Charts produced by the
build process are licensed under the AppsCode Free Trial License 1.0.0. See
[LICENSE.md](LICENSE.md).
