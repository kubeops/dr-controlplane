# Design and architecture

This document explains how the common DC failover service works and why it is built this way. For the user guide see `README.md`, for testing see `TEST.md`, for development see `DEV.md`.

## Problem

Stateful workloads spread across data centers need one agreed answer to a single question: which data center is currently primary. The answer must move automatically when a data center is lost, must never name two primaries at once (split brain), and must be readable by many different applications without each inventing its own coordination protocol.

## Core principle: the etcd majority, surfaced as a Lease

A three member etcd cluster, one member per data center, is the authority. Quorum is two of three. Every decision is an etcd write, and an etcd write commits only on a majority. A data center that is partitioned away from the majority cannot commit anything, so it cannot claim or keep the primary role. It self fences. This is the entire split brain guarantee, and it comes from etcd, not from application logic.

The decision is published as a normal `coordination.k8s.io` Lease served by an OCM control plane running on that external etcd. Any controller, operator, or client library can read a Lease with no custom protocol, which is the whole point of using the Lease API rather than a bespoke RPC.

```
        Data Center A            Data Center B            Data Center C
      +--------------+         +--------------+         +--------------+
      | etcd member  |<------->| etcd member  |<------->| etcd member  |   quorum 2/3
      | apiserver    |         | apiserver    |         | (apiserver)  |
      | DC agent     |         | DC agent     |         | DC agent     |
      +------+-------+         +------+-------+         +------+-------+
             |                        |                        |
             +----------- load balancer / GSLB ----------------+
                                      |
                          one kubeconfig server URL
                                      |
                   consumers (KubeDB DR driver, ...) read the Lease
```

## The Lease model

Two kinds of Lease live in the `dc-failover` namespace. Their names and annotations (in `pkg/leases`) are the contract between the controller that writes them, the agents that contend for them, and the consumers that read them.

Primary DC Lease, one per trigger scope:

- `primary-dc` for the global scope, or `primary-dc-<group>` for a workload group.
- `spec.holderIdentity` is the name of the current primary data center. A change in holder is a failover.
- annotation `dr.open-cluster-management.io/member-dcs` lists the Member data centers (the candidates), set by the controller.
- annotation `dr.open-cluster-management.io/handoff-to` names a target for a planned coordinated handoff.

Health Lease, one per data center:

- `dc-health-<dc>`, renewed continuously by that data center's agent.
- a data center that loses the etcd majority cannot renew it, so observers see it go stale. This is a liveness signal, separate from who is primary.

## Data center roles and the two modes

Each data center has a role, set per data center on the workload's PlacementPolicy (`distributionRules[].role`):

- `Member`: data bearing and primary eligible. A candidate for the primary DC Lease.
- `Arbiter`: votes in etcd, holds no data, never primary. Its agent never contends.
- `Witness`: data bearing but never primary. This exists for engines like MongoDB whose witness must carry data to satisfy `w:"majority"` writes, yet must never be elected. Its agent never contends.

The etcd topology is always three voting members. The only difference between the modes is which data centers are candidates:

- TwoDC: two Members plus one Arbiter or Witness. The two Members are the only Lease candidates.
- ThreeDC: three Members, any can be primary.

## Components

The four deployable pieces (the Helm chart installs each):

1. etcd quorum. Three members, one per data center, WAN tuned (heartbeat and election timeout above the inter DC round trip time). The chart ships a StatefulSet for single cluster dev; production uses one real external member per data center.

2. Control plane. OCM control plane apiserver replicas in external etcd mode (`ocmconfig.yaml` with `etcd.mode: external`), serving the Lease API. Replicas are stateless; their state is the external etcd. Clients reach them through one stable endpoint (see Client endpoint below).

3. DC agent (`pkg/agent`, the `agent` subcommand), one per data center. It always renews its health Lease. For each scope where its data center is a Member it runs a `client-go` leader election participant against the control plane; the winner's `holderIdentity` is that data center's name. Arbiter and Witness agents run only the health Lease.

4. Topology controller (`pkg/topology`, the `controller` subcommand), one instance. It watches PlacementPolicy on the OCM hub through a dynamic informer, derives for each trigger scope the set of Member, Arbiter, and Witness data centers, and ensures a primary DC Lease exists per scope annotated with the member set. It never touches the Lease spec the agents manage (holder, renew time).

Plus the consumer library (`pkg/client`), imported by applications that need the failover signal.

## Leader election and desired contention

The agent does not run one long lived election. It computes, per scope, whether its data center should be contending right now, and starts or stops a leader election participant to match. The decision (`desiredContend` in `pkg/agent/handoff.go`) is:

- not a Member: never contend (Arbiter, Witness).
- a Member with no handoff in progress: contend.
- a Member that is the handoff target: contend eagerly.
- a Member during a handoff to another data center: pause, so the target can acquire. If this data center currently holds the Lease, pausing releases it (`ReleaseOnCancel`).
- once the target holds the Lease: resume normal contention.

Because client-go's election returns after losing leadership, the participant is wrapped in a re-contend loop so a former primary goes back to contending rather than going idle.

Every Lease renewal is an etcd write that commits on a majority, so it costs one inter data center round trip. The election durations (`LeaseDuration`, `RenewDeadline`, `RetryPeriod`) must sit comfortably above the inter DC RTT. They also set the failover signal's RTO floor: a dead primary's Lease is reclaimable only after `LeaseDuration`.

## Coordinated failback handoff

Plain leader election has no priority, so releasing a Lease to hand it back to a preferred data center is a race: any candidate can re-acquire, including the wrong one. The service instead does a coordinated handoff. `dr-controlplane switchover --to <dc>` (or the controller) sets `handoff-to: <dc>` on the Lease. Every agent reacts through `desiredContend`: the current holder releases and the other Members pause, so only the target is still contending and it acquires. Once the target holds the Lease, the holder clears the annotation and normal contention resumes. A handoff whose target is not a Member (a stale annotation, or a Member removed from the set mid handoff) is ignored, so a bad target can never leave the scope with no primary. The target only becomes primary after it is healthy, and the consumer still applies its own lag guard before accepting writes there.

## DC ownership is not data currency

The primary DC Lease says which data center the quorum trusts. It does not say whether a given database's copy in that data center is current. Replication lag is per workload and the failover service does not see it. So the contract, stated in `pkg/client`, is that a consumer reads the primary DC from the Lease and then applies its own lag guard before promoting, surfacing its own Degraded state if its data in the new primary is not safely promotable. Keeping the service engine agnostic is deliberate: one Lease serves every workload, and each workload owns its own correctness check.

This is also why trigger granularity matters. A global trigger fails a whole set of workloads over together regardless of each one's lag. A per group trigger lets a group fail over independently, which is what you want when different groups have different lag tolerances.

## Triggers: global versus per group

A workload's PlacementPolicy `failoverPolicy.trigger` selects which Lease it follows: `Global` (the single `primary-dc` Lease) or `Group` with a group name (`primary-dc-<group>`). The controller derives one Lease per distinct scope it sees across all PlacementPolicies. Supporting both lets an operator choose coordinated, fleet wide failover or fine grained, per group failover from the same control plane.

## Failure model

- One Member or the Arbiter or Witness is lost: the remaining two are still an etcd majority, the service stays up, and the loss alone triggers no failover. The system is Degraded until the member returns, because it can no longer tolerate a second loss.
- The current primary data center is lost: it can no longer renew, the Lease expires after `LeaseDuration`, and another Member acquires it. That holder change is the failover signal.
- A network partition: only the side with the etcd majority can renew the Lease. The minority side cannot, so it self fences. There is never a second primary.
- A Witness only loss: the Witness never held the Lease, so nothing moves.
- Two data centers lost at once: no etcd majority remains, so the Lease becomes unchangeable. The decision freezes rather than guessing. This is the correct safe behavior for a quorum system, and recovery requires restoring a quorum.

## Client endpoint

A kubeconfig has a single server URL, and the Kubernetes client does not fail over across endpoints on its own. So consumers and agents reach the control plane through one load balancer or GSLB in front of the apiserver replicas, named as that single server URL. Cross replica failover happens at the load balancer, not in the client.

## The control plane is configured, not built here

An OCM control plane already serves `coordination.k8s.io/v1` and supports external etcd. Everything the failover service needs is the Lease API on a real three data center etcd, so the control plane is run from its published image and configured through `ocmconfig.yaml`, rather than built in this repo. All net new logic lives in the agent, the controller, and the client library.
