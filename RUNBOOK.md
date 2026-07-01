# Operations runbook

How to operate, observe, and recover the common DC failover service in production. For the architecture see `DESIGN.md`, for the user guide see `README.md`, for testing see `TEST.md`, for development see `DEV.md`.

## What this service does, and what it never does

The service runs a three data center etcd quorum and publishes one answer: which data center the quorum currently trusts as primary, as a `coordination.k8s.io` Lease. That is all it does.

- It **never** promotes, demotes, fences, or writes to any database. Consumers (the KubeDB DR driver and others) read the primary DC Lease and apply their own lag guard before promoting. If a workload is stuck after a failover, the fault is almost always in the consumer's promotion path, not here.
- The split brain guarantee is the etcd majority. Moving a Lease is an etcd write that needs two of three members, so a partitioned minority self fences. Do not try to "help" by hand editing a Lease holder; you can only create a split brain that the quorum was preventing.
- The RTO floor for an unplanned failover is `LeaseDuration`: a dead primary's Lease is reclaimable only after it expires.

## Quick reference

| Thing | Value |
|---|---|
| Namespace | `dc-failover` |
| Primary DC Lease (global) | `primary-dc`, `spec.holderIdentity` = current primary DC |
| Primary DC Lease (group) | `primary-dc-<group>` |
| Health Lease (per DC) | `dc-health-<dc>`, renewed by that DC's agent |
| Member set annotation | `dr.open-cluster-management.io/member-dcs` (controller-managed) |
| Handoff target annotation | `dr.open-cluster-management.io/handoff-to` (switchover) |
| Scope annotation | `dr.open-cluster-management.io/scope` |
| Quiesce annotation | `dr.open-cluster-management.io/quiesce` (planned zero-RPO handoff) |
| Managed-by label | `app.kubernetes.io/managed-by=dr-controlplane` (Leases), `dr-controlplane-agent` (spoke markers) |
| Active DC marker (per spoke) | ConfigMap `primary-dc` / `primary-dc-<group>` in the marker namespace (`dc-failover` by default), keys `activeDC` / `renewTime` / `quiesce` |
| Components | etcd (3 members) + control plane apiserver (replicas) + one topology `controller` + one `agent` per DC |
| Roles | `Member` (data, primary-eligible), `Arbiter` (votes, no data, never primary). No Witness role. |

```
dr-controlplane status                        # scopes, Members, current primary, per DC health
dr-controlplane switchover --to dc-b          # planned coordinated handoff (global scope)
dr-controlplane switchover --group orders --to dc-b
```

`status` and `switchover` take `--kubeconfig` (the coordination control plane kubeconfig) and `--namespace` (defaults to `dc-failover`).

## First response: read the state

Always start here. `dr-controlplane status` is the one dashboard; the raw Leases are the ground truth behind it.

```
dr-controlplane status
kubectl -n dc-failover get lease
kubectl -n dc-failover get lease primary-dc -o yaml     # holderIdentity, renewTime, annotations
```

A healthy global scope shows: `primary-dc` held by one Member DC with a fresh `renewTime`, `member-dcs` listing the expected Members, no `handoff-to` annotation, and every `dc-health-<dc>` Lease renewed within the last few `LeaseDuration` intervals.

Read health staleness directly when in doubt:

```
for dc in dc-a dc-b dc-c; do
  echo -n "$dc "; kubectl -n dc-failover get lease dc-health-$dc -o jsonpath='{.spec.renewTime}{"\n"}'
done
```

On each spoke (the DC's own workload cluster) the agent projects the active DC marker that the consumer fence reads locally. Check it there:

```
kubectl -n dc-failover get configmap primary-dc -o yaml   # data.activeDC, data.renewTime, data.quiesce
```

A healthy marker names the current primary in `activeDC` and has a `renewTime` that keeps advancing (within the marker refresh interval, 5s by default). A frozen `renewTime` is what trips the consumer fence read only. `quiesce` is empty except during a planned switchover.

An etcd cross check (run against any reachable member):

```
etcdctl --endpoints=<a,b,c> endpoint health --cluster
etcdctl --endpoints=<a,b,c> member list -w table
etcdctl --endpoints=<a,b,c> endpoint status -w table   # which member is leader, DB size, raft term
```

## Alerts and what they mean

- **A `dc-health-<dc>` Lease is stale** (renewTime older than a few `LeaseDuration`): that DC's agent cannot renew. Either the agent is down, or that DC has lost the etcd majority (it is partitioned). Check the agent pod and the etcd member in that DC.
- **`primary-dc` holderIdentity changed**: a failover happened. If unplanned, follow "The primary DC is lost". If it matches a `switchover` you ran, follow "Planned switchover".
- **`primary-dc` has no holder or an expired renewTime and no candidate acquires**: either no Member is healthy, or the quorum is lost (two members down). Follow "Quorum lost".
- **`handoff-to` annotation present for longer than a minute**: a handoff is stuck (the target is unhealthy or was removed from the member set). Follow "Handoff stuck".
- **Consumer reports Degraded after a failover**: expected and safe. The consumer's own lag guard is refusing to promote diverged data. This is not a control plane fault; triage in the consumer (the KubeDB DR driver's per-engine spec).
- **A spoke's active DC marker `renewTime` is frozen while the Lease looks fine**: the agent in that DC cannot reach the coordination plane, or its projector is wedged, so it stopped restamping the marker. The local consumer fence trips that DC read only at the fence TTL. Follow "The active DC marker is stale".

## Incident procedures

### A single Member, or the Arbiter, is lost

Two of three etcd members remain, so there is still a majority. The service stays up and this loss alone triggers no failover. The system is now Degraded: a second loss would freeze the decision.

1. Confirm it is a single loss: `etcdctl endpoint health --cluster` shows exactly one unhealthy member; the primary is unchanged.
2. Restore the lost member (bring its node/pod back, or replace the etcd member). Priority is high because you have no remaining fault tolerance until it returns.
3. If the lost DC was the primary, this is instead the next procedure.

### The primary DC is lost (automatic failover)

The primary can no longer renew, its Lease expires after `LeaseDuration`, and another Member acquires it. The holder change is the failover signal; the control plane's job is already done. Your job is to confirm it completed cleanly and let consumers finish.

1. Confirm the new holder: `dr-controlplane status` (or `kubectl -n dc-failover get lease primary-dc -o jsonpath='{.spec.holderIdentity}'`) names a surviving Member.
2. Confirm exactly one primary: no other scope names the dead DC as holder.
3. Consumers now see `OnPrimaryDCChange` and apply their own lag guard. Watch the consumer, not the Lease, for promotion. A consumer that stays Degraded is protecting you from a diverged copy; resolve it per the consumer's runbook, do not force the Lease.
4. When the dead DC returns, it comes back as a non-primary Member and rejoins etcd. Do not switch back automatically; fail back with a planned switchover once the returned DC is caught up (see below).

### Network partition

Only the side with the etcd majority can renew the Lease; the minority side cannot and self fences. There is never a second primary. Do not intervene at the control plane.

1. Identify the majority side: `etcdctl endpoint health --cluster` from each side; the side that reports a quorum is authoritative.
2. Verify the minority is fenced: its `dc-health-<dc>` Lease is stale and it does not hold any `primary-dc` Lease.
3. Do not hand edit any Lease to "restore" the minority. When the partition heals, the minority rejoins etcd, its agent resumes renewing health, and normal contention returns on its own.

### Quorum lost (two data centers down at once)

No etcd majority remains, so the Lease is unchangeable. The decision freezes rather than guessing. This is correct, safe behavior for a quorum system; recovery is restoring a quorum, not overriding it.

1. Do not attempt to move a primary by hand. There is no safe automatic answer with one member; any manual holder is a guess that can diverge data.
2. Restore a second member (bring back a downed DC's etcd member, or replace it and let it rejoin) so quorum returns. As soon as two members are up, the Lease becomes writable and a Member re-acquires the primary.
3. Last resort only, with data loss accepted and sign-off: if a permanent two DC loss makes the remaining single member the only survivor, recover etcd as a new single-member cluster from that member's data (`etcdutl snapshot restore` / `etcd --force-new-cluster`) and then re-add members. This abandons the quorum guarantee for the duration and can lose the most recent writes; treat it as disaster recovery, not routine.

### Handoff stuck (a planned switchover did not complete)

`handoff-to` is set but the target never acquires. Causes: the target is not a Member, the target is unhealthy, or the target was removed from the member set mid handoff (the service ignores a bad target so a scope is never left with no primary, which means the old holder keeps the Lease).

1. Inspect: `kubectl -n dc-failover get lease primary-dc -o yaml` and confirm `handoff-to` names a DC that is in `member-dcs` and whose `dc-health-<dc>` is fresh.
2. If the target is unhealthy, fix the target DC first (agent up, etcd member healthy), then the handoff completes on its own.
3. To abort a stuck handoff, clear the annotation and let normal contention resume with the current holder:
   ```
   kubectl -n dc-failover annotate lease primary-dc dr.open-cluster-management.io/handoff-to-
   ```
4. Re-issue `switchover --to <dc>` once the target is healthy.

### A control plane apiserver replica is down

Replicas are stateless; their state is the external etcd. The load balancer routes around a dead replica.

1. Confirm clients still reach the endpoint: `dr-controlplane status` succeeds.
2. If `status` fails, the fault is the load balancer / GSLB or the etcd behind every replica, not one replica. Check the LB target health and etcd quorum.
3. Replace the replica; no Lease state is lost.

### An agent is down in a data center

That DC's `dc-health-<dc>` goes stale and, if it was contending, it stops. If it held the primary, its Lease expires and another Member takes over (a failover). If it was a standby Member, nothing moves.

1. Check the agent pod: `kubectl -n <agent-ns> logs deploy/dr-agent-<dc>` (or the agent pod name from your chart release).
2. Restart it. On restart it resumes renewing health and, if it is a Member, rejoins contention (the former primary re-contends rather than idling).

### The active DC marker is stale (a DC is fenced read only)

Each agent projects a marker ConfigMap onto its local spoke (named after the scope's primary Lease, in the marker namespace) with `activeDC`, `renewTime`, and `quiesce`. Consumers read it locally and fail closed: if the `renewTime` stops advancing past the consumer's fence TTL, the local primary demotes itself. An agent can renew its health Lease yet still fail to write its marker (a bad `--spoke-kubeconfig` or `--marker-namespace`, or an unreachable spoke apiserver), so a DC can be elected active while its consumers stay fenced read only.

1. Read the marker on the affected spoke: `kubectl -n <marker-ns> get configmap <primary-lease-name> -o yaml`. A missing ConfigMap, or a `renewTime` that is not advancing, is the problem.
2. Check the agent's projector logs: `active DC marker projector` (logged when the spoke client becomes ready) and `failed to project active DC marker` or `active DC marker projection waiting` (the spoke client cannot be built). The spoke client is retried every tick, so a transient failure self heals; a persistent one is a misconfig.
3. Confirm the projector flags: `--spoke-kubeconfig` (empty means in cluster, correct when the agent runs in its own DC), `--marker-namespace` (must match where the consumer fence reads), `--marker-refresh-interval` (must be well under the consumer fence TTL).
4. Once the marker refreshes, the consumer fence clears on its own; no Lease action is needed.

### The topology controller is down

The single `controller` reconciles the `member-dcs` annotation from PlacementPolicies. While it is down, agents and consumers keep working off the last-written member set; only changes to PlacementPolicy roles or new scopes are not reconciled.

1. This is not a failover emergency; the failover path (agents + etcd) does not depend on the controller being up.
2. Restart the controller. Confirm it re-derives the expected Members: `dr-controlplane status` shows the correct member set per scope.
3. If it refuses a scope, check `--require-spread` and the `--region` mapping: the controller can be configured to refuse a scope that does not span three failure domains.

## Routine procedures

### Planned switchover and failback

Use for maintenance and for failing back to a preferred DC after it recovers. This is the only safe way to move the primary to a chosen DC (plain leader election has no priority, so a manual release is a race).

```
dr-controlplane switchover --to dc-b               # global
dr-controlplane switchover --group orders --to dc-b
```

The current holder releases, non target Members pause, and the target (which must be a Member) acquires without a race. Then the annotation clears and normal contention resumes.

1. Preconditions: the target is a Member and its `dc-health-<dc>` is fresh; the consumer reports the target is caught up (low lag). The control plane does not know lag; confirm it in the consumer before switching.
2. For a near-zero-RPO handoff, the consumer's hub orchestrator quiesces writes on the active DC first. The primary DC Lease carries the `dr.open-cluster-management.io/quiesce` annotation naming the current holder; each agent projects it onto its spoke as the marker's `data.quiesce`, and the active DC's consumer reads that to hold its primary read-only until the target has caught up, then the handoff completes. The control plane only carries the signal; the quiesce and catch-up logic live in the engine-aware consumer.
3. Verify: `dr-controlplane status` shows the new holder and no `handoff-to`.

### Add or remove a data center

Three coordinated steps; do them in this order.

- **Add**: add an etcd member for the new DC (`etcdctl member add`, then start that member), install the `agent` in the new DC (`helm install ... --set agent.dcName=<dc>`), and add its `distributionRule` with a `role` to the workloads' PlacementPolicy. The controller updates `member-dcs`; the new agent begins renewing health and, if a Member, contending.
- **Remove**: drain the role from PlacementPolicy first (so the controller drops it from `member-dcs` and no handoff can target it), then remove its agent, then remove the etcd member last (`etcdctl member remove`). Never remove an etcd member while it is still a candidate; removing it from the member set first prevents a handoff race.

Keep etcd at three voting members. Changing the voting count changes the quorum math and the failure model.

### Change the trigger granularity (global vs group)

Edit the workload's PlacementPolicy `failoverPolicy.trigger` between `Global` and `Group` with a group name. The controller derives one Lease per distinct scope. A `Group` scope (`primary-dc-<group>`) lets a group fail over independently of the fleet, which is what you want when groups have different lag tolerances. Existing consumers must be pointed at the matching scope.

### Upgrade

- **Agents and controller**: rolling restart per component. Agents re-contend on restart (a restarted primary reclaims via normal contention after `LeaseDuration` at worst); the controller is stateless against etcd. Upgrade one DC's agent at a time so you never lose two DCs' agents at once.
- **Control plane apiserver**: rolling upgrade of the replicas behind the load balancer; state is in external etcd, so replicas are disposable. Do not upgrade all replicas simultaneously if that would drop the LB to zero healthy targets.
- **etcd**: upgrade one member at a time, waiting for `endpoint health --cluster` to be green between members, so quorum is never lost.

### Rotate the coordination kubeconfig or certs

The agents, the controller, and the consumers all authenticate to the control plane with a kubeconfig (often from a Secret, for example `coordKubeconfigSecret`). Rotate the Secret, then restart the consumers of it. Because a kubeconfig has a single server URL pointed at the load balancer, cross replica failover is unaffected by the rotation.

## Diagnostics cookbook

- **Who is primary, per scope**: `dr-controlplane status`, or `kubectl -n dc-failover get lease -l app.kubernetes.io/managed-by=dr-controlplane -o custom-columns=NAME:.metadata.name,HOLDER:.spec.holderIdentity,RENEW:.spec.renewTime`.
- **Why did it fail over**: the primary's `dc-health-<dc>` went stale just before the holder changed points at that DC losing the majority (partition or double component loss); the agent logs in that DC show the election loss.
- **Elections flapping / frequent unexpected failovers**: the election durations are too close to the inter DC RTT. `LeaseDuration`, `RenewDeadline`, and `RetryPeriod` must sit comfortably above the round trip time. Raise them; every renewal is a cross DC etcd write. When you raise them, keep the consumer fence TTL strictly inside `LeaseDuration` (see the guardrail below); the defaults are fence TTL 30s, `LeaseDuration` 45s.
- **A consumer is stuck read only in the active DC**: read that spoke's marker ConfigMap; a frozen or missing `renewTime` means the agent's projector is not writing it. See "The active DC marker is stale".
- **etcd slow / high latency**: `etcdctl endpoint status -w table` for DB size and raft term churn; WAN-tune heartbeat and election timeouts above the inter DC RTT.
- **A Lease looks wrong**: check the `managed-by` label and the `scope` annotation to confirm it is one the controller owns, then compare `member-dcs` against the PlacementPolicy roles.

## Guardrails

- Never hand edit `spec.holderIdentity` on a primary DC Lease. The holder is the output of a quorum-committed election; editing it does not move the workload (the service does not promote anything) and can desync consumers.
- Never remove or add etcd members to escape a quorum-lost freeze except as the documented last-resort disaster recovery, with data loss accepted and signed off. The freeze is the safe behavior.
- Never assume a failover means data is safe in the new primary. DC ownership is not data currency. The consumer's lag guard, not this service, decides whether promotion is safe.
- Keep the three-member etcd topology and the WAN-tuned election durations. Both are load-bearing for the failure model.
- Keep the consumer fence TTL plus cross DC clock skew strictly less than `LeaseDuration`. The marker `renewTime` tracks the primary Lease renewTime, so a partitioned active DC self fences at the fence TTL while a survivor acquires the Lease at `LeaseDuration`; invert that relation and a survivor can go writable before the old active DC fences, a split brain window. The defaults hold it with margin (30s < 45s); retune both sides together.

## Escalation

Escalate to the workload owner (the consumer, for example the KubeDB DR driver) when: a failover completed at the control plane (holder changed) but the workload will not promote or stays Degraded; or a planned switchover's Lease moved but the application did not follow. Escalate to the platform/etcd owner when: quorum is lost (two members down), etcd endpoint health cannot be restored, or the load balancer in front of the control plane is unhealthy. Include `dr-controlplane status`, `kubectl -n dc-failover get lease -o yaml`, and `etcdctl endpoint status -w table` output in the escalation.
