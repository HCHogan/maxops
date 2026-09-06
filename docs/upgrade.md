# Upgrade and rollout

maxops is one fleet writer among humans and other automation. A rollout must
observe the Git remote and each running host again; the Hub's stored deployment
history is evidence about its own work, not an inventory of every fleet change.

## Before changing a host

1. Fetch the consuming configuration repository and review commits that are not
   in the checkout. Do not deploy from a stale branch or overwrite a moved
   remote ref.
2. Compare the host's observed `/run/current-system` closure and persistent
   system profile with the source being evaluated. A manual rebuild can move
   either independently of maxops.
3. Check `self.status`, `jobs.list`, and `changes.history`. Let active jobs on
   the host finish or cancel jobs that are still definitively queued. Resolve
   every `outcome_unknown` by observing the target before continuing.
4. Back up the Hub and executor SQLite files while the services are stopped, or
   use a SQLite-consistent backup. Database migrations are forward-only; an old
   binary is not a supported reader of a database opened by a newer binary.

## Rolling order

Roll one host at a time. Update its executor and agent from the same maxops
package revision, verify `/healthz`, perform one observation, then exercise a
harmless configured execution profile. Update the Hub after all selected target
components work. Finally update `maxopsctl` and optional `maxops-mcp` clients.

There is no independent agent/executor wire-version negotiation yet. Keep the
Hub, agent and executor on one release during normal operation. A newer client
discovers the Hub's permitted registry from `/v1/operations`; it must only use
the returned operations and honor each operation's minimum protocol version.

Between hosts, fetch the configuration remote and re-observe runtime state
again. If either moved, discard or re-prepare the affected change plan. Do not
continue an activation merely because an earlier maxops job succeeded.

## Read-only fallback

Observation and execution credentials are separate. To keep a host visible
while its executor is unavailable or incompatible, remove that host's
`executionTokenFile`, diagnostic probes, repository executor assignments and
deployment profiles from the Hub configuration. Keep its agent token, readable
units and observation grants. The catalog remains principal-scoped; mutation
requests involving that host fail explicitly while `host.facts`, `units.*`
reads and fleet observations continue to work.

To disable execution permanently:

1. Drain or resolve nonterminal jobs and record the last known target state.
2. Deploy the Hub policy without the execution credential and execution-backed
   objects, then verify a read succeeds and a mutation is rejected.
3. Disable the target executor service and remove its credential only after the
   Hub can no longer dispatch to it.

Re-enabling follows the reverse dependency order: install and verify the target
executor, add its credential and policy to the Hub, then test a bounded profile.

## Rollback

Application rollback means restoring the previous package and its compatible
configuration. Restore the matching pre-upgrade database backup if the newer
binary ran migrations. Before resuming writes, fetch Git and observe every
target again. Treat work whose result cannot be matched to current source and
runtime identity as unknown or superseded; never replay it automatically.
