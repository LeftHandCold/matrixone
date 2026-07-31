# TAE Object Lifecycle certification harness

This harness executes the Commercial GA data-volume and coexistence profiles
against an already deployed MatrixOne cluster. It does not enable the feature,
certify a Stage, or claim that elapsed soak work completed.

Required tools:

- a MySQL-compatible client;
- Python 3;
- `curl` when metrics capture is enabled.

Example smoke run:

```bash
MO_LIFECYCLE_PROFILE=smoke \
MO_LIFECYCLE_ACTION=DELETE \
MO_MYSQL_HOST=127.0.0.1 \
MO_MYSQL_PORT=6001 \
MO_MYSQL_USER=dump \
MO_MYSQL_PASSWORD=111 \
MO_METRICS_URL=http://127.0.0.1:7001/metrics \
MO_LIFECYCLE_ALLOW_DROP=1 \
./tools/lifecycle-certification/run.sh
```

Supported profiles are `smoke`, `1tib`, `10tib`, `coexist-50`,
`coexist-200`, `coexist-500`, and `coexist-1000`. For ARCHIVE, also set
`MO_LIFECYCLE_STAGE` to a Stage already present in the release
certification. Run with `MO_LIFECYCLE_DRY_RUN=1` to validate and record the
planned workload without connecting to MatrixOne.

`1tib` and `10tib` each certify one table at that logical size; the coexistence
profiles separately certify many bound tables. The profile may be passed as
the first argument or through `MO_LIFECYCLE_PROFILE`. `MO_LIFECYCLE_LOAD_BATCH_ROWS`
controls the bounded load transaction size; by default the harness chooses the
smallest single-round batch for small/coexistence profiles and caps large
single-table loads at 131072 rows per transaction.

Set `MO_LIFECYCLE_WAIT_SECONDS` to make the run poll all generated tables until
their active row count reaches zero. A timed run that does not finish exits
with status 3 after writing evidence; a run with no wait remains workload
evidence, not retirement-completion evidence. `MO_LIFECYCLE_POLL_SECONDS`
controls the poll interval.

An executable `MO_LIFECYCLE_FAULT_HOOK` may drive deployment-specific chaos.
It is invoked with `MO_LIFECYCLE_FAULT_PHASE` set to `after-load`,
`after-bind`, and `before-verify`, and receives the evidence directory in
`MO_LIFECYCLE_RUN_DIR`. The hook output is preserved in `fault-hook.log`.
This keeps CN/TN/provider restart mechanics outside the database kernel while
using the same deterministic Lifecycle fault boundaries in unit tests.

The executable run recreates only a database named `lifecycle_cert` or
`lifecycle_cert_<suffix>` and requires the explicit
`MO_LIFECYCLE_ALLOW_DROP=1` acknowledgement.

Every run creates a new evidence directory containing:

- the exact SQL stream and command output;
- Lifecycle Prometheus metrics before and after the run;
- optional deployment-specific fault-hook output;
- `evidence.json`, whose booleans distinguish executed evidence from a plan
  and from verified retirement or owner-reviewed release evidence.

The 30-day soak is an external release activity. Run one daily profile per
day into the same durable evidence root, retain CN/TN/provider logs and
profiles, and mark it complete only through the release checklist in the
operator runbook.
