#!/usr/bin/env bash
#
# Repeatable MatrixOne TAE Object Lifecycle certification driver.
# It creates only disposable objects under MO_LIFECYCLE_DATABASE and never
# changes the cluster release gate or Stage certification.

set -euo pipefail

profile="${MO_LIFECYCLE_PROFILE:-${1:-smoke}}"
action="${MO_LIFECYCLE_ACTION:-DELETE}"
database="${MO_LIFECYCLE_DATABASE:-lifecycle_cert}"
stage="${MO_LIFECYCLE_STAGE:-}"
mysql_bin="${MO_MYSQL_BIN:-mysql}"
host="${MO_MYSQL_HOST:-127.0.0.1}"
port="${MO_MYSQL_PORT:-6001}"
user="${MO_MYSQL_USER:-dump}"
password="${MO_MYSQL_PASSWORD:-111}"
metrics_url="${MO_METRICS_URL:-}"
evidence_dir="${MO_LIFECYCLE_EVIDENCE_DIR:-./lifecycle-certification-evidence}"
dry_run="${MO_LIFECYCLE_DRY_RUN:-0}"
allow_drop="${MO_LIFECYCLE_ALLOW_DROP:-0}"
wait_seconds="${MO_LIFECYCLE_WAIT_SECONDS:-0}"
poll_seconds="${MO_LIFECYCLE_POLL_SECONDS:-30}"
batch_rows="${MO_LIFECYCLE_LOAD_BATCH_ROWS:-}"
fault_hook="${MO_LIFECYCLE_FAULT_HOOK:-}"

if [[ ! "${database}" =~ ^lifecycle_cert(_[A-Za-z0-9_]+)?$ ]]; then
  echo "MO_LIFECYCLE_DATABASE must use the lifecycle_cert[_suffix] namespace" >&2
  exit 2
fi
if [[ -n "${stage}" && ! "${stage}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
  echo "MO_LIFECYCLE_STAGE must be a simple SQL identifier" >&2
  exit 2
fi
if [[ "${dry_run}" != "0" && "${dry_run}" != "1" ]]; then
  echo "MO_LIFECYCLE_DRY_RUN must be 0 or 1" >&2
  exit 2
fi
if [[ "${dry_run}" != "1" && "${allow_drop}" != "1" ]]; then
  echo "set MO_LIFECYCLE_ALLOW_DROP=1 to authorize recreating ${database}" >&2
  exit 2
fi
if [[ -n "${fault_hook}" && ! -x "${fault_hook}" ]]; then
  echo "MO_LIFECYCLE_FAULT_HOOK must name an executable file" >&2
  exit 2
fi

case "${profile}" in
  smoke)
    target_bytes=$((64 * 1024 * 1024))
    table_count=4
    ;;
  1tib)
    target_bytes=$((1 * 1024 * 1024 * 1024 * 1024))
    table_count=1
    ;;
  10tib)
    target_bytes=$((10 * 1024 * 1024 * 1024 * 1024))
    table_count=1
    ;;
  coexist-50)
    target_bytes=$((64 * 1024 * 1024))
    table_count=50
    ;;
  coexist-200)
    target_bytes=$((256 * 1024 * 1024))
    table_count=200
    ;;
  coexist-500)
    target_bytes=$((512 * 1024 * 1024))
    table_count=500
    ;;
  coexist-1000)
    target_bytes=$((1024 * 1024 * 1024))
    table_count=1000
    ;;
  *)
    echo "unsupported MO_LIFECYCLE_PROFILE=${profile}" >&2
    exit 2
    ;;
esac

row_payload_bytes=1024
if [[ -z "${batch_rows}" ]]; then
  batch_rows=$(((target_bytes + table_count * row_payload_bytes - 1) /
    (table_count * row_payload_bytes)))
  if ((batch_rows > 131072)); then
    batch_rows=131072
  fi
fi
for number in "${wait_seconds}" "${poll_seconds}" "${batch_rows}"; do
  if [[ ! "${number}" =~ ^[0-9]+$ ]]; then
    echo "wait, poll, and batch row settings must be unsigned integers" >&2
    exit 2
  fi
done
if ((poll_seconds == 0 || batch_rows == 0)); then
  echo "MO_LIFECYCLE_POLL_SECONDS and MO_LIFECYCLE_LOAD_BATCH_ROWS must be positive" >&2
  exit 2
fi

case "${action}" in
  DELETE) ;;
  ARCHIVE)
    if [[ -z "${stage}" ]]; then
      echo "MO_LIFECYCLE_STAGE is required for ARCHIVE" >&2
      exit 2
    fi
    ;;
  *)
    echo "MO_LIFECYCLE_ACTION must be DELETE or ARCHIVE" >&2
    exit 2
    ;;
esac

if [[ "${dry_run}" != "1" ]]; then
  command -v "${mysql_bin}" >/dev/null
fi
command -v python3 >/dev/null
mkdir -p "${evidence_dir}"

action_lower="$(printf '%s' "${action}" | tr '[:upper:]' '[:lower:]')"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-${profile}-${action_lower}"
run_dir="${evidence_dir}/${run_id}"
mkdir -p "${run_dir}"
sql_log="${run_dir}/sql.log"
fault_log="${run_dir}/fault-hook.log"
metrics_before="${run_dir}/metrics-before.prom"
metrics_after="${run_dir}/metrics-after.prom"
: >"${fault_log}"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

export MYSQL_PWD="${password}"
mysql_args=(
  "--host=${host}"
  "--port=${port}"
  "--user=${user}"
  "--batch"
  "--raw"
  "--skip-column-names"
)

sql() {
  local statement="$1"
  printf '%s\n' "${statement}" >>"${sql_log}"
  if [[ "${dry_run}" != "1" ]]; then
    "${mysql_bin}" "${mysql_args[@]}" --execute="${statement}" >>"${sql_log}" 2>&1
  fi
}

run_fault_hook() {
  local phase="$1"
  if [[ -z "${fault_hook}" || "${dry_run}" == "1" ]]; then
    return
  fi
  MO_LIFECYCLE_FAULT_PHASE="${phase}" \
    MO_LIFECYCLE_RUN_DIR="${run_dir}" \
    MO_LIFECYCLE_PROFILE="${profile}" \
    MO_LIFECYCLE_ACTION="${action}" \
    "${fault_hook}" >>"${fault_log}" 2>&1
}

query_scalar() {
  local statement="$1"
  printf '%s\n' "${statement}" >>"${sql_log}"
  "${mysql_bin}" "${mysql_args[@]}" --execute="${statement}" \
    2>>"${sql_log}" | tail -n 1
}

capture_metrics() {
  local destination="$1"
  if [[ -n "${metrics_url}" && "${dry_run}" != "1" ]]; then
    curl --fail --silent --show-error "${metrics_url}" |
      grep '^mo_lifecycle_' >"${destination}"
  else
    : >"${destination}"
  fi
}

bytes_per_round=$((table_count * batch_rows * row_payload_bytes))
rounds=$(((target_bytes + bytes_per_round - 1) / bytes_per_round))

capture_metrics "${metrics_before}"
sql "drop database if exists \`${database}\`"
sql "create database \`${database}\`"

for ((table_index = 0; table_index < table_count; table_index++)); do
  table_name="events_${table_index}"
  sql "create table \`${database}\`.\`${table_name}\` (
id bigint,
created_at timestamp not null,
payload varchar(2048) not null
)"
done

for ((round = 0; round < rounds; round++)); do
  for ((table_index = 0; table_index < table_count; table_index++)); do
    table_name="events_${table_index}"
    offset=$(((round * table_count + table_index) * batch_rows))
    sql "insert into \`${database}\`.\`${table_name}\`
select ${offset}+g.result,
       date_sub(utc_timestamp(), interval 120 day),
       repeat(char(65 + (${table_index} % 26)), ${row_payload_bytes})
from generate_series(1, ${batch_rows}) g"
  done
done
run_fault_hook "after-load"

for ((table_index = 0; table_index < table_count; table_index++)); do
  table_name="events_${table_index}"
  if [[ "${action}" == "ARCHIVE" ]]; then
    sql "alter table \`${database}\`.\`${table_name}\` set lifecycle (
column created_at,
expire after interval 90 day,
action archive,
stage \`${stage}\`,
purge eligible after interval 365 day
)"
  else
    sql "alter table \`${database}\`.\`${table_name}\` set lifecycle (
column created_at,
expire after interval 90 day,
action delete
)"
  fi
done
run_fault_hook "after-bind"

retirement_completed=false
remaining_rows=-1
if [[ "${dry_run}" != "1" && "${wait_seconds}" -gt 0 ]]; then
  remaining_sql="select sum(remaining_rows) from ("
  for ((table_index = 0; table_index < table_count; table_index++)); do
    if ((table_index > 0)); then
      remaining_sql+=" union all "
    fi
    remaining_sql+="select count(*) remaining_rows from \`${database}\`.\`events_${table_index}\`"
  done
  remaining_sql+=") lifecycle_remaining"
  deadline_epoch=$(( $(date +%s) + wait_seconds ))
  while true; do
    if value="$(query_scalar "${remaining_sql}")" &&
      [[ "${value}" =~ ^[0-9]+$ ]]; then
      remaining_rows="${value}"
      if ((remaining_rows == 0)); then
        retirement_completed=true
        break
      fi
    fi
    if (($(date +%s) >= deadline_epoch)); then
      break
    fi
    sleep "${poll_seconds}"
  done
fi
run_fault_hook "before-verify"

if [[ "${dry_run}" != "1" ]]; then
  sql "show lifecycle jobs"
  sql "select count(*) from mo_catalog.mo_lifecycle_bindings where state='ACTIVE'"
  sql "select state,count(*),coalesce(sum(row_count),0),coalesce(sum(logical_bytes),0)
from mo_catalog.mo_lifecycle_datasets group by state order by state"
fi
capture_metrics "${metrics_after}"
finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

python3 - "${run_dir}/evidence.json" <<PY
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
path.write_text(json.dumps({
    "schema_version": 1,
    "run_id": "${run_id}",
    "profile": "${profile}",
    "action": "${action}",
    "started_at": "${started_at}",
    "finished_at": "${finished_at}",
    "dry_run": ${dry_run},
    "database": "${database}",
    "target_logical_bytes": ${target_bytes},
    "table_count": ${table_count},
    "row_payload_bytes": ${row_payload_bytes},
    "batch_rows": ${batch_rows},
    "rounds": ${rounds},
    "artifacts": {
        "sql_log": "sql.log",
        "metrics_before": "metrics-before.prom",
        "metrics_after": "metrics-after.prom",
        "fault_hook_log": "fault-hook.log"
    },
    "certification": {
        "data_loaded": ${dry_run} == 0,
        "retirement_completed": "${retirement_completed}" == "true",
        "remaining_active_rows": ${remaining_rows},
        "elapsed_soak_completed": False,
        "owner_reviewed": False
    }
}, indent=2, sort_keys=True) + "\\n")
PY

printf 'Lifecycle certification evidence: %s\n' "${run_dir}/evidence.json"
if [[ "${dry_run}" != "1" && "${wait_seconds}" -gt 0 &&
  "${retirement_completed}" != "true" ]]; then
  echo "Lifecycle retirement did not finish within ${wait_seconds}s" >&2
  exit 3
fi
