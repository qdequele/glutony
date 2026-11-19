#!/usr/bin/env bash
# Reproduction, on Tinybird Local, of what the hourly billing copy
# (tinybird/pipes/usage_daily_billing_hourly.pipe, `COPY_MODE replace`) does to
# months its query no longer returns. Review findings USG-01 and USG-02. It reads
# and reports; it changes nothing in the repository.
#
#   scripts/repro-tinybird-billing-copy.sh
#
# Needs the Tinybird Forward CLI (`tb`, 4.6+), docker, python3, and Tinybird
# Local running (`tb local start --daemon`).
#
# The copy limits its result to `toDate(u.ts) >= toStartOfMonth(subtractDays(today(), 2))`,
# so from the 3rd of a month on it returns the current month only. The pipe's
# comment assumes `replace` swaps only the partitions (toYYYYMM(day)) the query
# produces; if it replaces the whole data source instead, every run erases every
# older month from usage_daily_billing (USG-01). Separately, a raw row for last
# month that arrives after the 2nd is outside the query, so it never reaches
# usage_daily_billing whatever the mode does (USG-02).
#
# How: the server clock cannot be moved, so the script deploys a throwaway copy
# of tinybird/ (its own Tinybird Local workspace, keyed on the directory) in
# which the copy pipe's `today()` is the only change, replaced with a fixed date.
# Every data source, the rest of the SQL and the copy mode are the repository's.
# The rows are dated in the previous and the current real month so the raw
# table's 180-day TTL keeps them. Steps:
#
#   1. "today" = the 15th of the previous month (A). Append one A job row to
#      meili_ingest_usage, run the copy: usage_daily_billing holds month A.
#   2. "today" = the 5th of the current month (B). Append one B job row, run the
#      copy: does month A survive in usage_daily_billing? (USG-01)
#   3. Still on the 5th of B, append a second A job row (a late delivery) and run
#      the copy again: does usage_daily_billing count it? (USG-02)
#
# Each copy runs with `--mode replace`, the mode the scheduled run uses. The
# workspace is left in Tinybird Local for inspection; the temporary directory
# is removed.
#
# Exit status: 0 when neither defect shows, 1 when at least one is confirmed,
# 2 when a prerequisite is missing or a step fails.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
pipe=pipes/usage_daily_billing_hourly.pipe

die() { echo "repro: $*" >&2; exit 2; }

command -v tb >/dev/null || die "the Tinybird Forward CLI (tb) is not installed: curl -LsSf https://tbrd.co/fwd | sh"
command -v docker >/dev/null || die "docker is not installed (Tinybird Local runs in it)"
command -v python3 >/dev/null || die "python3 is not installed"
tb local status >/dev/null 2>&1 || die "Tinybird Local is not running: tb local start --daemon"

# Month A (previous) and B (current), from the real date.
read -r a_today a_row a_late b_today b_row a_month b_month <<EOF
$(python3 -I -c '
import datetime as d
t = d.date.today()
b = t.replace(day=1)
a = (b - d.timedelta(days=1)).replace(day=1)
print(a.replace(day=15), f"{a.replace(day=10)}T12:00:00.000Z", f"{a.replace(day=28)}T12:00:00.000Z",
      b.replace(day=5), f"{b.replace(day=3)}T12:00:00.000Z", a.strftime("%Y%m"), b.strftime("%Y%m"))
')
EOF

work="$(mktemp -d "${TMPDIR:-/tmp}/repro-tinybird.XXXXXX")"
trap 'rm -rf "$work"' EXIT
cp -R "$root/tinybird/." "$work/project"
rm -rf "$work/project/fixtures" "$work/project/.tinyb"
cp "$work/project/$pipe" "$work/pipe.orig"
grep -q 'today()' "$work/pipe.orig" || die "$pipe no longer calls today(); update this script"
project="repro-$(python3 -I -c 'import uuid; print(uuid.uuid4().hex[:12])')"

tbl() { (cd "$work/project" && tb --local "$@"); }

# deploy_as DATE: deploy the project with the copy's today() fixed to DATE.
deploy_as() {
  sed "s/today()/toDate('$1')/g" "$work/pipe.orig" >"$work/project/$pipe"
  echo "--- deploy, copy pipe's today() = $1"
  tbl deploy >"$work/deploy.log" 2>&1 || { cat "$work/deploy.log" >&2; die "tb --local deploy failed"; }
}

# append_job EVENT_ID TS: one succeeded job row for $project.
append_job() {
  printf '{"event_id":"%s","kind":"job","ts":"%s","job_id":"%s","workflow_id":"ingest-%s","project_id":"%s","region":"local","pipeline_uid":"repro","pipeline_builtin":0,"index_name":"repro","step_id":"","plugin":"","task_queue":"workers-general","status":"succeeded","attempt":1,"branches":0,"duration_ms":1,"input_bytes":1,"input_mime":"application/json","documents_out":1,"llm_input_tokens":0,"llm_output_tokens":0,"llm_requests":0,"audio_seconds":0.0,"pages":0,"images":0,"external_requests":0,"error_kind":"","error":""}\n' \
    "$1" "$2" "$1" "$1" "$project" >"$work/$1.ndjson"
  echo "--- append raw job row $1 (ts $2)"
  tbl datasource append meili_ingest_usage --file "$work/$1.ndjson" >"$work/append.log" 2>&1 ||
    { cat "$work/append.log" >&2; die "tb --local datasource append failed"; }
}

run_copy() {
  echo "--- copy run usage_daily_billing_hourly --mode replace"
  tbl copy run usage_daily_billing_hourly --mode replace --wait --yes >"$work/copy.log" 2>&1 ||
    { cat "$work/copy.log" >&2; die "tb --local copy run failed"; }
}

# scalar SQL: the integer the query's single `n` column returns, read from the
# CLI's table output through a marker so the output format does not matter.
scalar() {
  local out
  out="$(tbl sql "SELECT concat('REPRO<', toString(n), '>') AS r FROM ($1)" 2>&1)" ||
    { echo "$out" >&2; die "tb --local sql failed"; }
  sed -n 's/.*REPRO<\([0-9]*\)>.*/\1/p' <<<"$out" | head -n 1
}
billing_jobs() { scalar "SELECT toUInt64(sum(jobs)) AS n FROM usage_daily_billing WHERE project_id = '$project' AND toYYYYMM(day) = $1"; }
raw_jobs() { scalar "SELECT count() AS n FROM meili_ingest_usage FINAL WHERE project_id = '$project' AND kind = 'job' AND toYYYYMM(ts) = $1"; }

report() {
  echo "    usage_daily_billing jobs: $a_month=$(billing_jobs "$a_month") $b_month=$(billing_jobs "$b_month")" \
    "| raw FINAL jobs: $a_month=$(raw_jobs "$a_month") $b_month=$(raw_jobs "$b_month")"
}

echo "project_id $project, month A $a_month, month B $b_month, workspace dir $work/project"

deploy_as "$a_today"
append_job "$project-a1" "$a_row"
run_copy
report
[ "$(billing_jobs "$a_month")" = 1 ] || die "step 1: month $a_month did not reach usage_daily_billing; the reproduction cannot judge the next steps"

deploy_as "$b_today"
append_job "$project-b1" "$b_row"
run_copy
report
a_after="$(billing_jobs "$a_month")"
b_after="$(billing_jobs "$b_month")"
[ "$b_after" = 1 ] || die "step 2: month $b_month did not reach usage_daily_billing"

append_job "$project-a2" "$a_late"
run_copy
report
a_late_billed="$(billing_jobs "$a_month")"
a_late_raw="$(raw_jobs "$a_month")"

status=0
echo
if [ "$a_after" = 1 ]; then
  echo "USG-01 NOT REPRODUCED: month $a_month kept its row in usage_daily_billing after a replace run that returned month $b_month only (replace swapped only the partitions the query produced)."
else
  echo "USG-01 CONFIRMED: month $a_month went from 1 job to ${a_after:-0} in usage_daily_billing after a replace run that returned month $b_month only (replace replaced the whole data source)."
  status=1
fi
# Judged on the increase, so a month already wiped by USG-01 does not decide it.
if [ "${a_late_billed:-0}" -gt "${a_after:-0}" ]; then
  echo "USG-02 NOT REPRODUCED: the $a_month row delivered on $b_today is counted (usage_daily_billing ${a_after:-0} -> $a_late_billed jobs, raw $a_late_raw)."
else
  echo "USG-02 CONFIRMED: the $a_month row delivered on $b_today is in the raw table ($a_late_raw jobs for $a_month) but usage_daily_billing stays at ${a_late_billed:-0}."
  status=1
fi
exit "$status"
