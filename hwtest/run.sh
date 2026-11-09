#!/usr/bin/env bash
# hwtest/run.sh: LibrePods hardware regression harness (Linux host + a Mac over SSH + the AirPods).
#   run.sh build                     build and stamp the release binary the harness will run
#   run.sh mac-setup                 put the test track ($MAC_TRACK) into Music on the Mac
#   run.sh list                      list the scenarios
#   run.sh all|<scenario>... [--unattended] [--rounds N] [--keep-app]
# See hwtest/README.md.
set -u
HW_DIR=$(cd "$(dirname "$0")" && pwd)
if [ -z "${HWTEST_SHELL:-}" ]; then
  exec env HWTEST_SHELL=1 nix-shell "$HW_DIR/shell.nix" --run "$(printf '%q ' "$0" "$@")"
fi
[ -f "$HW_DIR/hwtest.env" ] || { echo "copy hwtest/hwtest.env.example to hwtest/hwtest.env and fill it in" >&2; exit 1; }
. "$HW_DIR/hwtest.env"
. "$HW_DIR/lib.sh"
. "$HW_DIR/scenarios.sh"

UNATTENDED='' ROUNDS=3 KEEP_APP='' args=()
while [ $# -gt 0 ]; do
  case $1 in
    --unattended) UNATTENDED=1 ;;
    --rounds) ROUNDS=$2; shift ;;
    --keep-app) KEEP_APP=1 ;;
    *) args+=("$1") ;;
  esac
  shift
done
[ ${#args[@]} -gt 0 ] || { sed -n '2,7p' "$0"; exit 1; }

OUT_ROOT=${OUT_ROOT:-$HW_DIR/out}
OUT=$OUT_ROOT/$(date +%F-%H%M%S)
mkdir -p "$OUT"

case ${args[0]} in
  build) build_app; exit ;;
  list) for s in "${SCENARIOS[@]}"; do printf '%-18s %s\n' "$s" "${DESC[$s]}"; done; exit ;;
  mac-setup)
    mac_ssh "set -e; t=\$(mktemp -t hwtest).aiff; say -o \"\$t\" \"\$(for i in \$(seq 1 60); do printf 'This is the LibrePods hardware test track. '; done)\";
      afconvert -f m4af -d aac \"\$t\" ~/Music/$MAC_TRACK.m4a; rm -f \"\$t\";
      osascript -e 'tell application \"Music\" to add POSIX file \"'\$HOME'/Music/$MAC_TRACK.m4a\"'" && echo "added $MAC_TRACK to Music"
    exit ;;
  all) todo=("${SCENARIOS[@]}") ;;
  *) todo=("${args[@]}") ;;
esac
for s in "${todo[@]}"; do declare -F "scenario_$s" >/dev/null || { echo "unknown scenario: $s (run.sh list)" >&2; exit 1; }; done

: >"$OUT/results.tsv"
check_build || exit 1
trap 'stop_observers; [ -n "$KEEP_APP" ] || stop_app' EXIT
start_observers || exit 1
start_app || exit 1
log "run $OUT: ${todo[*]} (rounds $ROUNDS${UNATTENDED:+, unattended})"

for s in "${todo[@]}"; do
  SCEN=$s; XF=
  log "== $s: ${DESC[$s]}"
  scenario_"$s"
  rc=$?
  for p in "${HELPER_PIDS[@]}"; do kill -- "$p" 2>/dev/null; done; HELPER_PIDS=()
  if [ "$rc" = 2 ] && ! observers_ok >/dev/null; then
    log "an observer died; restarting before the next scenario"
    stop_observers; OBS_PIDS=(); start_observers && { [ -n "$(app_pids)" ] || start_app; } || { log "can't recover; stopping"; break; }
  fi
done
SCEN=-

# ---------- report ----------
verdict() { # verdict SCENARIO
  awk -F'\t' -v s="$1" '$1 == s { v[$3] = 1 } END {
    if (v["INVALID"]) print "INVALID"; else if (v["SETUP"]) print "SETUP"; else if (v["FAIL"]) print "FAIL";
    else if (v["UNCHECKED"]) print "INCOMPLETE"; else if (length(v)) print "PASS"; else print "NOT RUN" }' "$OUT/results.tsv"
}
{
  echo "# LibrePods hardware run $(basename "$OUT")"
  echo
  echo "- Build: \`$(sed -n 's/^src=//p' "$STAMP")\`"
  echo "- macOS: $(mac_ssh sw_vers -productVersion 2>/dev/null || echo '?'); mode: $([ -n "$UNATTENDED" ] && echo unattended || echo attended); rounds: $ROUNDS"
  echo
  echo "| Scenario | Verdict | Notes |"
  echo "|---|---|---|"
  overall=PASS
  for s in "${todo[@]}"; do
    v=$(verdict "$s")
    x=$(awk -F'\t' -v s="$s" '$1 == s && ($3 == "XFAIL" || $3 == "XPASS") { printf "%s %s; ", $3, $2 }' "$OUT/results.tsv")
    echo "| $s | **$v** | ${x%; } |"
    case $v in FAIL | INVALID | SETUP | "NOT RUN") overall=FAIL ;; INCOMPLETE) [ "$overall" = PASS ] && overall=INCOMPLETE ;; esac
  done
  echo
  echo "**Overall: $overall**"
  echo
  echo "## Checks"
  echo
  echo "| Scenario | Check | Verdict | Detail |"
  echo "|---|---|---|---|"
  awk -F'\t' '{ gsub(/\|/, "\\|"); printf "| %s | %s | %s | %s |\n", $1, $2, $3, $4 }' "$OUT/results.tsv"
  echo
  echo "Evidence in this folder: app.log (LibrePods), bluez.log (BlueZ property changes, ms timestamps), mac.log (the Mac's audioaccessoryd), run.log."
} >"$OUT/report.md"
sed -i 's/[0-9A-F]\{2\}\(:[0-9A-F]\{2\}\)\{5\}/<addr>/g' "$OUT/report.md" # the report is meant for posting
echo
sed -n '/^| Scenario | Verdict/,/^\*\*Overall/p' "$OUT/report.md"
echo "report: $OUT/report.md"
