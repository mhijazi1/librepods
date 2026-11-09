# hwtest/lib.sh: shared helpers for the LibrePods hardware regression harness. Sourced by run.sh.
# Everything that observes the system writes into $OUT, and every check lands in $OUT/results.tsv as
#   scenario <TAB> check <TAB> verdict <TAB> detail
# Verdicts: PASS, FAIL, XFAIL (failed as a known open issue expects), XPASS (a known-issue check passed:
# the issue may be fixed), UNCHECKED (a human check skipped with --unattended), SETUP (a precondition
# wasn't met, so the scenario didn't run), INVALID (an observer died, so nothing after that is evidence).

# ---------- config and paths ----------
REPO=$(git -C "$HW_DIR/.." rev-parse --show-toplevel)
BIN=$REPO/.cargo-target/release/librepods
STAMP=$BIN.hwtest-stamp
STATE_DIR=${XDG_STATE_HOME:-$HOME/.local/state}/librepods
JSONL=$STATE_DIR/handoff.jsonl
AP_ID=${AIRPODS//:/_}
POLL=0.2

now_ms() { date +%s%3N; }
log() { printf '%s %s\n' "$(date +%T)" "$*" | tee -a "$OUT/run.log" >&2; }

# ---------- build identity ----------
# The source identity covers what the binary is built from (linux-rust/, flake.nix, flake.lock) at HEAD,
# plus any uncommitted or untracked change to it. Commits elsewhere (docs, hwtest/) don't change it.
src_id() {
  local tree diff untracked
  tree=$(git -C "$REPO" rev-parse HEAD:linux-rust HEAD:flake.nix HEAD:flake.lock | sha256sum | cut -c1-16)
  diff=$(git -C "$REPO" diff HEAD -- linux-rust flake.nix flake.lock | sha256sum | cut -c1-12)
  untracked=$(git -C "$REPO" ls-files -o --exclude-standard -- linux-rust | sort | xargs -r cat 2>/dev/null | sha256sum | cut -c1-12)
  echo "$(git -C "$REPO" rev-parse --short HEAD)/tree-$tree-$diff-$untracked"
}

build_app() {
  log "building release binary into $BIN"
  (cd "$REPO" && CARGO_TARGET_DIR="$REPO/.cargo-target" nix develop -c bash -c "cd linux-rust && cargo build --locked --release") || return 1
  printf 'src=%s\nbin=%s\n' "$(src_id)" "$(sha256sum "$BIN" | cut -d' ' -f1)" >"$STAMP"
  log "stamped $(head -1 "$STAMP")"
}

# Refuse to test a binary that wasn't built by build_app from the current sources.
check_build() {
  [ -x "$BIN" ] && [ -f "$STAMP" ] || { echo "no stamped build at $BIN; run: hwtest/run.sh build" >&2; return 1; }
  local want_src want_bin
  want_src=$(sed -n 's/^src=//p' "$STAMP"); want_bin=$(sed -n 's/^bin=//p' "$STAMP")
  [ "${want_src#*/}" = "$(src_id | cut -d/ -f2)" ] || { echo "stale build: linux-rust or the flake changed since $want_src; run: hwtest/run.sh build" >&2; return 1; }
  [ "$want_bin" = "$(sha256sum "$BIN" | cut -d' ' -f1)" ] || { echo "binary changed after it was stamped; run: hwtest/run.sh build" >&2; return 1; }
}

# ---------- results ----------
SCEN=-
XF=
result() { # result VERDICT check detail
  local v=$1 d=$3
  if [ -n "$XF" ]; then
    case $v in
      FAIL) v=XFAIL d="$d (known issue #$XF)" ;;
      PASS) v=XPASS d="$d (known issue #$XF passed: is it fixed?)" ;;
    esac
    XF=
  fi
  printf '%s\t%s\t%s\t%s\n' "$SCEN" "$2" "$v" "$d" >>"$OUT/results.tsv"
  log "  $v  $2${d:+: $d}"
}
xfail() { XF=$1; } # the next check is expected to fail until issue $1 is fixed
setup_fail() { result SETUP "precondition" "$1"; return 3; }
invalid() { result INVALID "observers" "$1"; return 2; }

# ---------- observers ----------
OBS_PIDS=() HELPER_PIDS=()

start_observers() {
  : >"$OUT/bluez.log"
  # BlueZ property changes (transport State, device Connected), timestamped on arrival. Its own process
  # group (setsid), so stopping it also stops gdbus.
  setsid bash -c 'stdbuf -oL gdbus monitor --system --dest org.bluez 2>&1 | while IFS= read -r l; do
     case $l in *MediaTransport1*State* | *org.bluez.Device1*Connected* | Monitoring*) printf "%s %s\n" "$(date +%s%3N)" "$l" ;; esac
   done' >>"$OUT/bluez.log" &
  OBS_PIDS+=(-$!); OBS_BLUEZ=$!
  # The Mac's routing daemon.
  mac_ssh "/usr/bin/log stream --style compact --level debug --predicate 'process == \"audioaccessoryd\" OR (process == \"bluetoothd\" AND composedMessage CONTAINS[c] \"ownership\")'" \
    >"$OUT/mac.log" 2>&1 &
  OBS_PIDS+=($!); OBS_MAC=$!
  local i
  for i in $(seq 1 50); do grep -q Monitoring "$OUT/bluez.log" && grep -q . "$OUT/mac.log" && break; sleep $POLL; done
  grep -q Monitoring "$OUT/bluez.log" || { echo "BlueZ monitor didn't start" >&2; return 1; }
  grep -q . "$OUT/mac.log" || { echo "Mac log stream didn't start (ssh $MAC_SSH)" >&2; return 1; }
  # Keep the Mac awake: an idle sleep disconnects the AirPods mid-test.
  mac_ssh 'nohup caffeinate -dimsu -t 7200 >/dev/null 2>&1 &' || true
}

stop_observers() { local p; for p in "${OBS_PIDS[@]}" "${HELPER_PIDS[@]}"; do kill -- "$p" 2>/dev/null; done; }

app_pids() { local p; for p in $(pgrep -x librepods); do [ "$(readlink -f /proc/$p/exe)" = "$(readlink -f "$BIN")" ] && echo "$p"; done; true; }

# observers_ok: every observer, and the app, still running.
observers_ok() {
  kill -0 "$OBS_BLUEZ" 2>/dev/null || { echo "BlueZ monitor died"; return 1; }
  kill -0 "$OBS_MAC" 2>/dev/null || { echo "Mac log stream died"; return 1; }
  [ -n "$(app_pids)" ] || { echo "LibrePods isn't running"; return 1; }
}

# ---------- the app ----------
start_app() {
  check_build || return 1
  local other="" p
  for p in $(pgrep -x librepods); do [ "$(readlink -f /proc/$p/exe)" = "$(readlink -f "$BIN")" ] || other+="$p $(tr '\0' ' ' </proc/$p/cmdline | cut -c1-120)"$'\n'; done
  if [ -n "$other" ] || systemctl --user -q is-active librepods 2>/dev/null; then
    echo "another LibrePods is running; stop it first:" >&2; echo "${other:-the librepods user service}" >&2; return 1
  fi
  stop_app
  echo "=== start $(date -u +%FT%TZ) $(head -1 "$STAMP")" >>"$OUT/app.log"
  (cd "$REPO" && exec env RUST_BACKTRACE=1 nix develop -c "$BIN" -d --start-minimized) >>"$OUT/app.log" 2>&1 &
  local m i; m=$(wc -l <"$OUT/app.log")
  for i in $(seq 1 150); do tail -n +"$m" "$OUT/app.log" | grep -q 'Listening for Bluetooth connections' && break; sleep $POLL; done
  tail -n +"$m" "$OUT/app.log" | grep -q 'Listening for Bluetooth connections' || { echo "LibrePods didn't start" >&2; return 1; }
  if ap_connected; then
    for i in $(seq 1 100); do tail -n +"$m" "$OUT/app.log" | grep -q 'AUTO enabled' && break; sleep $POLL; done
  fi
}

stop_app() {
  local p i
  p=$(app_pids); [ -n "$p" ] || return 0
  kill $p 2>/dev/null
  for i in $(seq 1 25); do [ -z "$(app_pids)" ] && return 0; sleep $POLL; done
  p=$(app_pids); [ -n "$p" ] && kill -9 $p 2>/dev/null
  return 0
}

# ---------- marks and evidence since a mark ----------
declare -A M_APP M_BZ M_MAC M_JS M_T
mark() {
  M_APP[$1]=$(wc -l <"$OUT/app.log"); M_BZ[$1]=$(wc -l <"$OUT/bluez.log"); M_MAC[$1]=$(wc -l <"$OUT/mac.log")
  M_JS[$1]=$(wc -l <"$JSONL" 2>/dev/null || echo 0); M_T[$1]=$(now_ms)
}
app_since() { tail -n +"$((M_APP[$1] + 1))" "$OUT/app.log"; }
mac_since() { tail -n +"$((M_MAC[$1] + 1))" "$OUT/mac.log"; }
jsonl_since() { tail -n +"$((M_JS[$1] + 1))" "$JSONL" 2>/dev/null; }
transport_since() { # "<ms> <state>" lines since the mark
  tail -n +"$((M_BZ[$1] + 1))" "$OUT/bluez.log" | sed -n "s/^\([0-9]*\) .*MediaTransport1.*'State': <'\([a-z]*\)'>.*/\1 \2/p"
}
since_ms() { echo $(($(now_ms) - M_T[$1])); }

# wait_for MARK SECONDS CMD...: poll CMD (with the mark's evidence available) until it succeeds.
# Sets WAITED_MS. Returns 2 if an observer dies meanwhile.
wait_for() {
  local m=$1 s=$2; shift 2
  local end=$(($(now_ms) + s * 1000)) why
  while :; do
    "$@" && { WAITED_MS=$(since_ms "$m"); return 0; }
    why=$(observers_ok) || { WHY=$why; return 2; }
    [ "$(now_ms)" -ge "$end" ] && return 1
    sleep $POLL
  done
}
has_app() { app_since "$1" | grep -qE -- "$2"; }
has_seq() { # has_seq MARK re1 re2 ...: the regexes match in this order in the app log since MARK
  local m=$1; shift
  app_since "$m" | awk -v n=$# 'BEGIN { for (i = 1; i <= n; i++) re[i] = ARGV[i]; ARGC = 1; k = 1 }
    k <= n && $0 ~ re[k] { k++ } END { exit !(k > n) }' "$@"
}
has_mac() { mac_since "$1" | grep -qE -- "$2"; }
has_transport() { transport_since "$1" | grep -q " $2\$"; }
player_paused() { [ "$(player_status)" != Playing ]; }
# take MARK CHECK: Linux playback was started after MARK; expect the take and record its ACK time.
expect_take() {
  expect_seq "$1" 5 "$2" 'AUTO playback started|playback restarted' 'ACK owner=linux' || return
}
# released MARK CHECK: Linux playback stopped after MARK; expect the hold-then-release (or a remote yield).
expect_release() { expect_log "$1" 12 "$2" 'ACK owner=not-linux' || return; }

expect_log() { # expect_log MARK SECONDS CHECK REGEX
  local rc; wait_for "$1" "$2" has_app "$1" "$4"; rc=$?
  case $rc in 0) result PASS "$3" "after ${WAITED_MS} ms" ;; 1) result FAIL "$3" "no /$4/ within $2 s" ;; 2) invalid "$WHY"; return 2 ;; esac
}
expect_seq() { # expect_seq MARK SECONDS CHECK RE1 RE2 ...
  local m=$1 s=$2 c=$3 rc; shift 3
  wait_for "$m" "$s" has_seq "$m" "$@"; rc=$?
  case $rc in 0) result PASS "$c" "after ${WAITED_MS} ms" ;; 1) result FAIL "$c" "no ordered [$*] within $s s" ;; 2) invalid "$WHY"; return 2 ;; esac
}
expect_mac() { # expect_mac MARK SECONDS CHECK REGEX
  local rc; wait_for "$1" "$2" has_mac "$1" "$4"; rc=$?
  case $rc in 0) result PASS "$3" "after ${WAITED_MS} ms" ;; 1) result FAIL "$3" "no Mac /$4/ within $2 s" ;; 2) invalid "$WHY"; return 2 ;; esac
}
expect_transport() { # expect_transport MARK SECONDS CHECK STATE
  local rc; wait_for "$1" "$2" has_transport "$1" "$4"; rc=$?
  case $rc in 0) result PASS "$3" "after ${WAITED_MS} ms" ;; 1) result FAIL "$3" "transport never $4 within $2 s" ;; 2) invalid "$WHY"; return 2 ;; esac
}
# hold_quiet MARK SECONDS: watch for SECONDS with the observers alive throughout (for negative checks).
hold_quiet() {
  local end=$(($(now_ms) + $2 * 1000)) why
  while [ "$(now_ms)" -lt "$end" ]; do why=$(observers_ok) || { WHY=$why; return 2; }; sleep 0.5; done
}
expect_no_log() { # expect_no_log MARK SECONDS CHECK REGEX
  hold_quiet "$1" "$2" || { invalid "$WHY"; return 2; }
  if has_app "$1" "$4"; then result FAIL "$3" "$(app_since "$1" | grep -E -- "$4" | head -1 | cut -c1-160)"
  else result PASS "$3" "none in $2 s"; fi
}
expect_no_transport_active() { # expect_no_transport_active MARK SECONDS CHECK
  hold_quiet "$1" "$2" || { invalid "$WHY"; return 2; }
  local a; a=$(transport_since "$1" | awk '$2 == "active" { print $1 }' | head -1)
  if [ -n "$a" ]; then result FAIL "$3" "transport active $((a - M_T[$1])) ms after the mark"
  else result PASS "$3" "no transport start in $2 s"; fi
}
transport_starts() { transport_since "$1" | grep -c ' active$' || true; }
check_eq() { # check_eq CHECK ACTUAL EXPECTED
  if [ "$2" = "$3" ]; then result PASS "$1" "$2"; else result FAIL "$1" "got '$2', want '$3'"; fi
}
ack_ms() { # ack_ms MARK KIND: elapsed_ms of the first ACK of KIND (ack_linux / ack_not_linux) since MARK
  jsonl_since "$1" | jq -r --arg k "$2" 'select(.kind == $k) | .line' 2>/dev/null | sed -n 's/.*elapsed_ms=\([0-9]*\).*/\1/p' | head -1
}

# ask CHECK QUESTION: a human check. y = PASS, n = FAIL (with a note), skipped under --unattended.
ask() {
  if [ -n "$UNATTENDED" ]; then result UNCHECKED "$1" "$2"; return 0; fi
  local a note
  while :; do
    printf '\n>>> %s [y/n] ' "$2" >/dev/tty; read -r a </dev/tty
    case $a in y | Y) result PASS "$1" "mo: yes"; return 0 ;;
      n | N) printf '    what happened? ' >/dev/tty; read -r note </dev/tty; result FAIL "$1" "mo: $note"; return 0 ;; esac
  done
}
# tell MESSAGE: an instruction for the human; waits for Enter (skipped under --unattended).
tell() {
  log "STEP: $1"
  [ -n "$UNATTENDED" ] && return 1
  printf '\n>>> %s\n    press Enter when done ' "$1" >/dev/tty; read -r _ </dev/tty
}

# ---------- Linux side ----------
ap_connected() { bluetoothctl info "$AIRPODS" 2>/dev/null | grep -q 'Connected: yes'; }
airpods_sink() { pactl list sinks short | awk -v id="bluez_output.$AP_ID" 'index($2, id) == 1 { print $2; exit }'; }
card_profile() { pactl list cards | awk -v id="bluez_card.$AP_ID" '$1 == "Name:" { f = ($2 == id) } f && /Active Profile/ { print $3; exit }'; }
default_sink() { pactl get-default-sink; }
player_status() { playerctl -p "$PLAYER" status 2>/dev/null || echo none; }

linux_play() {
  playerctl -p "$PLAYER" play || return 1
  local i; for i in $(seq 1 15); do [ "$(player_status)" = Playing ] && return 0; sleep $POLL; done
  echo "$PLAYER didn't start playing (status: $(player_status))" >&2; return 1
}
linux_pause() { playerctl -p "$PLAYER" pause 2>/dev/null; true; }

# The AirPods' own host list (last Connected Devices report): hosts whose first flag isn't 0.
ap_hosts() { grep 'Received Connected Devices' "$OUT/app.log" | tail -1 | grep -oE 'info1: [0-9]+' | awk '$2 != 0' | wc -l; }

reconnect_peaches() {
  local i
  for i in 1 2 3; do ap_connected && return 0; bluetoothctl connect "$AIRPODS" >/dev/null 2>&1; sleep 3; done
  ap_connected
}

# ensure_hosts: exactly Peaches + the Mac on the AirPods, stable for 5 s.
ensure_hosts() {
  reconnect_peaches || { setup_fail "the AirPods won't connect to Linux (in the case, or out of range?)"; return 3; }
  local m=hosts i; mark $m
  # a fresh connection needs its init reports before the host count means anything
  for i in $(seq 1 75); do [ "$(ap_hosts)" -gt 0 ] && break; sleep $POLL; done
  mac_ap_connected || { setup_fail "connect the AirPods on the Mac (Control Centre > Bluetooth)"; return 3; }
  sleep 5
  ap_connected || { setup_fail "Linux lost the AirPods while settling (evicted?)"; return 3; }
  local n; n=$(ap_hosts)
  [ "$n" -le 2 ] || { setup_fail "$n hosts on the AirPods; turn the iPhone's Bluetooth off (the Mac stands in for it)"; return 3; }
  [ "$n" -eq 2 ] || { setup_fail "the AirPods list $n host(s); want Linux + the Mac"; return 3; }
}

# reset_baseline: nothing plays anywhere, the default output is the DAC, no stream sits on the AirPods sink,
# the app has gone quiet, and both hosts are on the AirPods. Fails the scenario's setup otherwise.
reset_baseline() {
  local p; for p in "${HELPER_PIDS[@]}"; do kill -- "$p" 2>/dev/null; done; HELPER_PIDS=()
  playerctl -a pause 2>/dev/null; mac_pause
  pactl set-default-sink "$DAC_SINK" || { setup_fail "no DAC sink $DAC_SINK"; return 3; }
  local s i; s=$(airpods_sink)
  if [ -n "$s" ]; then
    for i in $(pactl list sink-inputs short | awk -v s="$(pactl list sinks short | awk -v n="$s" '$2 == n { print $1 }')" '$2 == s { print $1 }'); do
      pactl move-sink-input "$i" "$DAC_SINK" 2>/dev/null
    done
  fi
  quiesce 6 40 || { setup_fail "LibrePods or BlueZ didn't go quiet within 40 s"; return 3; }
  ensure_hosts || return 3
  [ "$(player_status)" != Playing ] || { setup_fail "$PLAYER is still playing"; return 3; }
  log "  baseline: card=$(card_profile) default=$(default_sink) hosts=$(ap_hosts) owner=$(owner_now)"
}

# quiesce QUIET MAX: wait until neither the app's handoff lines nor BlueZ changed for QUIET seconds.
quiesce() {
  local quiet=$1 max=$2 start; start=$(now_ms)
  local a b a2 b2 t
  while [ $(($(now_ms) - start)) -lt $((max * 1000)) ]; do
    a=$(grep -c 'Handoff\|A2DP' "$OUT/app.log"); b=$(wc -l <"$OUT/bluez.log"); t=$(now_ms)
    while [ $(($(now_ms) - t)) -lt $((quiet * 1000)) ]; do
      sleep 0.5
      a2=$(grep -c 'Handoff\|A2DP' "$OUT/app.log"); b2=$(wc -l <"$OUT/bluez.log")
      [ "$a2" = "$a" ] && [ "$b2" = "$b" ] || continue 2
    done
    return 0
  done
  return 1
}
owner_now() { grep 'OwnsConnection, value' "$OUT/app.log" | tail -1 | sed -n 's/.*value: \(..\).*/\1/p' | sed 's/01/linux/; s/00/other/'; }

# ---------- the Mac ----------
mac_ssh() { ssh -o BatchMode=yes -o ConnectTimeout=5 "$MAC_SSH" "$@"; }
mac_osa() { mac_ssh "osascript -e 'tell application \"Music\" to $1'"; }
mac_play() {
  mac_osa "play track \"$MAC_TRACK\" of library playlist 1" >/dev/null || return 1
  local i; for i in $(seq 1 15); do [ "$(mac_osa 'get player state')" = playing ] && return 0; sleep $POLL; done
  echo "Music on the Mac didn't start" >&2; return 1
}
mac_pause() { mac_osa pause >/dev/null 2>&1; true; }
mac_ap_connected() {
  mac_ssh 'system_profiler SPBluetoothDataType 2>/dev/null' |
    awk '/^ +Connected:$/ { c = 1; next } /^ +Not Connected:$/ { c = 0 } c && /AirPods/ { f = 1 } END { exit !f }'
}
mac_afplay() { # mac_afplay SECONDS VOLUME FILE: plays in the background (tracked as a helper)
  mac_ssh "afplay -v $2 -t $1 '$3'" &
  HELPER_PIDS+=($!)
}
