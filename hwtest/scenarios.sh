# hwtest/scenarios.sh: the hardware scenarios. Each scenario_<name> starts from reset_baseline and returns
# 0 when it ran (its checks carry the verdicts), 2 if an observer died (INVALID), 3 if a precondition failed.
# Order matters for `all`: destructive scenarios (app restarts) run last.
SCENARIOS=(mac_categories take_from_idle switch_loop hold_stray_stream reconnect_take lingering_source long_stream probe_baseline crash_leftovers)

declare -A DESC=(
  [mac_categories]="The Mac still sends audio categories 201 (afplay) and 301 (Music) and relays 100 on pause (macOS drift check; #257, #272)"
  [take_from_idle]="Linux play takes the AirPods from an idle Mac: one transport start, both ears, default sink follows (#207, #267, #161)"
  [switch_loop]="N rounds Linux play -> Mac play: ACK timings, player paused and DAC default on yield, card held off, no re-acquire (#193, #161, #184, #264)"
  [hold_stray_stream]="A stray uncorked stream and a notification sound can't pull the AirPods back while the Mac plays; a take releases the hold (#264, #265)"
  [reconnect_take]="N forced reconnects: handshake reply then host state, then a take works (#286, #291)"
  [lingering_source]="Linux play 5 s after the Mac pauses takes once the Mac's audio source clears (#280; expected to fail until fixed)"
  [long_stream]="With a long-lived non-MPRIS stream playing, an MPRIS play still takes back from a playing Mac (#96, #170, #272; expected to fail until fixed)"
  [probe_baseline]="Probe: a stream started 0.8 s before 'auto' doesn't take; a later start does (#90, #171)"
  [crash_leftovers]="SIGTERM while held restores the card (#294, expected to fail until fixed); SIGKILL leaves it off until restart clears it (#265 q4)"
)

scenario_mac_categories() {
  reset_baseline || return
  mark a; mac_afplay 8 0.3 "Music/$MAC_TRACK.m4a"
  expect_mac a 5 "afplay (no Now Playing) is category 201" 'Updating local audio category [0-9]+ -> 201' || return
  sleep 9
  mark b; mac_play || { setup_fail "Music won't play"; return; }
  expect_mac b 5 "Music (Now Playing) is category 301" 'Updating local audio category [0-9]+ -> 301' || return
  sleep 3
  mark c; mac_pause
  expect_mac c 3 "a pause relays 100 at once" 'Updating local audio category 301 -> 100' || return
}

scenario_take_from_idle() {
  reset_baseline || return
  mark t; linux_play || { setup_fail "$PLAYER won't play"; return; }
  expect_take t "Linux takes on play" || return
  result PASS "take ACK time" "$(ack_ms t ack_linux) ms"
  expect_transport t 6 "A2DP transport active" active || return
  sleep 4
  check_eq "one transport start, no restart (#267)" "$(transport_starts t)" 1
  check_eq "default sink follows to the AirPods (#40)" "$(default_sink)" "$(airpods_sink)"
  ask "both ears" "Is $PLAYER playing in BOTH AirPods (not left only)?"
  mark r; linux_pause
  expect_release r "release after Linux stops" || return
}

scenario_switch_loop() {
  reset_baseline || return
  local i takes=() yields=()
  for i in $(seq 1 "$ROUNDS"); do
    mark a; linux_play || { setup_fail "$PLAYER won't play (round $i)"; return; }
    expect_take a "round $i: Linux takes" || return
    takes+=("$(ack_ms a ack_linux)")
    sleep 6
    mark b; mac_play || { setup_fail "Music won't play (round $i)"; return; }
    expect_seq b 5 "round $i: Linux yields to the Mac" 'REMOTE requested ownership; yielding' 'ACK owner=not-linux' || return
    yields+=("$(ack_ms b ack_not_linux)")
    wait_for b 3 player_paused; check_eq "round $i: $PLAYER paused on yield" "$(player_status)" Paused
    check_eq "round $i: default sink back on the DAC" "$(default_sink)" "$DAC_SINK"
    expect_log b 4 "round $i: card held off" 'A2DP hold\(true\)' || return
    expect_no_transport_active b 10 "round $i: no A2DP re-acquire while the Mac plays (#264)" || return
    mac_pause
    sleep 12 # the AirPods' audio source lingers ~8 s after a pause (#280)
  done
  result PASS "ACK timings (ms)" "takes ${takes[*]}; yields ${yields[*]}"
  observers_ok >/dev/null && result PASS "LibrePods survived $((ROUNDS * 2)) switches (#193)" "" || result FAIL "LibrePods survived the loop (#193)" "$(observers_ok)"
  ask "clean switches" "Did every round switch cleanly (both ears, Mac uninterrupted, nothing from the speakers)?"
}

scenario_hold_stray_stream() {
  reset_baseline || return
  mark a; linux_play || { setup_fail "$PLAYER won't play"; return; }
  expect_take a "Linux takes" || return
  sleep 3
  local s; s=$(airpods_sink)
  [ -n "$s" ] || { setup_fail "no AirPods sink after the take"; return; }
  pacat --raw -d "$s" </dev/zero & HELPER_PIDS+=($!) # uncorked silence forever, like a stuck stream
  sleep 1
  mark r; linux_pause
  expect_release r "Linux releases after stopping" || return
  mark b; mac_play || { setup_fail "Music won't play"; return; }
  expect_log b 5 "card held off when the Mac takes" 'A2DP hold\(true\)' || return
  sleep 2
  check_eq "card profile while held" "$(card_profile)" off
  pw-play /run/current-system/sw/share/sounds/freedesktop/stereo/message.oga 2>/dev/null & HELPER_PIDS+=($!)
  expect_no_transport_active b 15 "no A2DP re-acquire with a stray stream and a notification (#264)" || return
  ask "Mac uninterrupted" "Did the Mac's audio keep playing in the AirPods with no cut-out?"
  mac_pause
  mark c; linux_play || { setup_fail "$PLAYER won't play"; return; }
  expect_log c 6 "a take releases the hold (#265 q3)" 'A2DP hold\(false\)' || return
  expect_take c "Linux takes back" || return
  expect_transport c 8 "A2DP transport active again" active || return
  linux_pause
}

scenario_reconnect_take() {
  reset_baseline || return
  local i
  for i in $(seq 1 "$ROUNDS"); do
    mark a
    bluetoothctl disconnect "$AIRPODS" >/dev/null 2>&1; sleep 3; bluetoothctl connect "$AIRPODS" >/dev/null 2>&1
    expect_seq a 15 "reconnect $i: handshake reply, then host state (#286)" 'Received AACP handshake reply' 'Received Connected Devices' || return
    expect_log a 15 "reconnect $i: ear state arrives" 'Received Ear Detection Status' || return
    sleep 10
    mark b; linux_play || { setup_fail "$PLAYER won't play (reconnect $i)"; return; }
    expect_take b "reconnect $i: Linux takes" || return
    if has_app a 'missing host state'; then result FAIL "reconnect $i: no 'missing host state' block" "blocked"
    else result PASS "reconnect $i: no 'missing host state' block" ""; fi
    mark r; linux_pause
    expect_release r "reconnect $i: release after stopping" || return
  done
}

scenario_lingering_source() {
  reset_baseline || return
  mac_play || { setup_fail "Music won't play"; return; }
  sleep 6; mac_pause; sleep 5
  mark b; linux_play || { setup_fail "$PLAYER won't play"; return; }
  xfail 280
  expect_log b 12 "Linux takes 5 s after the Mac paused" 'ACK owner=linux' || return
  linux_pause
}

scenario_long_stream() {
  reset_baseline || return
  mark a
  mpv --no-config --no-video --really-quiet --loop-file=inf --volume=20 'av://lavfi:sine=frequency=330:sample_rate=48000' &
  HELPER_PIDS+=($!)
  expect_take a "a non-MPRIS stream takes" || return
  sleep 3
  mark b; mac_play || { setup_fail "Music won't play"; return; }
  expect_log b 5 "the Mac takes" 'REMOTE requested ownership' || return
  sleep 3
  mark c; linux_play || { setup_fail "$PLAYER won't play"; return; }
  xfail 96
  expect_log c 8 "an MPRIS play takes back while the stream keeps playing" 'ACK owner=linux' || return
  linux_pause; mac_pause
}

scenario_probe_baseline() {
  reset_baseline || return
  mac_play || { setup_fail "Music won't play"; return; }
  sleep 4; mac_pause; sleep 2
  stop_app
  : >"$OUT/probe.cmds"
  echo "=== probe $(date -u +%FT%TZ)" >>"$OUT/app.log"
  (cd "$REPO" && exec setsid bash -c 'tail -n0 -f "$1" | nix develop -c "$2" -d --handoff-probe "$3"' _ "$OUT/probe.cmds" "$BIN" "$AIRPODS") >>"$OUT/app.log" 2>&1 &
  HELPER_PIDS+=(-$!)
  mark p
  wait_for p 30 has_app p 'READY commands' || { setup_fail "the probe didn't start"; start_app; return; }
  sleep 8
  mark a; linux_play || { setup_fail "$PLAYER won't play"; return; }
  sleep 0.8; echo auto >>"$OUT/probe.cmds"
  expect_log a 5 "auto enabled" 'AUTO enabled' || return
  mark b
  expect_no_log b 20 "a stream started 0.8 s before auto doesn't take (#90)" 'AUTO playback started' || return
  linux_pause; sleep 6
  mark c; linux_play || { setup_fail "$PLAYER won't play"; return; }
  expect_log c 6 "a later start takes" 'AUTO playback started' || return
  linux_pause; sleep 1
  echo quit >>"$OUT/probe.cmds"; sleep 3
  start_app || { invalid "LibrePods didn't restart after the probe"; return 2; }
}

scenario_crash_leftovers() {
  local sig
  for sig in TERM KILL; do
    reset_baseline || return
    mark a; linux_play || { setup_fail "$PLAYER won't play"; return; }
    expect_take a "SIG$sig: Linux takes" || return
    sleep 3
    mark b; mac_play || { setup_fail "Music won't play"; return; }
    expect_log b 5 "SIG$sig: card held off" 'A2DP hold\(true\)' || return
    sleep 2; mac_pause
    kill -s "$sig" $(app_pids) 2>/dev/null
    local i; for i in $(seq 1 25); do [ -z "$(app_pids)" ] && break; sleep $POLL; done
    sleep 1
    if [ "$sig" = TERM ]; then
      xfail 294; check_eq "SIGTERM restores the card before exiting" "$(card_profile)" a2dp-sink
    else
      result PASS "SIGKILL leaves the card $(card_profile) (nothing can run; restart must clear it)" ""
    fi
    start_app || { invalid "LibrePods didn't restart"; return 2; }
    sleep 3
    check_eq "SIG$sig: restart restores the card" "$(card_profile)" a2dp-sink
    mark c; linux_play || { setup_fail "$PLAYER won't play"; return; }
    expect_take c "SIG$sig: a take works after the restart" || return
    linux_pause
  done
}
