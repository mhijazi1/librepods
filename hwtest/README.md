# Hardware regression harness

`hwtest/` replays the hardware checks from the M1 spikes against real devices and reports, scenario by scenario, whether LibrePods still behaves.
- **Devices:** the Linux host, a Mac reached over SSH, and the AirPods.
- **What it does:** it drives playback on both computers, watches LibrePods' log, BlueZ's transport state and the Mac's routing daemon, and checks what happened.
- **What it doesn't replace:** the human checks (ears, cut-outs) stay human. The harness asks for them, or marks them UNCHECKED with `--unattended`.

The Mac stands in for the iPhone, because both sides of every switch can then be scripted with exact timestamps. Apple devices use the same arbitration. Tests that need the iPhone itself (calls, its own UI) stay manual spikes.

## Setup (once)

1. `cp hwtest/hwtest.env.example hwtest/hwtest.env` and fill it in. It's gitignored; never commit device addresses.
2. Set up SSH key login to the Mac as the user logged in to its desktop. The first `osascript` call shows a "control Music" prompt on the Mac; allow it.
3. `hwtest/run.sh mac-setup` puts a 3-minute spoken test track into Music on the Mac.
4. Keep a music tab (or track) ready in `$PLAYER` on Linux.

## Before each run

- **Build:** `hwtest/run.sh build`. The harness only runs a binary it built and stamped from the current `linux-rust/` and flake, and it refuses a stale one.
- **Stop other LibrePods instances:** the service, or a dev build. The harness starts its own instance.
- **Exactly two hosts:** connect the AirPods to the Mac (Control Centre > Bluetooth) and turn the iPhone's Bluetooth off. The AirPods hold at most three hosts and evict one on the fourth connection, so a third host makes results meaningless. Setup checks this.
- **Wear the buds** (both in).

## Running

```
hwtest/run.sh list                         # scenarios and the issues they cover
hwtest/run.sh all                          # everything, asking the human checks on the terminal
hwtest/run.sh switch_loop --rounds 5       # one scenario
hwtest/run.sh all --unattended             # log checks only; human checks become UNCHECKED
```

Each run writes `hwtest/out/<date-time>/`:
- `report.md`: verdicts per scenario and per check, ready to paste into an issue; addresses are scrubbed
- `app.log`: LibrePods' own log
- `bluez.log`: BlueZ property changes with millisecond timestamps
- `mac.log`: the Mac's `audioaccessoryd`
- `results.tsv`
- `run.log`

## Verdicts

| Check | Meaning |
|---|---|
| PASS / FAIL | The check held, or it didn't |
| XFAIL | Failed, but an open issue says it will until fixed (the issue is named) |
| XPASS | A known-issue check passed: verify the issue and close it, then drop the `xfail` |
| UNCHECKED | A human check skipped with `--unattended` |
| SETUP | A precondition wasn't met (wrong hosts, a player that won't play, …), so the scenario didn't run |
| INVALID | An observer (BlueZ monitor, Mac log, the app) died mid-scenario, so its silence isn't evidence |

A scenario passes only if every check passes or XFAILs. One with UNCHECKED checks is **INCOMPLETE**, never PASS.

## How the checks avoid false passes

- **Fresh baseline:** each scenario starts from `reset_baseline`.
  - Nothing plays anywhere, and the default output is the DAC.
  - Streams are moved off the AirPods sink.
  - LibrePods and BlueZ have been quiet for 6 s.
  - Both hosts have stayed on the AirPods for 5 s.
  - If any of this fails, the scenario is SETUP, not PASS.
- **Marks:** checks only read evidence logged after a mark taken right before the action. Ordered expectations use `expect_seq`.
- **Negative checks fail closed:** "no re-acquire for 10 s" re-checks the observers throughout and turns INVALID if one dies.
- **Transport states** come from BlueZ's `PropertiesChanged` signals (`gdbus monitor`), not polling, so short restarts are caught.

## Adding a scenario

1. Write `scenario_<name>` in `scenarios.sh`, starting with `reset_baseline || return`.
2. Use the `expect_*`, `check_eq` and `ask` helpers from `lib.sh`.
3. Put `xfail <issue>` right before a check that an open issue is expected to break.
4. Add the scenario to `SCENARIOS` (keep destructive ones last) and to `DESC`, naming the issues it covers.

When a hardware spike's question becomes a repeatable check, add it here and link the scenario from the spike. M1's exit criterion is `hwtest/run.sh all` with every scenario PASS or XFAIL.
