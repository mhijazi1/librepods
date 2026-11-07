# Project
Owned by the human (mo). The swarm planner reads this before every run.

## End goal
On a Linux desktop (reference machine: NixOS 26.05, BlueZ 5.86, PipeWire 1.6.6 + WirePlumber, Hyprland), AirPods behave like they do
on a Mac in a household with an iPhone and a Mac on the same Apple Account:
- automatic switching both ways between Linux and the Apple devices (Apple "smart routing": ownership + Hijackv2 over AACP),
  driven by real local activity (playback, calls, idle/lock), without stealing audio on non-events;
- "Moved to …" style notifications with Undo, calls routed to the AirPods with the mic (HFP) and back to A2DP afterwards;
- ear detection pause/resume, battery (left/right/case) in the desktop, case-open popup;
- a one-line NixOS / Home Manager install (package, user service, BlueZ `DeviceID = bluetooth:004C:0000:0000`).
Background research (mo's vault): Research/Headphones/AirPods Automatic Switching.md and AirPods Switching on Linux.md.

## Current milestone
Linux parity v1: the issues labelled `ready` in mhijazi1/librepods, built on branch `linux-parity`.

## Constraints
- **This is a fork. All issues, PRs and merges go to `mhijazi1/librepods` only, with base `linux-parity`. Never open, comment on or
  reference-write anything in `librepods-org/librepods` (upstream).** Upstream PR numbers (#688, #689, #724, #766, #775, #841) refer to
  upstream; their heads are available locally as branches `pr-<n>` in the main clone, or via
  `git fetch https://github.com/librepods-org/librepods.git pull/<n>/head`.
- Code lives in `linux-rust/` (Rust 2024, tokio, bluer, dbus, iced, libpulse-binding, ksni). Build and test only through the check
  command in `.swarm/config.json` (Nix dev shell). Keep `Cargo.lock` consistent (`--locked`); add a dependency only when a ticket needs it
  and say why in the PR.
- **Agents have no AirPods or Bluetooth hardware.** Acceptance criteria must be provable offline: unit tests of pure packet/state logic,
  tests against mocked D-Bus / PipeWire / AACP event streams, emitted-command assertions, `nix flake check` evaluation. Real-device
  behaviour goes in an issue/PR section titled **"Hardware validation by mo; not acceptance"** and must never be claimed as done.
- Prefer extracting pure, testable logic (parsing, encoding, policy decisions) from I/O code over adding large mocks.
- Don't break the existing app: the tray, the window UI, Nothing devices and the non-handoff path must still build and behave as before
  unless a ticket says otherwise.
- `cargo fmt --check` fails on upstream code; don't reformat unrelated files. Format only the code you touch.
- Licence: GPL-3.0 (AGPL per README); keep upstream copyright headers.

## Non-goals
Siri, iCloud/MagicPairing key sync, Find My, AirPods firmware updates, Spatial Audio / head tracking, hearing-aid and hearing-test
features, Android/Windows code, upstreaming (mo may upstream later by hand).
