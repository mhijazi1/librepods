# Tools the hardware harness needs on the Linux host (LibrePods itself runs through the repo's flake dev shell).
{ pkgs ? import <nixpkgs> { } }:
pkgs.mkShell {
  packages = with pkgs; [
    bash
    coreutils
    gnugrep
    gawk
    gnused
    jq
    glib # gdbus monitor (BlueZ PropertiesChanged, unprivileged)
    bluez # bluetoothctl
    pulseaudio # pactl, pacat (PipeWire's pulse server)
    pipewire # pw-play
    playerctl
    mpv
    openssh
    procps
  ];
}
