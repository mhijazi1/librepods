# NixOS module for LibrePods. Exposed as `nixosModules.default` by flake.nix,
# which applies the `{ self }` argument via flake-parts' importApply.
{ self }:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.programs.librepods;
in
{
  options.programs.librepods = {
    enable = lib.mkEnableOption "LibrePods, a client for AirPods features on Linux";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      defaultText = lib.literalExpression "librepods.packages.\${pkgs.stdenv.hostPlatform.system}.default";
      description = "The LibrePods package to install into {option}`environment.systemPackages`.";
    };

    appleDeviceId = lib.mkOption {
      type = lib.types.bool;
      default = true;
      example = false;
      description = ''
        Whether to set the BlueZ Device ID to Apple's vendor ID
        (`hardware.bluetooth.settings.General.DeviceID = "bluetooth:004C:0000:0000"`).
        AirPods only enable some features (for example multi-device
        connectivity and hearing aid settings) when the host reports an Apple
        Device ID.

        Trade-off: the Device ID is a property of the Bluetooth adapter, so the
        adapter identifies as an Apple device to all devices it talks to, not
        just AirPods. Other peripherals may change their behaviour as a result.
        AirPods must be re-paired after this value changes.
      '';
    };
  };

  config = lib.mkIf cfg.enable (
    lib.mkMerge [
      { environment.systemPackages = [ cfg.package ]; }
      (lib.mkIf cfg.appleDeviceId {
        hardware.bluetooth.settings.General.DeviceID = lib.mkDefault "bluetooth:004C:0000:0000";
      })
    ]
  );
}
