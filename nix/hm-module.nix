# Home Manager module for LibrePods. Exposed as `homeManagerModules.default`
# by flake.nix, which applies the `{ self }` argument via flake-parts' importApply.
{ self }:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.librepods;
in
{
  options.services.librepods = {
    enable = lib.mkEnableOption "the LibrePods user service, started with the graphical session";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      defaultText = lib.literalExpression "librepods.packages.\${pkgs.stdenv.hostPlatform.system}.default";
      description = "The LibrePods package to install and run.";
    };

    extraArgs = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      example = [
        "--no-tray"
        "--start-minimized"
      ];
      description = "Extra command-line arguments passed to {command}`librepods`.";
    };

    wireplumber.dummyAvrcpPlayer = lib.mkOption {
      type = lib.types.bool;
      default = true;
      example = false;
      description = ''
        Whether to install a WirePlumber drop-in
        (`wireplumber/wireplumber.conf.d/51-librepods-avrcp.conf`) that sets
        `bluez5.dummy-avrcp-player = true` in `monitor.bluez.properties`, so
        WirePlumber registers a dummy AVRCP player with BlueZ. Disable this if
        you run {command}`mpris-proxy` or manage this setting yourself.
      '';
    };
  };

  config = lib.mkIf cfg.enable (
    lib.mkMerge [
      {
        assertions = [
          (lib.hm.assertions.assertPlatform "services.librepods" pkgs lib.platforms.linux)
        ];

        home.packages = [ cfg.package ];

        systemd.user.services.librepods = {
          Unit = {
            Description = "LibrePods AirPods client";
            Documentation = [ "https://github.com/kavishdevar/librepods" ];
            PartOf = [ "graphical-session.target" ];
            After = [
              "graphical-session.target"
              "pipewire.service"
              "wireplumber.service"
            ];
          };

          Service = {
            ExecStart = lib.escapeShellArgs ([ (lib.getExe cfg.package) ] ++ cfg.extraArgs);
            Restart = "on-failure";
            RestartSec = 5;
          };

          Install.WantedBy = [ "graphical-session.target" ];
        };
      }

      (lib.mkIf cfg.wireplumber.dummyAvrcpPlayer {
        xdg.configFile."wireplumber/wireplumber.conf.d/51-librepods-avrcp.conf".text = ''
          monitor.bluez.properties = {
            bluez5.dummy-avrcp-player = true
          }
        '';
      })
    ]
  );
}
