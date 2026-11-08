{
  description = "AirPods liberated from Apple's ecosystem";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    crane.url = "github:ipetkov/crane";
    flake-parts.url = "github:hercules-ci/flake-parts";
    flake-compat.url = "https://flakehub.com/f/edolstra/flake-compat/1.tar.gz";
    systems.url = "github:nix-systems/default";
    treefmt-nix.url = "github:numtide/treefmt-nix";
    # Only used to evaluate the Home Manager module in `checks`.
    home-manager = {
      url = "github:nix-community/home-manager";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{
      self,
      crane,
      flake-parts,
      systems,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = import systems;
      imports = [
        inputs.treefmt-nix.flakeModule
      ];

      flake = {
        nixosModules.default = flake-parts.lib.importApply ./nix/nixos-module.nix { inherit self; };
        homeManagerModules.default = flake-parts.lib.importApply ./nix/hm-module.nix { inherit self; };
        homeModules.default = self.homeManagerModules.default;
      };

      perSystem =
        {
          self',
          pkgs,
          lib,
          system,
          ...
        }:
        let
          buildInputs =
            with pkgs;
            [
              dbus
              libpulseaudio
              alsa-lib
              bluez

              # https://github.com/max-privatevoid/iced/blob/master/DEPENDENCIES.md
              expat
              fontconfig
              freetype
              freetype.dev
              libGL
              pkg-config
              xorg.libX11
              xorg.libXcursor
              xorg.libXi
              xorg.libXrandr
              wayland
              libxkbcommon
              vulkan-loader
            ]
            ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
              pkgs.libiconv
            ];

          nativeBuildInputs = with pkgs; [
            pkg-config
            makeWrapper
          ];

          craneLib = crane.mkLib pkgs;
          unfilteredRoot = ./linux-rust/.;
          src = lib.fileset.toSource {
            root = unfilteredRoot;
            fileset = lib.fileset.unions [
              # Default files from crane (Rust and cargo files)
              (craneLib.fileset.commonCargoSources unfilteredRoot)
              (lib.fileset.maybeMissing ./linux-rust/assets/font)
            ];
          };

          commonArgs = {
            inherit buildInputs nativeBuildInputs src;
            strictDeps = true;

            # RUST_BACKTRACE = "1";
          };

          librepods = craneLib.buildPackage (
            commonArgs
            // {
              cargoArtifacts = craneLib.buildDepsOnly commonArgs;

              doCheck = false;

              # Wrap the binary after build to set runtime library path
              postInstall = ''
                wrapProgram $out/bin/librepods \
                  --prefix LD_LIBRARY_PATH : ${lib.makeLibraryPath buildInputs}
              '';

              meta = {
                description = "AirPods liberated from Apple's ecosystem";
                homepage = "https://github.com/kavishdevar/librepods";
                license = pkgs.lib.licenses.gpl3Only;
                maintainers = [ "kavishdevar" ];
                platforms = pkgs.lib.platforms.unix;
                mainProgram = "librepods";
              };
            }
          );

          # Evaluation-only checks for the NixOS and Home Manager modules. Every
          # `lib.assertMsg` below is forced while `nix flake check` evaluates the
          # check derivation, so a regressed option value fails the check
          # without building anything.
          mkEvalCheck =
            name: assertions:
            assert lib.all ({ ok, msg }: lib.assertMsg ok "${name}: ${msg}") assertions;
            pkgs.runCommandLocal name { } "touch $out";

          appleDeviceId = "bluetooth:004C:0000:0000";

          nixosEval =
            extra:
            inputs.nixpkgs.lib.nixosSystem {
              modules = [
                self.nixosModules.default
                {
                  nixpkgs.hostPlatform = system;
                  boot.isContainer = true;
                  system.stateVersion = lib.trivial.release;
                  programs.librepods.enable = true;
                }
                extra
              ];
            };

          nixosOn = (nixosEval { }).config;
          nixosOff = (nixosEval { programs.librepods.appleDeviceId = false; }).config;

          nixosModuleCheck = mkEvalCheck "nixos-module-eval" [
            {
              ok = (nixosOn.hardware.bluetooth.settings.General.DeviceID or null) == appleDeviceId;
              msg = "programs.librepods.enable must set hardware.bluetooth.settings.General.DeviceID to ${appleDeviceId}";
            }
            {
              ok = !(nixosOff.hardware.bluetooth.settings ? General.DeviceID);
              msg = "programs.librepods.appleDeviceId = false must leave General.DeviceID unset";
            }
            {
              ok = lib.elem self'.packages.default nixosOn.environment.systemPackages;
              msg = "programs.librepods.enable must add the librepods package to environment.systemPackages";
            }
          ];

          hmEval =
            extra:
            inputs.home-manager.lib.homeManagerConfiguration {
              inherit pkgs;
              modules = [
                self.homeManagerModules.default
                {
                  home.username = "librepods";
                  home.homeDirectory = "/home/librepods";
                  home.stateVersion = "25.11";
                  services.librepods.enable = true;
                  services.librepods.extraArgs = [ "--no-tray" ];
                }
                extra
              ];
            };

          hmOn = (hmEval { }).config;
          hmOff = (hmEval { services.librepods.wireplumber.dummyAvrcpPlayer = false; }).config;
          hmService = hmOn.systemd.user.services.librepods;
          wireplumberFiles =
            cfg: lib.filter (lib.hasPrefix "wireplumber/") (lib.attrNames cfg.xdg.configFile);
          wireplumberText = lib.concatMapStrings (n: hmOn.xdg.configFile.${n}.text) (wireplumberFiles hmOn);

          hmModuleCheck = mkEvalCheck "hm-module-eval" [
            {
              ok = lib.all (u: lib.elem u hmService.Unit.After) [
                "pipewire.service"
                "wireplumber.service"
              ];
              msg = "systemd.user.services.librepods.Unit.After must contain pipewire.service and wireplumber.service";
            }
            {
              ok = lib.elem "graphical-session.target" hmService.Install.WantedBy;
              msg = "systemd.user.services.librepods.Install.WantedBy must contain graphical-session.target";
            }
            {
              ok = toString hmService.Service.ExecStart == "${lib.getExe self'.packages.default} --no-tray";
              msg = "systemd.user.services.librepods.Service.ExecStart must run <package>/bin/librepods followed by extraArgs";
            }
            {
              ok =
                lib.length (wireplumberFiles hmOn) == 1
                && lib.hasInfix "monitor.bluez.properties" wireplumberText
                && lib.hasInfix "bluez5.dummy-avrcp-player = true" wireplumberText;
              msg = "wireplumber.dummyAvrcpPlayer must install one WirePlumber drop-in setting bluez5.dummy-avrcp-player = true in monitor.bluez.properties";
            }
            {
              ok = wireplumberFiles hmOff == [ ];
              msg = "wireplumber.dummyAvrcpPlayer = false must not generate any wireplumber/ config file";
            }
          ];
        in
        {
          checks = {
            inherit librepods;
            hm-module-eval = hmModuleCheck;
          }
          // lib.optionalAttrs pkgs.stdenv.isLinux {
            nixos-module-eval = nixosModuleCheck;
          };

          packages.default = librepods;
          apps.default = {
            type = "app";
            program = lib.getExe librepods;
          };

          devShells.default = craneLib.devShell {
            name = "librepods-dev";
            checks = self'.checks;

            # NOTE: cargo and rustc are provided by default.
            buildInputs =
              with pkgs;
              [
                rust-analyzer
              ]
              ++ buildInputs;

            LD_LIBRARY_PATH = lib.makeLibraryPath buildInputs;
          };

          treefmt = {
            programs.nixfmt.enable = pkgs.lib.meta.availableOn pkgs.stdenv.buildPlatform pkgs.nixfmt-rfc-style.compiler;
            programs.nixfmt.package = pkgs.nixfmt-rfc-style;
          };
        };
    };
}
