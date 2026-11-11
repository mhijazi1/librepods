> [!WARNING]
> librepods.org is not an official website of the LibrePods project. It inaccurately claims to be the official website of the project by claiming copyrights and using the LibrePods logo in the footer. And at the same time, they say that the project is not affiliated with the LibrePods project or its developers.
> 
> Please report any other such websites to [me@kavish.xyz](mailto:me@kavish.xyz)
 
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="./imgs/banner-dark.png" />
  <source media="(prefers-color-scheme: light)" srcset="./imgs/banner.png" />
  <img alt="LibrePods" src="./imgs/banner.png" />
</picture>

<div align="center" style="margin: 20px 0px;">
<a href="https://github.com/kavishdevar/librepods/releases/latest">
  <img src="https://img.shields.io/github/downloads/kavishdevar/librepods/total?label=GitHub%20Downloads" />
</a>
<a href="https://github.com/kavishdevar/librepods/actions/workflows/ci-android.yml">
  <img src="https://github.com/kavishdevar/librepods/actions/workflows/ci-android.yml/badge.svg" />
</a>
<a href="https://github.com/kavishdevar/librepods/actions/workflows/ci-linux-rust.yml">
  <img src="https://github.com/kavishdevar/librepods/actions/workflows/ci-linux-rust.yml/badge.svg" />
</a>
<a href="https://github.com/kavishdevar/librepods/issues">
  <img src="https://img.shields.io/github/issues/kavishdevar/librepods" />  
</a>
<a href="https://discord.gg/HhG4ycVum4">
  <img src="https://img.shields.io/discord/1441416992027574375?logoColor=white&color=5865F2&label=Discord" />
</a>
</div>

# What is LibrePods?

LibrePods allows you to use AirPods features that are exclusive to Apple devices. It implements the proprietary protocol used to exchange data between AirPods and Apple devices, enabling features like changing noise control modes, fast ear detection, accurate battery status, head gestures, conversational awareness, and more on non-Apple platforms.

# Feature availability

| Feature                                                     | Linux | Android |
| ----------------------------------------------------------- | ----- | ------- |
| Changing Listening Mode                                     | ✅     | ✅       |
| Ear detection                                               | ✅     | ✅       |
| Battery status                                              | ✅     | ✅       |
| Renaming AirPods <details><summary>Note for Android</summary>On Android, you need to re-pair your AirPods after renaming them because Android might not use the latest name.</details>                                            | ✅     | ✅       |
| Loud Sound Reduction                                        | 🔴     | ⚪       |
| Head Gestures                                               | ⛔     | ✅       |
| Conversational Awareness                                    | ✅     | ✅       |
| Automatically connect to AirPods                            | ✅     | ✅       |
| Hearing Aid                                                 | 🔴     | ⚪       |
| Transparency Mode customization                             | 🔴     | ⚪       |
| Multi-device connectivity (Bluetooth Multipoint; 2 devices only) | ⚪     | ⚪       |
| <details><summary>Other accessibility configs (click to expand)</summary><ul><li>Press speed</li><li>Press and Hold duration</li><li>Noise Cancellation with single AirPod</li><li>Volume control on swipe</li><li>Volume swipe speed</li></ul></details>       | 🔴     | ✅       |
| <details><summary>Other general configs</summary><ul><li>Press and Hold to cycle between listening modes/invoke digital assistant (invoking digital assistant needs a recent firmware)</li><li>Configure call controls</li><li>Personalized volume</li><li>Loud Sound Reduction (needs <a href="#vendorid-spoofing">VendorID spoofing</a>)</li><li>Microphone side</li><li>Pause media when falling asleep (needs a recent firmware)</li><li>Enable <code>Off listening mode</code> to switch to <code>Off</code></li></ul></details>                   | 🔴     | ✅       |
| [Head-tracked Spatial Audio](#spatial-audio)                | ❓     | ❓       |
| [Heart Rate Monitoring](#heart-rate-monitoring)             | 🔴     | 🟡       |
| [Find My](#find-my)                                         | ❓     | ❓       |
| [High quality two-way audio](#high-quality-two-way-audio)   | 🟡     | 🟡       |

| Symbol | Meaning                                                             |
| ------ | ------------------------------------------------------------------- |
| ✅     | Implemented and works well                                          |
| ⚪     | Needs [VendorID spoofing](#vendorid-spoofing); use at your own risk |
| 🟡     | Experimental/Work in progress                                       |
| 🔴     | Not implemented yet; planned                                        |
| ⛔     | Will not be implemented                                             |
| ❓     | Unknown                                                             |

## Find My

The following features related to Find My are planned, but require further RE and might need root on Android:

- Add your AirPods to the Find My network
- Play sound through charging case to find it
- Notify when leaving behind
- Toggle case charging sounds

## Spatial Audio

The app does not currently provide head tracking information to Android for the OS to perform HRTF. This has not been explored completely, and it might need root. 

Spatializing stereo sound is beyond this project's scope and will never be available. Many OEMs have an implementation of their own for this.

## Heart Rate Monitoring

#### Automatic switching

Linux takes the AirPods from an iPhone or Mac the way macOS does, by audio
category ([#272](https://github.com/mhijazi1/librepods/issues/272)). A player
that reports playback over MPRIS (most browsers, Spotify, mpv with an MPRIS
plugin, ...) takes them from an idle owner or from one playing media. A player
without MPRIS (games, plain mpv, many Electron apps) takes them from an idle
owner, but not from one playing media: it plays on the next sink instead.
Neither takes them from an owner in a call.

#### Desktop battery (BlueZ BatteryProvider)

LibrePods exports the AirPods battery to BlueZ as a battery provider, so it
shows up in BlueZ's `org.bluez.Battery1`, in UPower (`upower -d`) and in
desktop bars and battery widgets. BlueZ holds one percentage per device, so
LibrePods exports a single value:

- both buds worn (or both in the case): the lower bud;
- one bud charging in the case: the bud you are wearing;
- only one bud connected: that bud;
- single-battery devices (headphones): the headset level.

The case level is not exported. The per-bud and case levels stay in the tray
and the LibrePods window.

Depending on the BlueZ version, the battery provider API
(`org.bluez.BatteryProviderManager1`) may only be available with experimental
features on. If the desktop shows no AirPods battery, set
`Experimental = true` in the `[General]` section of `/etc/bluetooth/main.conf`
(NixOS: `hardware.bluetooth.settings.General.Experimental = true;`) and restart
Bluetooth. Without it, LibrePods logs one warning and carries on without the
export.

The LE features (auto-connect when the AirPods leave the case, the case-open
popup, battery from advertisements) need `org.bluez.AdvertisementMonitorManager1`,
which BlueZ also marks experimental. Turn it on the same way: `Experimental = true`
in the `[General]` section of `/etc/bluetooth/main.conf` (NixOS:
`hardware.bluetooth.settings.General.Experimental = true;`), then restart
Bluetooth. Without it, LibrePods logs one warning and runs without the LE
features.

PipeWire's bluez5 plugin also registers a battery with BlueZ for HFP headsets,
from the headset's own battery reports. BlueZ keeps the first battery provided
for a device and ignores later ones, so the desktop may show PipeWire's HFP
level instead of LibrePods' value. To check which one BlueZ shows, read the
`Source` of the AirPods' `org.bluez.Battery1` (use your adapter and the AirPods'
address):

```sh
busctl get-property org.bluez /org/bluez/hci0/dev_XX_XX_XX_XX_XX_XX org.bluez.Battery1 Source
```

`s "LibrePods"` means LibrePods' value is shown. When LibrePods sees another
`Source` there (or none), it logs one info line per connection saying that its
level is not shown.

To turn the export off, add `"exportBattery": false` at the top level of
`~/.config/librepods/preferences.json`:

```json
{
  "exportBattery": false
}
```

#### NixOS and Home Manager

The flake provides a NixOS module (`nixosModules.default`) and a Home Manager
module (`homeManagerModules.default`, also exported as `homeModules.default`).
Add the flake as an input:

```nix
{
  inputs.librepods.url = "github:kavishdevar/librepods";
  # Optional: reuse your own nixpkgs.
  # inputs.librepods.inputs.nixpkgs.follows = "nixpkgs";
}
```

NixOS configuration (installs the package and sets the BlueZ Device ID):

```nix
{ inputs, ... }:
{
  imports = [ inputs.librepods.nixosModules.default ];
  programs.librepods.enable = true;
}
```

Home Manager configuration (runs LibrePods as a systemd user service with the
graphical session):

```nix
{ inputs, ... }:
{
  imports = [ inputs.librepods.homeManagerModules.default ];
  services.librepods.enable = true;
}
```

NixOS options (`programs.librepods`):

| Option          | Default                     | Description                                                                                               |
| --------------- | --------------------------- | --------------------------------------------------------------------------------------------------------- |
| `enable`        | `false`                     | Install LibrePods and apply the settings below.                                                           |
| `package`       | this flake's `librepods`    | Package added to `environment.systemPackages`.                                                            |
| `appleDeviceId` | `true`                      | Set `hardware.bluetooth.settings.General.DeviceID = "bluetooth:004C:0000:0000"` (Apple's vendor ID).      |

Home Manager options (`services.librepods`):

| Option                          | Default                  | Description                                                                                                                   |
| ------------------------------- | ------------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `enable`                        | `false`                  | Install LibrePods and start it as a user service with `graphical-session.target`, after `pipewire.service` and `wireplumber.service`. |
| `package`                       | this flake's `librepods` | Package to install and run.                                                                                                   |
| `extraArgs`                     | `[ ]`                    | Extra arguments for `librepods`, for example `[ "--no-tray" ]` or `[ "--start-minimized" ]`.                                  |
| `wireplumber.dummyAvrcpPlayer`  | `true`                   | Add a WirePlumber drop-in that sets `bluez5.dummy-avrcp-player = true` in `monitor.bluez.properties`.                         |

About `appleDeviceId`: AirPods only enable some features (such as multi-device
connectivity and the hearing aid settings) when the host reports Apple's Device
ID. The DeviceID is a property of the Bluetooth adapter, so with this option the
adapter identifies as an Apple device to every device it talks to, not only to
your AirPods. Set `programs.librepods.appleDeviceId = false;` if you don't want
that. After changing the DeviceID (in either direction), rebuild, restart
Bluetooth, then remove and re-pair your AirPods so they pick up the new
identity.

> [!NOTE]
> These modules are checked by `nix flake check` at evaluation level only; they
> have not yet been verified on real hardware.

### Android

Available in the `android/rewrite` branch. [builds](https://github.com/librepods-org/librepods/actions/workflows/ci-android.yml?query=branch%3Aandroid%2Frewrite)

### Linux

Not available yet (doesn't seem like something that would be useful on a computer anyway. If you have any suggestions, create a discussion.)

## High quality two-way audio
On iOS/iPadOS, you can continue using A2DP while AirPods send the hi-res (AAC-ELD mono 64kHz?) audio stream from its microphone over AACP.

### Android
Record audio within the app using the AirPods' microphone without switching to Headset/Handsfree profile degrading the playback quality. But, making this available to other apps/telephony might need root, if possible at all.

Recorder is work in progress; available in the `android/rewrite` branch. [builds](https://github.com/librepods-org/librepods/actions/workflows/ci-android.yml?query=branch%3Aandroid%2Frewrite)

### Linux

Check out [PR #655](https://github.com/librepods-org/librepods/pull/655).

# Installation

- [**Android**](/android/README.md)
- [**Linux**](/linux/README.md)

# VendorID Spoofing

Turns out, if you change the VendorID in DID Profile to that of Apple, you get access to several special features!

You can do this on Linux by editing the DeviceID in `/etc/bluetooth/main.conf`. Add this line to the config file `DeviceID = bluetooth:004C:0000:0000`. For android you can enable the `act as Apple device` setting in the app's settings (shown only when Xposed is available and LibrePods module is enabled).

## Multi-device Connectivity

Upto two devices can be simultaneously connected to AirPods, for audio and control both. Seamless connection switching. The same notification shows up on Apple device when Android takes over the AirPods as if it were an Apple device ("Move to iPhone"). Android also shows a popup when the other device takes over.

## Accessibility Settings and Hearing Aid

Accessibility settings like customizing transparency mode (amplification, balance, tone, conversation boost, and ambient noise reduction), and loud sound reduction can be configured.

All hearing aid customizations can be done from Android (linux soon), including setting the audiogram result. The app doesn't provide a way to take a hearing test because it requires much more precision. It is much better to use an already available audiogram result. 

# Protocol and Reverse Engineering

Please refer to the Wireshark dissector plugin by Nojus ([@pabloaul](https://github.com/pabloaul)) for more information on the protocols used: [pabloaul/apple-wireshark](https://github.com/pabloaul/apple-wireshark)

The dissector had not been used in LibrePods for most of the implementation; I had reverse engineered the protocol myself before this dissector was made. But many (future) features including two-way high quality audio and spatial audio would not have been possible without their RE efforts!

# Use of AI

## Android app

These parts of the app were completely AI-generated: 
- Head Gestures - all of it, including logic and the UI
- The offset setup with r2+the xposed module (both versions)
- Troubleshooter and LogCollector

Rest everything- the background service, the Bluetooth manager classes (AACP and ATT), the entire UI, even the smallest components were written manually.

Some parts of the UI components were borrowed from [Kyant0's demo app](https://github.com/Kyant0/AndroidLiquidGlass/tree/master/catalog), which is licensed under [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).

## Linux (rewrite)

The `aacp.rs` and the `att.rs` files were translated from Kotlin to Rust with AI. Some parts of the `media_controller.rs` file, mainly the pulse integration, was also AI-generated.

# Supporters

A huge thank you to everyone supporting the project!

<table>
  <tr>
    <td align="center">
      <a href="https://github.com/davdroman">
        <img src="https://github.com/davdroman.png?size=48" width="48" height="48"alt="davdroman"/><br />
        @davdroman
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/tedsalmon">
        <img src="https://github.com/tedsalmon.png?size=48" width="48" height="48"alt="tedsalmon"/><br />
        @tedsalmon
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/wiless">
        <img src="https://github.com/wiless.png?size=48" width="48" height="48"alt="wiless"/><br />
        @wiless
      </a>
    </td>
  </tr>
  <tr>
    <td align="center">
      <a href="https://github.com/SmartMsg">
        <img src="https://github.com/SmartMsg.png?size=48" width="48" height="48"alt="SmartMsg"/><br />
        @SmartMsg
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/lunaroyster">
        <img src="https://github.com/lunaroyster.png?size=48" width="48" height="48"alt="lunaroyster"/><br />
        @lunaroyster
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/ressiwage">
        <img src="https://github.com/ressiwage.png?size=48" width="48" height="48"alt="ressiwage"/><br />
        @ressiwage
      </a>
    </td>
  </tr>
  <tr>
    <td align="center">
      <a href="https://github.com/kkjdroid">
        <img src="https://github.com/kkjdroid.png?size=48" width="48" height="48"alt="kkjdroid"/><br />
        @kkjdroid
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/CitrusJoules">
        <img src="https://github.com/CitrusJoules.png?size=48" width="48" height="48"alt="CitrusJoules"/><br />
        @CitrusJoules
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/DanielReyesDev">
        <img src="https://github.com/DanielReyesDev.png?size=48" width="48" height="48"alt="DanielReyesDev"/><br />
        @DanielReyesDev
      </a>
    </td>
  </tr>
  <tr>
    <td align="center">
      <a href="https://github.com/sumitduster">
        <img src="https://github.com/sumitduster.png?size=48" width="48" height="48"alt="sumitduster"/><br />
        @sumitduster
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/GrifTheDev">
        <img src="https://github.com/GrifTheDev.png?size=48" width="48" height="48"alt="GrifTheDev"/><br />
        @GrifTheDev
      </a>
    </td>
  </tr>
</table>

# Special Thanks
- @tyalie for making the first documentation on the protocol! ([tyalie/AAP-Protocol-Definition](https://github.com/tyalie/AAP-Protocol-Defintion))
- @rithvikvibhu and folks over at lagrangepoint for helping with the hearing aid feature ([gist](https://gist.github.com/rithvikvibhu/45e24bbe5ade30125f152383daf07016))
- @devnoname120 for helping with the first root patch
- @timgromeyer for making the first version of the linux app
- @hackclub for hosting [High Seas](https://highseas.hackclub.com) and [Low Skies](https://low-skies.hackclub.com)!
- Of course, everyone who has contributed to the project in any way, including by testing, sharing feedback, or just showing interest!

# Alternates for other platforms:
- CAPod - A companion app for AirPods on Android. ([play store](https://play.google.com/store/apps/details?id=eu.darken.capod) | [source code](https://github.com/d4rken-org/capod)). Use this if you're using Android version 16 QPR3 or below and are not rooted.
- MagicPods for Steam Deck ([website](https://magicpods.app/steamdeck/))
- MagicPods - if you're looking for "LibrePods for Windows"  ([ms store](https://apps.microsoft.com/store/detail/9P6SKKFKSHKM) [installer](https://magicpods.app/installer/MagicPods.appinstaller) | [website](https://magicpods.app/))

# Star History

<a href="https://www.star-history.com/?repos=librepods-org%2Flibrepods&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/chart?repos=librepods-org/librepods&type=date&theme=dark&legend=top-left&sealed_token=CS9QF2Wcx-_EN6M7lBhwvTa2WBd5eB8FbQa5FWt3SV14v1OcQdy2aCGsXxHoytVaoylP_pTxj1h2U-odoXDV_EwTqMxM-MQwG_WpvV_1g_yH-Jh3ux2BuWCmS98LxLqfox2ibLOlFDHu_-geTLFsTmqTc1SjX6-NYxVHXF5DaqnxFOAyjLPL5_WtXVew" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/chart?repos=librepods-org/librepods&type=date&legend=top-left&sealed_token=CS9QF2Wcx-_EN6M7lBhwvTa2WBd5eB8FbQa5FWt3SV14v1OcQdy2aCGsXxHoytVaoylP_pTxj1h2U-odoXDV_EwTqMxM-MQwG_WpvV_1g_yH-Jh3ux2BuWCmS98LxLqfox2ibLOlFDHu_-geTLFsTmqTc1SjX6-NYxVHXF5DaqnxFOAyjLPL5_WtXVew" />
   <img alt="Star History Chart" src="https://api.star-history.com/chart?repos=librepods-org/librepods&type=date&legend=top-left&sealed_token=CS9QF2Wcx-_EN6M7lBhwvTa2WBd5eB8FbQa5FWt3SV14v1OcQdy2aCGsXxHoytVaoylP_pTxj1h2U-odoXDV_EwTqMxM-MQwG_WpvV_1g_yH-Jh3ux2BuWCmS98LxLqfox2ibLOlFDHu_-geTLFsTmqTc1SjX6-NYxVHXF5DaqnxFOAyjLPL5_WtXVew" />
 </picture>
</a>

# License

LibrePods - AirPods liberated from Apple’s ecosystem
Copyright (C) 2025 LibrePods contributors

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.

# Trademark Notice

The GPL does not grant any rights to use the LibrePods name, logo, or branding. The LibrePods name and logo may not be used for software, websites, domains, products, services, or other projects in a manner that suggests affiliation with, endorsement by, or association with the official LibrePods project without prior permission.

If you see any misuse of the LibrePods name or logo, please report it to [me@kavish.xyz](mailto:me@kavish.xyz).

The SF Pro font used in the Android app is the property of Apple Inc.. This will be removed in future versions of the app and replaced with an open alternative soon.

AirPods, AirPods Pro, AirPods Max, and the AirPods logo are trademarks of Apple Inc. The LibrePods project is not affiliated with or endorsed by Apple Inc. in any way.
