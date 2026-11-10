//! The core's inputs from an AACP session, and the packets announcing Linux to the other hosts.

use crate::bluetooth::aacp::{
    AACPEvent, AACPManager, AACPManagerState, ControlCommandIdentifiers as Control, TipiFields,
};
use crate::handoff::{Input, SessionFacts};
use log::error;

/// Copy a session's routing, wear and ownership state for the core. The
/// caller holds the guard only for the copy.
pub(crate) fn session_facts(state: &AACPManagerState) -> SessionFacts {
    SessionFacts {
        hosts: state.connected_devices.clone(),
        ears: state.ear_detection_status.clone(),
        audio: state.audio_source.clone(),
        owner: state
            .control_command_status_list
            .iter()
            .find(|s| s.identifier == Control::OwnsConnection)
            .map(|s| s.value.clone()),
    }
}

/// The core inputs that replay a session snapshot, for reports the AirPods
/// sent before the shell saw the event stream. Decision-free: the hosts are
/// reported as all new, and the core announces nothing while not automatic.
pub(crate) fn seed_inputs(facts: SessionFacts) -> Vec<Input> {
    let mut inputs = vec![
        Input::ConnectedDevices {
            old: Vec::new(),
            new: facts.hosts,
        },
        Input::EarState(facts.ears),
    ];
    if let Some(audio) = facts.audio {
        inputs.push(Input::AudioSource(audio));
    }
    if let Some(owner) = facts.owner {
        inputs.push(Input::Ownership(owner));
    }
    inputs
}

/// The core input for an AACP event, if the core takes it.
pub(crate) fn event_input(event: AACPEvent) -> Option<Input> {
    match event {
        AACPEvent::ControlCommand(s) if s.identifier == Control::OwnsConnection => {
            Some(Input::Ownership(s.value))
        }
        AACPEvent::ConnectedDevices(old, new) => Some(Input::ConnectedDevices { old, new }),
        AACPEvent::EarDetection(_, new) => Some(Input::EarState(new)),
        AACPEvent::AudioSource(source) => Some(Input::AudioSource(source)),
        AACPEvent::OwnershipToFalseRequest => Some(Input::RemoteOwnershipRequest),
        AACPEvent::MediaInformation { host, category } => {
            Some(Input::MediaInformation { host, category })
        }
        _ => None,
    }
}

/// Announce Linux to every connected remote host (the core's `Announce`),
/// with the tipi values `fields`.
pub(crate) async fn announce(
    manager: &AACPManager,
    local: &str,
    fields: TipiFields,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let peers = manager.state.lock().await.connected_devices.clone();
    for peer in peers.iter().filter(|d| d.mac != local) {
        manager
            .send_media_information_new_device(local, &peer.mac)
            .await?;
        manager
            .send_add_tipi_device(local, &peer.mac, fields)
            .await?;
    }
    Ok(())
}

/// Re-send Linux's tipi, without newTipi, to every connected remote host
/// (#509). A failed send is logged and the next host still gets its update.
pub(super) async fn send_tipi_updates(manager: &AACPManager, local: &str, fields: TipiFields) {
    let peers = manager.state.lock().await.connected_devices.clone();
    for peer in peers.iter().filter(|d| d.mac != local) {
        if let Err(e) = manager.send_tipi_update(local, &peer.mac, fields).await {
            error!(
                "Handoff: failed to send the tipi update to {}: {e}",
                peer.mac
            );
        }
    }
}

/// Tell every connected remote host whether Linux is in a call (the core's
/// `AdvertiseCall`): the in-call media information, or the idle one the
/// announcement sends once the call ends.
pub(crate) async fn advertise_call(
    manager: &AACPManager,
    local: &str,
    in_call: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let peers = manager.state.lock().await.connected_devices.clone();
    for peer in peers.iter().filter(|d| d.mac != local) {
        if in_call {
            manager
                .send_media_information_call(local, &peer.mac)
                .await?;
        } else {
            manager
                .send_media_information_new_device(local, &peer.mac)
                .await?;
        }
    }
    Ok(())
}

/// Relay Linux's audio category to every connected remote host (the core's
/// `RelayCategory`, #272).
pub(super) async fn relay_category(
    manager: &AACPManager,
    local: &str,
    category: u32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let peers = manager.state.lock().await.connected_devices.clone();
    for peer in peers.iter().filter(|d| d.mac != local) {
        manager
            .send_media_information_category(local, &peer.mac, category)
            .await?;
    }
    Ok(())
}

/// Whether the AACP receive loop still has the control session. The guard is
/// dropped before this returns.
pub(super) async fn session_alive(manager: &AACPManager) -> bool {
    manager.state.lock().await.sender.is_some()
}
