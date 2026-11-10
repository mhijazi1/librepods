use super::*;

#[test]
fn seed_inputs_copy_the_snapshot_without_decisions() {
    let facts = SessionFacts {
        hosts: vec![host(LOCAL), host(REMOTE)],
        ears: vec![EarDetectionStatus::InEar; 2],
        audio: None,
        owner: Some(vec![0]),
    };
    assert_eq!(
        seed_inputs(facts),
        [
            Input::ConnectedDevices {
                old: Vec::new(),
                new: vec![host(LOCAL), host(REMOTE)],
            },
            Input::EarState(vec![EarDetectionStatus::InEar; 2]),
            Input::Ownership(vec![0]),
        ]
    );
}

#[tokio::test]
async fn category_relay_reaches_each_remote_host() {
    let (manager, mut rx, _tx) = ready_session().await;
    manager
        .state
        .lock()
        .await
        .connected_devices
        .push(host(SECOND_REMOTE));
    relay_category(&manager, LOCAL, AUDIO_CATEGORY_STREAM)
        .await
        .expect("relay");
    for remote in [REMOTE, SECOND_REMOTE] {
        let expected = reference(|m| async move {
            m.send_media_information_category(LOCAL, remote, AUDIO_CATEGORY_STREAM)
                .await
        })
        .await;
        assert_eq!(rx.try_recv().expect("a relay"), expected, "{remote}");
    }
    assert!(rx.try_recv().is_err());
}
