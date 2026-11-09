//! Each builder setter reaches the settings it builds, `settings` takes the whole, the last setter
//! wins, and the settings are values a file fills. `Capabilities` and `Halves` set by name.

use std::time::Duration;

use coqui_transport::frame::LengthPrefix;
use coqui_transport::{
    Capabilities, Carrier, Delivery, Halves, Interrupt, NonDefault, StreamChannel,
    StreamChannelSettings,
};
use guatiao_serde::Presentation;
use guatiao_serde::text::{json, toml};

fn plain() -> Presentation {
    Presentation::new()
}

#[test]
fn every_setter_reaches_the_settings_the_builder_reports() {
    let found = StreamChannel::builder(LengthPrefix::u32_be())
        .max_message(10)
        .pending(11)
        .queued_bytes(12)
        .send_limit(Duration::from_secs(13))
        .read_buffer(14)
        .linger(Duration::from_secs(15))
        .non_defaults();
    let by_key: Vec<(&str, &str)> = found
        .iter()
        .map(|n| (n.setting.as_str(), n.value.as_str()))
        .collect();
    assert_eq!(
        by_key,
        [
            ("linger", "[15,0]"),
            ("max_message", "10"),
            ("pending", "11"),
            ("queued_bytes", "12"),
            ("read_buffer", "14"),
            ("send_limit", "[13,0]"),
        ]
    );
}

#[test]
fn settings_take_the_whole_and_the_last_setter_wins() {
    let mut whole = StreamChannelSettings::default();
    whole.pending = 5;
    let found = StreamChannel::builder(LengthPrefix::u32_be())
        .max_message(1)
        .settings(whole)
        .pending(6)
        .non_defaults();
    assert_eq!(found, [NonDefault::new("pending", "6", "64")]);
}

#[test]
fn the_defaults_report_nothing() {
    assert!(StreamChannelSettings::default().non_defaults().is_empty());
    assert!(
        StreamChannel::builder(LengthPrefix::u32_be())
            .non_defaults()
            .is_empty()
    );
}

#[test]
fn settings_read_from_toml_and_json_and_round_trip() {
    let settings: StreamChannelSettings =
        toml::from_str_as("max_message = 7\nlinger = 2\n", plain()).unwrap();
    assert_eq!(settings.max_message, 7);
    assert_eq!(settings.linger, Duration::from_secs(2));
    assert_eq!(settings.pending, StreamChannel::DEFAULT_PENDING);
    assert_eq!(
        toml::from_str_as::<StreamChannelSettings>("", plain()).unwrap(),
        StreamChannelSettings::default()
    );
    let mut changed = StreamChannelSettings::default();
    changed.queued_bytes = 99;
    changed.send_limit = Duration::from_millis(1500);
    let text = toml::to_string_of(&changed, plain()).unwrap();
    assert_eq!(
        toml::from_str_as::<StreamChannelSettings>(&text, plain()).unwrap(),
        changed
    );
    let text = json::to_string_pretty_of(&changed, plain()).unwrap();
    assert_eq!(
        json::from_str_as::<StreamChannelSettings>(&text, plain()).unwrap(),
        changed
    );
}

#[test]
fn settings_refuse_unknown_keys() {
    assert!(toml::from_str_as::<StreamChannelSettings>("pendin = 1", plain()).is_err());
    assert!(toml::from_str_as::<StreamChannelSettings>("linger = \"soon\"", plain()).is_err());
}

#[test]
fn capabilities_are_set_by_name() {
    let capabilities = Capabilities::new()
        .carrier(Carrier::Stream)
        .limits(10, 20)
        .datagrams(Some((Delivery::Unreliable, Some(30))));
    assert_eq!(capabilities.carrier, Carrier::Stream);
    assert_eq!(capabilities.max_send, Some(10));
    assert_eq!(capabilities.max_receive, Some(20));
    assert_eq!(capabilities.datagrams, Some(Delivery::Unreliable));
    assert_eq!(capabilities.max_datagram, Some(30));
    assert_eq!(capabilities.datagrams(None).max_datagram, None);
}

#[test]
fn halves_take_their_interrupt_by_name() {
    let halves = Halves::new(std::io::empty(), std::io::sink());
    assert!(halves.interrupt.is_none());
    let halves = halves.interrupt(Interrupt::new(|| {}));
    assert!(halves.interrupt.is_some());
}
