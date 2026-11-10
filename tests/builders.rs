//! Each builder setter reaches the settings it builds, `settings` takes the whole, the last setter
//! wins, and the settings are values a file fills. `Capabilities` and `Halves` set by name.

use std::time::Duration;

use coqui_builder::Partial;
use coqui_transport::frame::LengthPrefix;
use coqui_transport::memory;
use coqui_transport::{
    Capabilities, Carrier, Delivery, Halves, Interrupt, NonDefault, StreamChannel,
    StreamChannelBuilder, StreamChannelSettings,
};

type Builder = StreamChannelBuilder<LengthPrefix, memory::Stream>;

fn builder() -> Builder {
    StreamChannelBuilder::new().codec(LengthPrefix::u32_be())
}

/// The setting, value and default of each line, as text.
fn lines(found: &[NonDefault]) -> Vec<(String, String, String)> {
    found
        .iter()
        .map(|n| {
            (
                n.setting.clone(),
                n.value.to_string(),
                n.default.to_string(),
            )
        })
        .collect()
}

fn from_toml(text: &str) -> Result<StreamChannelSettings, String> {
    let partial =
        toml::from_str::<Partial<StreamChannelSettings>>(text).map_err(|e| e.to_string())?;
    partial.build().map_err(|e| e.to_string())
}

fn from_json(text: &str) -> Result<StreamChannelSettings, String> {
    let partial =
        serde_json::from_str::<Partial<StreamChannelSettings>>(text).map_err(|e| e.to_string())?;
    partial.build().map_err(|e| e.to_string())
}

#[test]
fn every_setter_reaches_the_settings_the_builder_reports() {
    let found = builder()
        .max_message(10)
        .pending(11)
        .queued_bytes(12)
        .send_limit(Duration::from_secs(13))
        .read_buffer(14)
        .linger(Duration::from_secs(15))
        .non_defaults();
    let by_key: Vec<(String, String)> = lines(&found)
        .into_iter()
        .map(|(setting, value, _)| (setting, value))
        .collect();
    let expected: Vec<(String, String)> = [
        ("linger", "Duration(15s)"),
        ("max_message", "usize(10)"),
        ("pending", "usize(11)"),
        ("queued_bytes", "usize(12)"),
        ("read_buffer", "usize(14)"),
        ("send_limit", "Duration(13s)"),
    ]
    .map(|(setting, value)| (setting.to_owned(), value.to_owned()))
    .into();
    assert_eq!(by_key, expected);
}

#[test]
fn fill_takes_the_whole_and_the_last_setter_wins() {
    let mut whole = StreamChannelSettings::default();
    whole.pending = 5;
    let found = builder()
        .max_message(1)
        .fill(Partial::from(whole))
        .pending(6)
        .non_defaults();
    assert_eq!(
        lines(&found),
        [(
            "pending".to_owned(),
            "usize(6)".to_owned(),
            "usize(64)".to_owned()
        )]
    );
}

#[test]
fn the_defaults_report_nothing() {
    assert!(StreamChannelSettings::default().non_defaults().is_empty());
    assert!(builder().non_defaults().is_empty());
}

#[test]
fn settings_read_from_toml_and_json() {
    let settings = from_toml(
        "max_message = 7
linger = 2
",
    )
    .unwrap();
    assert_eq!(settings.max_message, 7);
    assert_eq!(settings.linger, Duration::from_secs(2));
    assert_eq!(settings.pending, StreamChannel::DEFAULT_PENDING);
    assert_eq!(from_toml("").unwrap(), StreamChannelSettings::default());
    let mut changed = StreamChannelSettings::default();
    changed.queued_bytes = 99;
    changed.send_limit = Duration::from_millis(1500);
    let text = "queued_bytes = 99
send_limit = \"1500ms\"
";
    assert_eq!(from_toml(text).unwrap(), changed);
    let text = r#"{"queued_bytes": 99, "send_limit": "1500ms"}"#;
    assert_eq!(from_json(text).unwrap(), changed);
}

#[test]
fn settings_refuse_unknown_keys_and_unreadable_values() {
    assert!(from_toml("pendin = 1").is_err());
    assert!(from_toml("linger = \"soon\"").is_err());
    assert!(from_json(r#"{"pendin": 1}"#).is_err());
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
