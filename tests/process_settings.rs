//! The process settings are values a file fills, each setter reaches them, and `non_defaults` names
//! what differs.

use coqui_builder::Partial;
use coqui_transport::process::{ErrorOutput, Launch, ProcessBuilder, ProcessSettings};

fn from_toml(text: &str) -> Result<ProcessSettings, String> {
    toml::from_str::<Partial<ProcessSettings>>(text)
        .map_err(|e| e.to_string())?
        .build()
        .map_err(|e| e.to_string())
}

#[test]
fn an_empty_file_is_every_default() {
    let settings = from_toml("").unwrap();
    assert_eq!(settings, ProcessSettings::new());
    assert!(settings.non_defaults().is_empty());
}

#[test]
fn a_file_sets_each_key_and_unknown_keys_are_refused() {
    let settings = from_toml("stderr = \"inherit\"\nstderr_tail = 10\nconsole = true").unwrap();
    assert_eq!(settings.stderr, ErrorOutput::Inherit);
    assert_eq!(settings.stderr_tail, 10);
    assert!(settings.console);
    let keys: Vec<String> = settings
        .non_defaults()
        .into_iter()
        .map(|n| n.setting)
        .collect();
    assert_eq!(keys, ["console", "stderr", "stderr_tail"]);
    assert!(from_toml("stderr_taill = 1").is_err());
    assert!(from_toml("stderr = \"elsewhere\"").is_err());
}

#[test]
fn the_setters_reach_the_settings_and_a_launch_is_required() {
    let builder = ProcessBuilder::new()
        .stderr(ErrorOutput::Discard)
        .stderr_tail(7)
        .console(true);
    let keys: Vec<String> = builder
        .non_defaults()
        .into_iter()
        .map(|n| n.setting)
        .collect();
    assert_eq!(keys, ["console", "stderr", "stderr_tail"]);
    let missing = builder.build().unwrap_err().to_string();
    assert!(missing.contains("launch"), "{missing}");
    let builder = ProcessBuilder::new().launch(Launch::new("x"));
    assert!(builder.non_defaults().is_empty());
}
