//! A setting that differs from its default, reported as data.

use alloc::string::String;

/// A setting whose value differs from its default, as plain data for the caller to show or log.
///
/// A non-default setting is never refused for it; it is listed, and the application that knows
/// the use case decides what it means.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NonDefault {
    /// The setting's name, as its owner spells it (`max_message`, `linger`).
    pub setting: String,
    /// Its value, as text.
    pub value: String,
    /// The default value, as text.
    pub default: String,
}

impl NonDefault {
    /// A non-default `setting`, now `value`, against `default`.
    pub fn new(
        setting: impl Into<String>,
        value: impl Into<String>,
        default: impl Into<String>,
    ) -> Self {
        Self {
            setting: setting.into(),
            value: value.into(),
            default: default.into(),
        }
    }
}
