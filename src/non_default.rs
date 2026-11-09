//! A setting that differs from its default, reported as data.

use alloc::string::String;
use alloc::vec::Vec;

/// A setting whose value differs from its default, as plain data for the caller to show or log.
///
/// A non-default setting is never refused for it; it is listed, and the application that knows
/// the use case decides what it means.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NonDefault {
    /// The setting's name, as its owner spells it (`max_message`, `waiting.per_source`).
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

/// What stands for a secret value in a report.
const PLACEHOLDER: &str = "***";

impl NonDefault {
    /// The non-default setting a guatiao [`Difference`](guatiao::schema::Difference) reports:
    /// `setting` is its [`key`](guatiao::schema::Difference::key) (`waiting.per_source`), and
    /// `value` and `default` are rendered by one rule.
    ///
    /// A text value is its text, any other value is its compact JSON (`true`, `16`, `[60,0]`), and a
    /// side the difference has no value for is `unset`.
    pub fn from_difference(difference: &guatiao::schema::Difference) -> Self {
        Self::new(
            difference.key(),
            render(difference.value.as_ref()),
            render(difference.baseline.as_ref()),
        )
    }

    /// Every key of `settings` whose value differs from the default its schema declares, named by
    /// key path and in key order: empty for [`Default`] settings.
    pub fn of_settings<T: guatiao::ToValue + guatiao::Schema>(settings: &T) -> Vec<Self> {
        // The schema and the value are derived from one type, so the walk cannot fail unless the
        // allocator does.
        let derived = "a settings type is a value that builds";
        let schema = T::schema().expect(derived);
        let schema = guatiao::schema::SchemaRef::new(&schema).expect(derived);
        let value = settings.to_value().expect(derived);
        guatiao::schema::differences(schema, &value, None, PLACEHOLDER)
            .expect(derived)
            .iter()
            .map(Self::from_difference)
            .collect()
    }
}

fn render(value: Option<&guatiao::Value>) -> String {
    use guatiao::FromValue;
    let Some(value) = value else {
        return "unset".into();
    };
    String::from_value(value).unwrap_or_else(|_| {
        guatiao_serde::text::json::to_string(value, guatiao_serde::Presentation::new())
            .unwrap_or_else(|_| "unrepresentable".into())
    })
}
