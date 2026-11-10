#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(
    clippy::std_instead_of_core,
    clippy::std_instead_of_alloc,
    clippy::alloc_instead_of_core
)]

extern crate alloc;

mod capabilities;
mod channel;
mod connector;
mod deadline;
mod failure;
mod stream;
mod stream_channel;
mod sync;

#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub mod asynch;
pub mod frame;
pub mod memory;
pub mod stdio;
pub mod tcp;

pub use capabilities::{Capabilities, Carrier};
pub use channel::{Channel, Datagrams, Delivery};
pub use connector::{Connector, Listener};
pub use coqui_builder::NonDefault;
pub use failure::Failure;
pub use stream::{Duplex, Halves, Interrupt, Split};
pub use stream_channel::{
    StreamChannel, StreamChannelBuilder, StreamChannelParts, StreamChannelSettings,
};

/// The setters trait of a settings type, for a builder in another crate that holds the settings. A
/// [`StreamChannelBuilder`] needs no import.
pub mod prelude {
    pub use crate::stream_channel::StreamChannelSettingsSetters;
}
