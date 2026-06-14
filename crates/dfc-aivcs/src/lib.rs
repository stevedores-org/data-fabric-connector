//! aivcs-api client adapter for DFC.

mod client;
mod config;

pub use client::{AivcsClient, HttpAivcsClient, MockAivcsClient, ReplayOperation};
pub use config::AivcsConfig;
