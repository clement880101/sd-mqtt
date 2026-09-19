//! Subscription matching and retained message storage.
//!
//! This crate answers two questions, both synchronously and without any
//! async machinery: given a topic, which subscribers want it, and given a
//! filter, which retained messages match it.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod topic;
