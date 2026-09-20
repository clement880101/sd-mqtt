//! Durable storage for the broker.
//!
//! Nothing in the broker knows this crate exists yet. That is deliberate:
//! section 7.6 of the design builds one real backend and keeps the trait
//! honest with an in-memory one, and both are proved on their own before
//! anything depends on them.
