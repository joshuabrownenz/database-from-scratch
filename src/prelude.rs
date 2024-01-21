// Source: https://github.com/jeremychone-channel/rust-base

//! Crate prelude

// Re-export the crate Error.
pub use crate::error::Error;

// Alias Result to be the crate Result.
pub type Result<T> = core::result::Result<T, Error>;

// Generic Wrapper tuple struct for newtype pattern,
// mostly for external type to type From/TryFrom conversions
// pub struct W<T>(pub T); // Commented out cause I think it is unnecessary.

// Personal preference.
pub use std::format as f;
