//! Device adapters and the durable DAG record store.
//!
//! `io` contains block-device implementations and fault-test adapters;
//! `dag_store` contains the record log, scanner, and snapshot validation.

mod device_script;
mod media_model;

#[cfg(any(unix, windows))]
mod file_device;

#[path = "file_dag.rs"]
pub mod dag_store;

pub mod io {
    pub use super::device_script::*;
    pub use super::media_model::*;

    #[cfg(any(unix, windows))]
    pub use super::file_device::FileDevice;
}

pub use dag_store::{
    FileDagStore, FileDagStoreError, RecordKind, VerifiedSnapshot, RECORD_HEADER_LEN,
};
pub use io::*;
