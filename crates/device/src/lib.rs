mod device_script;

pub use device_script::*;

#[cfg(any(unix, windows))]
mod file_device;

#[cfg(any(unix, windows))]
pub use file_device::FileDevice;
