use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

use crate::{BlockDevice, DeviceError, IoOperation};

/// A fixed-capacity, position-independent file-backed block device.
#[derive(Debug)]
pub struct FileDevice {
    file: File,
    len: u64,
}

impl FileDevice {
    /// Opens an existing file without changing its size or contents.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DeviceError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|error| io_error(IoOperation::Open, error))?;
        Self::from_file(file)
    }

    /// Creates a new file with a fixed capacity and makes that size durable.
    pub fn create_preallocated(path: impl AsRef<Path>, capacity: u64) -> Result<Self, DeviceError> {
        let path = path.as_ref();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|error| io_error(IoOperation::Open, error))?;
        file.set_len(capacity)
            .map_err(|error| io_error(IoOperation::SetLen, error))?;
        file.sync_all()
            .map_err(|error| io_error(IoOperation::SyncAll, error))?;
        sync_parent_directory(path)?;
        Self::from_file(file)
    }

    fn from_file(file: File) -> Result<Self, DeviceError> {
        let len = file
            .metadata()
            .map_err(|error| io_error(IoOperation::Metadata, error))?
            .len();
        Ok(Self { file, len })
    }

    fn bounds(&self, offset: u64, len: usize) -> Result<(), DeviceError> {
        let end = offset
            .checked_add(len as u64)
            .ok_or(DeviceError::OutOfBounds {
                offset,
                len,
                capacity: self.len,
            })?;
        if end > self.len {
            return Err(DeviceError::OutOfBounds {
                offset,
                len,
                capacity: self.len,
            });
        }
        Ok(())
    }
}

impl BlockDevice for FileDevice {
    fn len(&self) -> u64 {
        self.len
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), DeviceError> {
        self.bounds(offset, buf.len())?;
        let mut done = 0;
        while done < buf.len() {
            let n = loop {
                match read_once(&self.file, &mut buf[done..], offset + done as u64) {
                    Ok(n) => break n,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(io_error(IoOperation::Read, error)),
                }
            };
            if n == 0 {
                return Err(io_error(
                    IoOperation::Read,
                    io::Error::from(io::ErrorKind::UnexpectedEof),
                ));
            }
            done += n;
        }
        Ok(())
    }

    fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), DeviceError> {
        self.bounds(offset, buf.len())?;
        let mut done = 0;
        while done < buf.len() {
            let n = loop {
                match write_once(&self.file, &buf[done..], offset + done as u64) {
                    Ok(n) => break n,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(io_error(IoOperation::Write, error)),
                }
            };
            if n == 0 {
                return Err(io_error(
                    IoOperation::Write,
                    io::Error::from(io::ErrorKind::WriteZero),
                ));
            }
            done += n;
        }
        Ok(())
    }

    fn flush_data(&mut self) -> Result<(), DeviceError> {
        self.file
            .sync_data()
            .map_err(|error| io_error(IoOperation::SyncData, error))
    }

    fn flush_all(&mut self) -> Result<(), DeviceError> {
        self.file
            .sync_all()
            .map_err(|error| io_error(IoOperation::SyncAll, error))
    }
}

#[cfg(unix)]
fn read_once(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;

    file.read_at(buf, offset)
}

#[cfg(windows)]
fn read_once(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::windows::fs::FileExt;

    file.seek_read(buf, offset)
}

#[cfg(unix)]
fn write_once(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;

    file.write_at(buf, offset)
}

#[cfg(windows)]
fn write_once(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    use std::os::windows::fs::FileExt;

    file.seek_write(buf, offset)
}

fn io_error(operation: IoOperation, error: io::Error) -> DeviceError {
    DeviceError::Io {
        operation,
        kind: error.kind(),
    }
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<(), DeviceError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let directory =
        File::open(parent).map_err(|error| io_error(IoOperation::SyncDirectory, error))?;
    directory
        .sync_all()
        .map_err(|error| io_error(IoOperation::SyncDirectory, error))
}

#[cfg(windows)]
fn sync_parent_directory(_path: &Path) -> Result<(), DeviceError> {
    Ok(())
}
