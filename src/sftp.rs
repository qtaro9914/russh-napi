use std::sync::Arc;

use napi::bindgen_prelude::Uint8Array;
use napi_derive::napi;
use russh_sftp::client::error::Error as SftpError;
use russh_sftp::client::fs::DirEntry;
use russh_sftp::client::{RawSftpSession, SftpSession};
use russh_sftp::protocol::{FileAttributes, FileType, OpenFlags, StatusCode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::error::WrappedError;

#[allow(dead_code)]
#[napi]
pub const OPEN_READ: u32 = OpenFlags::READ.bits();

#[allow(dead_code)]
#[napi]
pub const OPEN_WRITE: u32 = OpenFlags::WRITE.bits();

#[allow(dead_code)]
#[napi]
pub const OPEN_APPEND: u32 = OpenFlags::APPEND.bits();

#[allow(dead_code)]
#[napi]
pub const OPEN_CREATE: u32 = OpenFlags::CREATE.bits();

#[allow(dead_code)]
#[napi]
pub const OPEN_TRUNCATE: u32 = OpenFlags::TRUNCATE.bits();

/// russh-sftp's write length when the server doesn't advertise
/// `limits@openssh.com` (matches its private `MAX_WRITE_LENGTH`)
const DEFAULT_WRITE_LEN: u64 = 261120;

/// Writes `data` at `offset` as concurrent WRITEs of at most `max_len` bytes.
///
/// Every WRITE is driven to completion even after another one has failed:
/// `RawSftpSession` only removes a request from its pending table when that
/// request's own future sees the reply or times out, so dropping the rest on
/// the first error (as `try_join_all` does) would leave their entries behind -
/// forever, if the server never answers.
async fn write_split(
    session: &RawSftpSession,
    handle: &str,
    offset: u64,
    data: &[u8],
    max_len: usize,
) -> Result<(), SftpError> {
    let writes = data.chunks(max_len).enumerate().map(|(i, piece)| {
        session.write(
            handle.to_string(),
            offset + (i * max_len) as u64,
            piece.to_vec(),
        )
    });
    // returns the first error in data order
    futures::future::join_all(writes)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    Ok(())
}

#[napi]
#[derive(Debug, Clone)]
pub struct SftpFileMetadata {
    inner: FileAttributes,
    pub size: String, // u64
    pub uid: Option<u32>,
    pub user: Option<String>,
    pub gid: Option<u32>,
    pub group: Option<String>,
    pub permissions: Option<u32>,
    pub atime: Option<u32>,
    pub mtime: Option<u32>,
}

#[napi]
impl SftpFileMetadata {
    #[napi]
    pub fn type_(&self) -> SftpFileType {
        self.inner.file_type().into()
    }
}

impl From<&FileAttributes> for SftpFileMetadata {
    fn from(value: &FileAttributes) -> Self {
        Self {
            inner: value.clone(),
            size: format!("{}", value.size.unwrap_or(0)),
            uid: value.uid,
            user: value.user.clone(),
            gid: value.gid,
            group: value.group.clone(),
            permissions: value.permissions,
            atime: value.atime,
            mtime: value.mtime,
        }
    }
}

#[napi]
#[derive(Clone)]
pub enum SftpFileType {
    Directory,
    File,
    Symlink,
    Other,
}

impl From<FileType> for SftpFileType {
    fn from(value: FileType) -> Self {
        match value {
            FileType::Dir => SftpFileType::Directory,
            FileType::File => SftpFileType::File,
            FileType::Symlink => SftpFileType::Symlink,
            FileType::Other => SftpFileType::Other,
        }
    }
}

#[napi]
#[derive(Clone)]
pub struct SftpDirEntry {
    pub name: String,
    pub type_: SftpFileType,
    metadata: SftpFileMetadata,
}

#[napi]
impl SftpDirEntry {
    #[napi]
    pub fn metadata(&self) -> SftpFileMetadata {
        self.metadata.clone()
    }
}

impl From<DirEntry> for SftpDirEntry {
    fn from(entry: DirEntry) -> Self {
        SftpDirEntry {
            name: entry.file_name(),
            type_: entry.file_type().into(),
            metadata: SftpFileMetadata::from(&entry.metadata()),
        }
    }
}

#[napi]
pub struct SftpFile {
    handle: Arc<Mutex<russh_sftp::client::fs::File>>,
}

#[napi]
impl SftpFile {
    #[napi]
    pub async fn read(&self, n: u32) -> napi::Result<Uint8Array> {
        let mut handle = self.handle.lock().await;
        let mut buf = Vec::with_capacity(n as usize);
        let len = handle
            .read_buf(&mut buf)
            .await
            .map_err(WrappedError::from)?;
        Ok(Uint8Array::from(&buf[..len]))
    }

    /// Reads up to `n` bytes at the given absolute file offset.
    ///
    /// Unlike `read`, this does not use (or advance) the file cursor and is
    /// issued as a single protocol-level READ, so multiple `readAt` calls
    /// may be safely in flight concurrently — responses are matched by
    /// request id. Returns an empty array on EOF. May return fewer bytes
    /// than requested (e.g. when `n` exceeds the server's per-request
    /// read limit).
    #[napi]
    pub async fn read_at(&self, offset: f64, n: u32) -> napi::Result<Uint8Array> {
        // Only hold the file lock long enough to grab the raw session and
        // handle — holding it across the network round-trip would serialize
        // concurrent readAt calls and defeat pipelining
        let (session, sftp_handle) = {
            let handle = self.handle.lock().await;
            (handle.raw_session(), handle.raw_handle().to_string())
        };
        // f64 offsets are exact up to 2^53 — far beyond realistic file sizes
        let offset = offset.max(0.0) as u64;
        // servers advertise a per-request read limit (limits@openssh.com);
        // exceeding it is a hard client-side error, so clamp
        let n = session
            .configured_limits()
            .read_len
            .map_or(n, |limit| (n as u64).min(limit) as u32);
        match session.read(sftp_handle, offset, n).await {
            Ok(data) => Ok(Uint8Array::from(data.data)),
            Err(SftpError::Status(status)) if status.status_code == StatusCode::Eof => {
                Ok(Uint8Array::from(&[][..]))
            }
            Err(e) => Err(WrappedError::from(e).into()),
        }
    }

    /// The server's per-request read length limit in bytes
    /// (`limits@openssh.com`), if advertised. `readAt` requests larger than
    /// this are clamped, so callers can use it to pick an optimal chunk size.
    #[napi]
    pub async fn read_limit(&self) -> Option<f64> {
        let handle = self.handle.lock().await;
        handle.raw_session().configured_limits().read_len.map(|l| l as f64)
    }

    /// Writes all of `data` at the given absolute file offset.
    ///
    /// Unlike `writeAll`, this does not use (or advance) the file cursor.
    /// Each protocol-level WRITE is matched by request id, so multiple
    /// `writeAt` calls may be safely in flight concurrently. Data larger
    /// than the server's per-request write limit is split into several
    /// WRITEs, which are also issued concurrently. Resolves once every
    /// byte has been acknowledged; if a WRITE fails, rejects with the first
    /// error once all of them have settled.
    #[napi]
    pub async fn write_at(&self, offset: f64, data: Uint8Array) -> napi::Result<()> {
        // Same as readAt: don't hold the file lock across the round-trip
        let (session, sftp_handle) = {
            let handle = self.handle.lock().await;
            (handle.raw_session(), handle.raw_handle().to_string())
        };
        // f64 offsets are exact up to 2^53 — far beyond realistic file sizes
        let offset = offset.max(0.0) as u64;
        // exceeding limits@openssh.com is a hard client-side error; without
        // the extension fall back to russh-sftp's own default write length
        let max_len = session
            .configured_limits()
            .write_len
            .unwrap_or(DEFAULT_WRITE_LEN)
            .max(1) as usize;
        write_split(&session, &sftp_handle, offset, &data, max_len)
            .await
            .map_err(WrappedError::from)?;
        Ok(())
    }

    #[napi]
    pub async fn write_all(&self, data: Uint8Array) -> napi::Result<()> {
        let mut handle = self.handle.lock().await;
        handle.write_all(&data).await.map_err(WrappedError::from)?;
        Ok(())
    }

    #[napi]
    pub async fn flush(&self) -> napi::Result<()> {
        let mut handle = self.handle.lock().await;
        handle.flush().await.map_err(WrappedError::from)?;
        Ok(())
    }

    #[napi]
    pub async fn shutdown(&self) -> napi::Result<()> {
        let mut handle = self.handle.lock().await;
        handle.shutdown().await.map_err(WrappedError::from)?;
        Ok(())
    }
}

#[napi]
pub struct SftpChannel {
    pub channel_id: u32,
    handle: Arc<Mutex<SftpSession>>,
}

#[napi]
impl SftpChannel {
    pub fn new(id: u32, ch: SftpSession) -> Self {
        SftpChannel {
            channel_id: id,
            handle: Arc::new(Mutex::new(ch)),
        }
    }

    #[napi]
    pub async fn read_dir(&self, path: String) -> napi::Result<Vec<SftpDirEntry>> {
        let handle = self.handle.lock().await;
        let result = handle.read_dir(path).await.map_err(WrappedError::from)?;
        Ok(result.map(Into::into).collect::<_>())
    }

    #[napi]
    pub async fn stat(&self, path: String) -> napi::Result<SftpFileMetadata> {
        let handle = self.handle.lock().await;
        let result = (&handle.metadata(path).await.map_err(WrappedError::from)?).into();
        Ok(result)
    }

    #[napi]
    pub async fn create_dir(&self, path: String) -> napi::Result<()> {
        let handle = self.handle.lock().await;
        handle.create_dir(path).await.map_err(WrappedError::from)?;
        Ok(())
    }

    #[napi]
    pub async fn readlink(&self, path: String) -> napi::Result<String> {
        let handle = self.handle.lock().await;
        let link = handle.read_link(path).await.map_err(WrappedError::from)?;
        Ok(link)
    }

    #[napi]
    pub async fn rename(&self, src: String, dst: String) -> napi::Result<()> {
        let handle = self.handle.lock().await;
        handle.rename(src, dst).await.map_err(WrappedError::from)?;
        Ok(())
    }

    #[napi]
    pub async fn chmod(&self, path: String, mode: u32) -> napi::Result<()> {
        let handle = self.handle.lock().await;
        let mut metadata = handle.metadata(&path).await.map_err(WrappedError::from)?;
        metadata.permissions = Some(mode);
        handle
            .set_metadata(path, metadata)
            .await
            .map_err(WrappedError::from)?;
        Ok(())
    }

    #[napi]
    pub async fn remove_dir(&self, path: String) -> napi::Result<()> {
        let handle = self.handle.lock().await;
        handle.remove_dir(path).await.map_err(WrappedError::from)?;
        Ok(())
    }

    #[napi]
    pub async fn remove_file(&self, path: String) -> napi::Result<()> {
        let handle = self.handle.lock().await;
        handle.remove_file(path).await.map_err(WrappedError::from)?;
        Ok(())
    }

    #[napi]
    pub async fn close(&self) -> napi::Result<()> {
        let handle = self.handle.lock().await;
        handle.close().await.map_err(WrappedError::from)?;
        Ok(())
    }

    #[napi]
    pub async fn open(&self, path: String, mode: u32) -> napi::Result<SftpFile> {
        let handle = self.handle.lock().await;
        let f = handle
            .open_with_flags(
                path,
                OpenFlags::from_bits(mode).ok_or(napi::Error::from_reason("incorrect mode"))?,
            )
            .await
            .map_err(WrappedError::from)?;

        Ok(SftpFile {
            handle: Arc::new(Mutex::new(f)),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;
    use std::sync::Arc;
    use std::time::Duration;

    use russh_sftp::client::error::Error as SftpError;
    use russh_sftp::client::RawSftpSession;
    use russh_sftp::protocol::StatusCode;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, WriteHalf};
    use tokio::sync::Mutex;

    use super::write_split;

    const SSH_FXP_INIT: u8 = 1;
    const SSH_FXP_VERSION: u8 = 2;
    const SSH_FXP_WRITE: u8 = 6;
    const SSH_FXP_STATUS: u8 = 101;
    const SSH_FX_OK: u32 = 0;
    const SSH_FX_FAILURE: u32 = 4;

    #[derive(Clone, Copy)]
    enum Reply {
        Ok,
        OkAfter(Duration),
        Fail,
        Never,
    }

    struct ReceivedWrite {
        offset: u64,
        data: Vec<u8>,
    }

    fn frame(body: Vec<u8>) -> Vec<u8> {
        let mut packet = (body.len() as u32).to_be_bytes().to_vec();
        packet.extend(body);
        packet
    }

    fn status(id: u32, code: u32) -> Vec<u8> {
        let mut body = vec![SSH_FXP_STATUS];
        body.extend(id.to_be_bytes());
        body.extend(code.to_be_bytes());
        body.extend(0u32.to_be_bytes()); // message
        body.extend(0u32.to_be_bytes()); // language tag
        frame(body)
    }

    fn take_u32(buf: &mut &[u8]) -> u32 {
        let (head, rest) = buf.split_at(4);
        *buf = rest;
        u32::from_be_bytes(head.try_into().unwrap())
    }

    fn take_u64(buf: &mut &[u8]) -> u64 {
        let (head, rest) = buf.split_at(8);
        *buf = rest;
        u64::from_be_bytes(head.try_into().unwrap())
    }

    fn take_string(buf: &mut &[u8]) -> Vec<u8> {
        let len = take_u32(buf) as usize;
        let (head, rest) = buf.split_at(len);
        *buf = rest;
        head.to_vec()
    }

    async fn send(wr: &Mutex<WriteHalf<DuplexStream>>, packet: Vec<u8>) {
        let _ = wr.lock().await.write_all(&packet).await;
    }

    /// A minimal SFTP server answering the n-th WRITE as `plan(n)` says
    fn fake_server(
        stream: DuplexStream,
        plan: fn(usize) -> Reply,
    ) -> Arc<Mutex<Vec<ReceivedWrite>>> {
        let received = Arc::new(Mutex::new(vec![]));
        let log = received.clone();
        let (mut rd, wr) = tokio::io::split(stream);
        let wr = Arc::new(Mutex::new(wr));
        tokio::spawn(async move {
            let mut writes = 0;
            while let Ok(len) = rd.read_u32().await {
                let mut packet = vec![0; len as usize];
                if rd.read_exact(&mut packet).await.is_err() {
                    break;
                }
                let mut body = &packet[1..];
                match packet[0] {
                    SSH_FXP_INIT => {
                        let mut version = vec![SSH_FXP_VERSION];
                        version.extend(3u32.to_be_bytes());
                        send(&wr, frame(version)).await;
                    }
                    SSH_FXP_WRITE => {
                        let id = take_u32(&mut body);
                        let _handle = take_string(&mut body);
                        let offset = take_u64(&mut body);
                        let data = take_string(&mut body);
                        log.lock().await.push(ReceivedWrite { offset, data });
                        match plan(writes) {
                            Reply::Ok => send(&wr, status(id, SSH_FX_OK)).await,
                            Reply::Fail => send(&wr, status(id, SSH_FX_FAILURE)).await,
                            Reply::OkAfter(delay) => {
                                let wr = wr.clone();
                                tokio::spawn(async move {
                                    tokio::time::sleep(delay).await;
                                    send(&wr, status(id, SSH_FX_OK)).await;
                                });
                            }
                            Reply::Never => (),
                        }
                        writes += 1;
                    }
                    other => panic!("unexpected packet type {}", other),
                }
            }
        });
        received
    }

    async fn connect(plan: fn(usize) -> Reply) -> (RawSftpSession, Arc<Mutex<Vec<ReceivedWrite>>>) {
        let (client, server) = tokio::io::duplex(1 << 20);
        let received = fake_server(server, plan);
        let session = RawSftpSession::new(client);
        session.set_timeout(1).await;
        session.init().await.unwrap();
        (session, received)
    }

    #[tokio::test]
    async fn split_write_covers_the_data() {
        let (session, received) = connect(|_| Reply::Ok).await;
        let data: Vec<u8> = (0..2500u32).map(|i| i as u8).collect();
        write_split(&session, "h", 100, &data, 1000).await.unwrap();
        assert_eq!(session.pending_requests(), 0);
        let received = received.lock().await;
        let pieces: Vec<_> = received.iter().map(|w| (w.offset, w.data.len())).collect();
        assert_eq!(pieces, [(100, 1000), (1100, 1000), (2100, 500)]);
        let joined: Vec<u8> = received.iter().flat_map(|w| w.data.clone()).collect();
        assert_eq!(joined, data);
    }

    #[tokio::test]
    async fn failed_split_write_leaves_no_pending_requests() {
        // WRITE #1 fails right away while #2 is still in flight and #3 is
        // never answered (a stalled server - only its timeout ends it)
        let (session, received) = connect(|n| match n {
            1 => Reply::Fail,
            2 => Reply::OkAfter(Duration::from_millis(300)),
            3 => Reply::Never,
            _ => Reply::Ok,
        })
        .await;
        let data = vec![7u8; 4000];
        let result = write_split(&session, "h", 0, &data, 1000).await;
        assert!(
            matches!(&result, Err(SftpError::Status(s)) if s.status_code == StatusCode::Failure),
            "expected the WRITE failure, got {:?}",
            result
        );
        assert_eq!(received.lock().await.len(), 4);
        assert_eq!(
            session.pending_requests(),
            0,
            "requests of the other WRITEs were left in the session's table"
        );
        // well past the request timeout: an orphaned WRITE #3 would stay forever
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(session.pending_requests(), 0);
    }
}
