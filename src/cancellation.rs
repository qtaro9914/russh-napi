use std::future::Future;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};

use napi_derive::napi;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

/// Shared by transport creation, the handshake and the connected client.
#[napi]
pub struct SshCancellation {
    pub(crate) token: CancellationToken,
}

#[napi]
impl SshCancellation {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
        }
    }

    #[napi]
    pub fn cancel(&self) {
        self.token.cancel();
    }

    #[napi(getter)]
    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }
}

pub(crate) fn token(cancellation: Option<&SshCancellation>) -> CancellationToken {
    cancellation.map(|c| c.token.clone()).unwrap_or_default()
}

pub(crate) async fn run<T>(
    cancellation: &CancellationToken,
    operation: impl Future<Output = napi::Result<T>>,
) -> napi::Result<T> {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(napi::Error::new(napi::Status::Cancelled, "SSH operation cancelled")),
        result = operation => result,
    }
}

/// Dropping connect_stream's future alone detaches its background session.
/// Interrupt the actual I/O too, with separate read/write cancellation wakers.
pub(crate) struct CancellationStream<T> {
    inner: T,
    read_cancelled: Pin<Box<WaitForCancellationFutureOwned>>,
    write_cancelled: Pin<Box<WaitForCancellationFutureOwned>>,
}

impl<T> CancellationStream<T> {
    pub(crate) fn new(inner: T, cancellation: CancellationToken) -> Self {
        Self {
            inner,
            read_cancelled: Box::pin(cancellation.clone().cancelled_owned()),
            write_cancelled: Box::pin(cancellation.cancelled_owned()),
        }
    }
    fn error() -> io::Error {
        io::Error::new(io::ErrorKind::ConnectionAborted, "SSH operation cancelled")
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for CancellationStream<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.read_cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(Self::error()));
        }
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for CancellationStream<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.write_cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(Self::error()));
        }
        Pin::new(&mut this.inner).poll_write(cx, data)
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.write_cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(Self::error()));
        }
        Pin::new(&mut this.inner).poll_write_vectored(cx, data)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.write_cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(Self::error()));
        }
        Pin::new(&mut this.inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.write_cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(Self::error()));
        }
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}
