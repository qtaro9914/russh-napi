use std::fmt::Debug;
use std::io::IoSlice;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_http_proxy::http_connect_tokio;
use delegate::delegate;
use napi_derive::napi;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::process::Child;
use tokio::sync::Mutex;
use tokio_socks::tcp::Socks5Stream;

use crate::cancellation::{self, SshCancellation};
use crate::channel::NewSshChannel;

#[napi]
#[derive(Debug)]
pub struct SshTransport(Arc<Mutex<Option<SshTransportInner>>>);

pub(crate) enum SshTransportInner {
    Socket(TcpStream),
    Command(CommandTransport),
    SshChannel(russh::ChannelStream<russh::client::Msg>),
    SocksProxy(tokio_socks::tcp::socks5::Socks5Stream<TcpStream>),
}

impl Debug for SshTransportInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SshTransportInner::Socket(_) => f.debug_struct("Socket").finish(),
            SshTransportInner::Command(_) => f.debug_struct("Command").finish(),
            SshTransportInner::SshChannel(_) => f.debug_struct("SshChannel").finish(),
            SshTransportInner::SocksProxy(_) => f.debug_struct("SocksProxy").finish(),
        }
    }
}

pub(crate) struct CommandTransport {
    child: Option<Child>,
    runtime: tokio::runtime::Handle,
}
impl Drop for CommandTransport {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            // Reap explicitly instead of relying on opportunistic orphan reaping.
            let _ = child.start_kill();
            self.runtime.spawn(async move {
                let _ = child.wait().await;
            });
        }
    }
}

#[napi]
impl SshTransport {
    #[napi]
    pub async fn new_socket(
        address: String,
        cancellation: Option<&SshCancellation>,
    ) -> napi::Result<SshTransport> {
        cancellation::run(&cancellation::token(cancellation), async move {
            let socket = tokio::net::TcpStream::connect(address.clone()).await?;
            socket.set_nodelay(true)?;
            Ok(Self(Arc::new(Mutex::new(Some(SshTransportInner::Socket(
                socket,
            ))))))
        })
        .await
    }

    #[napi]
    pub async fn new_command(
        command: String,
        args: Vec<String>,
        cancellation: Option<&SshCancellation>,
    ) -> napi::Result<SshTransport> {
        cancellation::run(&cancellation::token(cancellation), async move {
            let child = tokio::process::Command::new(command)
                .args(args)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .kill_on_drop(true)
                .spawn()?;

            Ok(Self(Arc::new(Mutex::new(Some(
                SshTransportInner::Command(CommandTransport {
                    child: Some(child),
                    runtime: tokio::runtime::Handle::current(),
                }),
            )))))
        })
        .await
    }

    #[napi]
    pub async fn new_ssh_channel(
        channel: &NewSshChannel,
        cancellation: Option<&SshCancellation>,
    ) -> napi::Result<SshTransport> {
        cancellation::run(&cancellation::token(cancellation), async move {
            let Some(handle) = channel.take().await else {
                return Err(napi::Error::new(
                    napi::Status::GenericFailure,
                    "Channel is already consumed",
                ));
            };

            let stream = handle.into_stream();

            Ok(Self(Arc::new(Mutex::new(Some(
                SshTransportInner::SshChannel(stream),
            )))))
        })
        .await
    }

    #[napi]
    pub async fn new_socks_proxy(
        proxy_host: String,
        proxy_port: u16,
        target_host: String,
        target_port: u16,
        cancellation: Option<&SshCancellation>,
    ) -> napi::Result<SshTransport> {
        cancellation::run(&cancellation::token(cancellation), async move {
            let stream = Socks5Stream::connect(
                (proxy_host.as_ref(), proxy_port),
                (target_host, target_port),
            )
            .await
            .map_err(crate::WrappedError::from)?;

            Ok(Self(Arc::new(Mutex::new(Some(
                SshTransportInner::SocksProxy(stream),
            )))))
        })
        .await
    }

    #[napi]
    pub async fn new_http_proxy(
        proxy_host: String,
        proxy_port: u16,
        target_host: String,
        target_port: u16,
        cancellation: Option<&SshCancellation>,
    ) -> napi::Result<SshTransport> {
        cancellation::run(&cancellation::token(cancellation), async move {
            let mut socket = tokio::net::TcpStream::connect((proxy_host, proxy_port)).await?;
            socket.set_nodelay(true)?;
            http_connect_tokio(&mut socket, target_host.as_str(), target_port)
                .await
                .map_err(crate::WrappedError::from)?;

            Ok(Self(Arc::new(Mutex::new(Some(SshTransportInner::Socket(
                socket,
            ))))))
        })
        .await
    }

    #[napi]
    pub async fn close(&self) {
        drop(self.0.lock().await.take());
    }

    pub(crate) async fn take(&self) -> Option<SshTransportInner> {
        self.0.lock().await.take()
    }
}

impl AsyncRead for SshTransportInner {
    delegate! {
        to match self.get_mut() {
            SshTransportInner::Socket(stream) => Pin::new(stream),
            SshTransportInner::Command(child) => Pin::new(child.child.as_mut().unwrap().stdout.as_mut().unwrap()),
            SshTransportInner::SshChannel(stream) => Pin::new(stream),
            SshTransportInner::SocksProxy(stream) => Pin::new(stream),
        } {
            fn poll_read(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<Result<(), std::io::Error>>;
        }
    }
}

impl AsyncWrite for SshTransportInner {
    delegate! {
        to match self.get_mut() {
            SshTransportInner::Socket(stream) => Pin::new(stream),
            SshTransportInner::Command(child) => Pin::new(child.child.as_mut().unwrap().stdin.as_mut().unwrap()),
            SshTransportInner::SshChannel(stream) => Pin::new(stream),
            SshTransportInner::SocksProxy(stream) => Pin::new(stream),
        } {
            fn poll_write(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<Result<usize, std::io::Error>>;

            fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>>;

            fn poll_write_vectored(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                bufs: &[IoSlice<'_>],
            ) -> Poll<Result<usize, std::io::Error>>;

            fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>>;
        }
    }

    delegate! {
        to match self {
            SshTransportInner::Socket(stream) => Pin::new(stream),
            SshTransportInner::Command(child) => Pin::new(child.child.as_ref().unwrap().stdin.as_ref().unwrap()),
            SshTransportInner::SshChannel(stream) => Pin::new(stream),
            SshTransportInner::SocksProxy(stream) => Pin::new(stream),
        } {
            fn is_write_vectored(&self) -> bool;
        }
    }
}
