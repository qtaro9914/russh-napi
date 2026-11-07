//! Loopback-only integration fixture. Prints its ephemeral port on stdout.
use russh::server::{self, Msg, Server as _, Session};
use russh::{Channel, ChannelId};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

#[derive(Clone)]
struct TestServer;
impl server::Server for TestServer {
    type Handler = Self;
    fn new_client(&mut self, _: Option<std::net::SocketAddr>) -> Self {
        self.clone()
    }
}
impl server::Handler for TestServer {
    type Error = russh::Error;
    async fn auth_none(&mut self, username: &str) -> Result<server::Auth, Self::Error> {
        if username == "stall" {
            std::future::pending::<()>().await;
        }
        Ok(server::Auth::Accept)
    }
    async fn channel_open_session(
        &mut self,
        _: Channel<Msg>,
        _: &mut Session,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
    async fn shell_request(
        &mut self,
        channel: ChannelId,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        session.channel_success(channel)?;
        let sender = session.handle();
        tokio::spawn(async move {
            for i in 0..512 {
                let data = vec![(i % 251) as u8; 16 * 1024];
                let result = if i % 7 == 0 {
                    sender.extended_data(channel, 1, data).await
                } else {
                    sender.data(channel, data).await
                };
                if result.is_err() {
                    return;
                }
            }
            let _ = sender.eof(channel).await;
            let _ = sender.close(channel).await;
        });
        Ok(())
    }
}
#[tokio::main]
async fn main() {
    let config = Arc::new(server::Config {
        auth_rejection_time: Duration::ZERO,
        keys: vec![russh::keys::PrivateKey::random(
            &mut rand::rng(),
            russh::keys::Algorithm::Ed25519,
        )
        .unwrap()],
        ..Default::default()
    });
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    println!("{}", listener.local_addr().unwrap().port());
    TestServer.run_on_socket(config, &listener).await.unwrap();
}
