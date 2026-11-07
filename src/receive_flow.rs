use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use napi_derive::napi;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

const MAX_CONNECTION_BYTES: usize = 8 * 1024 * 1024;
const MAX_PENDING_PACKETS: usize = 1024;

#[derive(Default)]
struct ChannelBudget {
    limit: usize,
    bytes: usize,
    discard: bool,
}
#[derive(Default)]
struct State {
    channels: HashMap<u32, ChannelBudget>,
    packets: HashMap<u32, (u32, usize)>,
    bytes: usize,
    next_ticket: u32,
}

#[napi(object)]
pub struct SshReceiveBufferStats {
    pub pending_bytes: u32,
    pub pending_packets: u32,
    pub managed_channels: u32,
}

/// Opt-in accounting before allocating/enqueuing a JavaScript data callback.
/// A blocked handler pauses the ENTIRE shared SSH connection's packet dispatch,
/// including control packets. ACK, local close and cancel do not take the SSH
/// handle mutex and can release the reader even when it is blocked here.
#[derive(Clone, Default)]
pub(crate) struct ReceiveFlow {
    state: Arc<Mutex<State>>,
    changed: Arc<Notify>,
}

impl ReceiveFlow {
    pub(crate) fn clear(&self) {
        *self.state.lock().unwrap() = State::default();
        self.changed.notify_waiters();
    }
    fn clear_channel(state: &mut State, channel: u32) {
        let mut released = 0;
        state.packets.retain(|_, (id, bytes)| {
            if *id == channel {
                released += *bytes;
                false
            } else {
                true
            }
        });
        state.bytes -= released;
        state.channels.remove(&channel);
    }
    pub(crate) fn configure(&self, channel: u32, limit: u32) -> napi::Result<()> {
        if limit as usize > MAX_CONNECTION_BYTES {
            return Err(napi::Error::new(
                napi::Status::InvalidArg,
                "Receive limit exceeds 8 MiB",
            ));
        }
        let mut state = self.state.lock().unwrap();
        Self::clear_channel(&mut state, channel);
        if limit > 0 {
            state.channels.insert(
                channel,
                ChannelBudget {
                    limit: limit as usize,
                    ..Default::default()
                },
            );
        }
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }
    pub(crate) fn close(&self, channel: u32, discard: bool) {
        let mut state = self.state.lock().unwrap();
        // A remote close already removed this entry. A late/repeated local
        // close must not recreate a tombstone that no peer reply will remove.
        let was_managed = state.channels.contains_key(&channel);
        Self::clear_channel(&mut state, channel);
        if discard && was_managed {
            state.channels.insert(
                channel,
                ChannelBudget {
                    discard: true,
                    ..Default::default()
                },
            );
        }
        drop(state);
        self.changed.notify_waiters();
    }
    pub(crate) fn acknowledge(&self, ticket: u32) -> bool {
        let mut state = self.state.lock().unwrap();
        let Some((channel, bytes)) = state.packets.remove(&ticket) else {
            return false;
        };
        state.bytes -= bytes;
        if let Some(budget) = state.channels.get_mut(&channel) {
            budget.bytes -= bytes;
        }
        drop(state);
        self.changed.notify_waiters();
        true
    }
    pub(crate) fn stats(&self) -> SshReceiveBufferStats {
        let state = self.state.lock().unwrap();
        SshReceiveBufferStats {
            pending_bytes: state.bytes as u32,
            pending_packets: state.packets.len() as u32,
            managed_channels: state.channels.len() as u32,
        }
    }
    /// EOF/close must not overtake already dispatched managed data.
    pub(crate) async fn drain(&self, channel: u32, cancel: &CancellationToken) -> napi::Result<()> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .state
                .lock()
                .unwrap()
                .channels
                .get(&channel)
                .map_or(true, |b| b.bytes == 0)
            {
                return Ok(());
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(napi::Error::new(napi::Status::Cancelled, "SSH operation cancelled")),
                _ = &mut notified => {},
            }
        }
    }
    pub(crate) async fn reserve(
        &self,
        channel: u32,
        length: usize,
        cancel: &CancellationToken,
    ) -> napi::Result<Option<u32>> {
        // Empty packets consume credit too, bounding callback count.
        let charge = length.max(1);
        if charge > MAX_CONNECTION_BYTES {
            return Err(napi::Error::new(
                napi::Status::InvalidArg,
                "SSH data packet exceeds receive budget",
            ));
        }
        loop {
            if cancel.is_cancelled() {
                return Err(napi::Error::new(
                    napi::Status::Cancelled,
                    "SSH operation cancelled",
                ));
            }
            let notified = self.changed.notified();
            tokio::pin!(notified);
            // Register before inspecting state so concurrent ACKs cannot be lost.
            notified.as_mut().enable();
            {
                let mut state = self.state.lock().unwrap();
                let Some(budget) = state.channels.get(&channel) else {
                    return Ok(Some(0));
                };
                if budget.discard {
                    return Ok(None);
                }
                // Permit one packet exceeding a channel's small limit to avoid deadlock.
                if (budget.bytes == 0 || budget.bytes + charge <= budget.limit)
                    && state.bytes + charge <= MAX_CONNECTION_BYTES
                    && state.packets.len() < MAX_PENDING_PACKETS
                {
                    loop {
                        state.next_ticket = state.next_ticket.wrapping_add(1).max(1);
                        if !state.packets.contains_key(&state.next_ticket) {
                            break;
                        }
                    }
                    let ticket = state.next_ticket;
                    state.packets.insert(ticket, (channel, charge));
                    state.bytes += charge;
                    state.channels.get_mut(&channel).unwrap().bytes += charge;
                    return Ok(Some(ticket));
                }
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(napi::Error::new(napi::Status::Cancelled, "SSH operation cancelled")),
                _ = &mut notified => {},
            }
        }
    }
}
