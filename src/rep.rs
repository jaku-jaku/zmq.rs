use crate::codec::*;
use crate::endpoint::Endpoint;
use crate::error::*;
use crate::fair_queue::{FairQueue, QueueInner};
use crate::peer_map::PeerMap;
use crate::transport::AcceptStopHandle;
use crate::*;
use crate::{SocketType, ZmqResult};

use async_trait::async_trait;
use futures::lock::Mutex as AsyncMutex;
use futures::{SinkExt, StreamExt};
use parking_lot::Mutex;

use std::collections::HashMap;
use std::sync::Arc;

struct RepPeer {
    pub(crate) _identity: PeerIdentity,
    pub(crate) send_queue: Arc<AsyncMutex<ZmqFramedWrite>>,
    pub(crate) connection_id: u64,
}

struct RepSocketBackend {
    pub(crate) peers: PeerMap<RepPeer>,
    fair_queue_inner: Arc<Mutex<QueueInner<ZmqFramedRead, PeerIdentity>>>,
    socket_monitor: Mutex<Option<mpsc::Sender<SocketEvent>>>,
    socket_options: Arc<SocketOptions>,
}

pub struct RepSocket {
    backend: Arc<RepSocketBackend>,
    envelope: Option<ZmqMessage>,
    current_request: Option<PeerIdentity>,
    fair_queue: FairQueue<ZmqFramedRead, PeerIdentity>,
    binds: HashMap<Endpoint, AcceptStopHandle>,
}

impl Drop for RepSocket {
    fn drop(&mut self) {
        self.backend.shutdown();
    }
}

#[async_trait]
impl Socket for RepSocket {
    fn with_options(options: SocketOptions) -> Self {
        let mut fair_queue = FairQueue::new(true);
        let backend = Arc::new(RepSocketBackend {
            peers: PeerMap::default(),
            fair_queue_inner: fair_queue.inner(),
            socket_monitor: Mutex::new(None),
            socket_options: Arc::new(options),
        });

        let backend_weak = Arc::downgrade(&backend);
        fair_queue.set_on_disconnect(move |peer_id: PeerIdentity, connection_id: u64| {
            if let Some(backend) = backend_weak.upgrade() {
                backend.connection_closed(&peer_id, connection_id);
            }
        });

        Self {
            backend,
            envelope: None,
            current_request: None,
            fair_queue,
            binds: HashMap::new(),
        }
    }

    fn backend(&self) -> Arc<dyn MultiPeerBackend> {
        self.backend.clone()
    }

    fn binds(&mut self) -> &mut HashMap<Endpoint, AcceptStopHandle> {
        &mut self.binds
    }

    fn monitor(&mut self) -> mpsc::Receiver<SocketEvent> {
        let (sender, receiver) = mpsc::channel(1024);
        self.backend.socket_monitor.lock().replace(sender);
        receiver
    }
}

#[async_trait]
impl MultiPeerBackend for RepSocketBackend {
    async fn peer_connected(self: Arc<Self>, peer_id: &PeerIdentity, io: FramedIo) {
        let (recv_queue, send_queue) = io.into_parts();
        let connection_id = util::next_connection_id();

        self.peers.insert(
            peer_id.clone(),
            RepPeer {
                _identity: peer_id.clone(),
                send_queue: Arc::new(AsyncMutex::new(send_queue)),
                connection_id,
            },
        );
        self.fair_queue_inner
            .lock()
            .insert_connection(peer_id.clone(), connection_id, recv_queue);
    }

    fn peer_disconnected(&self, peer_id: &PeerIdentity) {
        self.report_disconnected(peer_id);
        self.peers.remove(peer_id);
    }
}

impl RepSocketBackend {
    /// Removes the peer for a closed connection, but only if that connection
    /// is still the registered one.
    fn connection_closed(&self, peer_id: &PeerIdentity, connection_id: u64) {
        if self
            .peers
            .remove_if(peer_id, |peer| peer.connection_id == connection_id)
            .is_some()
        {
            self.report_disconnected(peer_id);
        }
    }

    fn report_disconnected(&self, peer_id: &PeerIdentity) {
        if let Some(monitor) = self.monitor().lock().as_mut() {
            let _ = monitor.try_send(SocketEvent::Disconnected(peer_id.clone()));
        }
    }
}

impl SocketBackend for RepSocketBackend {
    fn socket_type(&self) -> SocketType {
        SocketType::REP
    }

    fn socket_options(&self) -> &Arc<SocketOptions> {
        &self.socket_options
    }

    fn shutdown(&self) {
        self.peers.clear();
    }

    fn monitor(&self) -> &Mutex<Option<mpsc::Sender<SocketEvent>>> {
        &self.socket_monitor
    }
}

#[async_trait]
impl SocketSend for RepSocket {
    async fn send(&mut self, mut message: ZmqMessage) -> ZmqResult<()> {
        match self.current_request.take() {
            Some(peer_id) => {
                let send_queue = self
                    .backend
                    .peers
                    .read(&peer_id, |peer| peer.send_queue.clone());
                let Some(send_queue) = send_queue else {
                    return Err(ZmqError::ReturnToSender {
                        reason: "Client disconnected",
                        message,
                    });
                };
                if let Some(envelope) = self.envelope.take() {
                    message.prepend(&envelope);
                }
                send_queue
                    .lock()
                    .await
                    .send(Message::Message(message))
                    .await?;
                Ok(())
            }
            None => Err(ZmqError::ReturnToSender {
                reason: "Unable to send reply. No request in progress",
                message,
            }),
        }
    }
}

#[async_trait]
impl SocketRecv for RepSocket {
    async fn recv(&mut self) -> ZmqResult<ZmqMessage> {
        loop {
            match self.fair_queue.next().await {
                Some((peer_id, Ok(message))) => match message {
                    Message::Message(mut m) => {
                        if m.len() < 2 {
                            return Err(ZmqError::Other("Invalid message format"));
                        }
                        let mut at = 1;
                        for (index, frame) in m.iter().enumerate() {
                            if frame.is_empty() {
                                // Include delimiter in envelope.
                                at = index + 1;
                                break;
                            }
                        }
                        let data = m.split_off(at);
                        self.envelope = Some(m);
                        self.current_request = Some(peer_id);
                        return Ok(data);
                    }
                    Message::Greeting(_) | Message::Command(_) => {
                        // Ignore non-message frames. REP sockets should only process actual messages.
                    }
                },
                Some((peer_id, Err(e))) => {
                    self.backend.peer_disconnected(&peer_id);
                    return Err(e.into());
                }
                None => {
                    return Err(ZmqError::NoMessage);
                }
            };
        }
    }
}

#[cfg(all(test, feature = "tokio-runtime"))]
mod tests {
    use super::*;
    use std::time::Duration;

    /// The fair queue's disconnect callback runs inside `recv`. It must
    /// remove the peer there and then, without parking the runtime thread.
    #[test]
    fn recv_removes_a_disconnected_peer_without_blocking() {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let mut rep = RepSocket::new();
                let endpoint = rep.bind("tcp://127.0.0.1:0").await.unwrap();
                let mut req = ReqSocket::new();
                req.connect(&endpoint.to_string()).await.unwrap();
                req.send("ping".into()).await.unwrap();
                rep.recv().await.unwrap();
                let peer = rep.current_request.clone().unwrap();
                rep.send("pong".into()).await.unwrap();

                drop(req);
                let _ = tokio::time::timeout(Duration::from_millis(500), rep.recv()).await;
                assert!(
                    !rep.backend.peers.contains(&peer),
                    "recv did not remove the disconnected peer"
                );
            });
            let _ = done_tx.send(());
        });
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("recv blocked after a disconnect");
    }
}
