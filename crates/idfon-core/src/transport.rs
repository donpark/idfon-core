use std::{
    collections::HashMap,
    future::Future,
    io,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    sync::{mpsc, RwLock},
};

use idfon_protocol::{
    decode_frame, encode_frame, AckStatus, MessageAck, MessageEnvelope, MAX_FRAME_BYTES,
};
use iroh::{
    endpoint::{presets, Connection, RecvStream, SendStream},
    protocol::ProtocolHandler,
    Endpoint, EndpointAddr, EndpointId, SecretKey,
};
use iroh_gossip::{api::GossipTopic, Gossip, TopicId};
use thiserror::Error;

/// Bounded dial-and-exchange: a first handshake can be aborted by a competing
/// multi-path handshake ("authentication failed" compliance error) and the
/// connection then sits dead until iroh's internal dial gives up ~30 s later —
/// observed as intermittent ~30 s message deliveries whose recovery is always
/// a *fresh* connection. So bound a whole attempt (connect + open + write +
/// ack) and retry it fresh (same lesson as the blob-fetch rework: never trust
/// a stalled attempt to recover in time). A healthy exchange is well under a
/// second; relayed worst case a few seconds.
const EXCHANGE_ATTEMPTS: u32 = 3;
const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(10);

async fn connect_attempts<T, F, Fut>(
    attempts: u32,
    per_try: Duration,
    mut try_once: F,
) -> Result<T, TransportError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, TransportError>>,
{
    let mut error = TransportError::Timeout(per_try);
    for attempt in 0..attempts.max(1) {
        match tokio::time::timeout(per_try, try_once()).await {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(failure)) => error = failure,
            Err(_) => {
                error = TransportError::Timeout(per_try);
            }
        }
        if attempt + 1 < attempts {
            tracing::warn!(target: "idfon.core", attempt, "dial attempt failed; retrying fresh");
        }
    }
    Err(error)
}

pub const MESSAGE_ALPN: &[u8] = crate::alpn::MESSAGE;
pub const SYNC_ALPN: &[u8] = crate::alpn::SYNC;

/// A gossip service attached to an existing Iroh message endpoint. Gossip is
/// only a dissemination path; callers still validate the signed envelope and
/// persist it through the normal message receiver.
pub struct GossipService {
    gossip: Gossip,
    _side_channel: SideChannelGuard,
}

impl GossipService {
    pub fn start(transport: &IrohTransport) -> Self {
        let gossip = Gossip::builder().spawn(transport.endpoint().clone());
        let (tx, mut rx) = mpsc::channel::<Connection>(32);
        let handler = gossip.clone();
        tokio::spawn(async move {
            while let Some(connection) = rx.recv().await {
                let handler = handler.clone();
                tokio::spawn(async move {
                    if let Err(error) = handler.accept(connection).await {
                        tracing::warn!("gossip connection failed: {error}");
                    }
                });
            }
        });
        let side_channel = transport.add_side_channel(iroh_gossip::ALPN, tx);
        Self {
            gossip,
            _side_channel: side_channel,
        }
    }

    pub async fn subscribe(
        &self,
        topic: TopicId,
        bootstrap: Vec<EndpointId>,
    ) -> Result<GossipTopic, iroh_gossip::api::ApiError> {
        self.gossip.subscribe(topic, bootstrap).await
    }
}

/// Stable topic mapping so every daemon derives the same gossip topic from a
/// room id without putting transport-specific data on the wire.
pub fn room_topic(room_id: &str) -> TopicId {
    TopicId::from_bytes(*blake3::hash(room_id.as_bytes()).as_bytes())
}

/// Copies `reader` to `writer` verbatim until EOF, then half-closes the writer.
/// Used by the byte-stream side channels (MCP bridge, daemon relay); it does not
/// parse, re-frame, or re-serialize the bytes.
pub async fn pump<R, W>(mut reader: R, mut writer: W) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    tokio::io::copy(&mut reader, &mut writer).await?;
    writer.shutdown().await
}

/// Unregisters a side-channel forwarder when dropped (see
/// [`IrohTransport::add_side_channel`]).
pub struct SideChannelGuard {
    alpn: Vec<u8>,
    channels: Arc<Mutex<HashMap<Vec<u8>, mpsc::Sender<Connection>>>>,
}

impl Drop for SideChannelGuard {
    fn drop(&mut self) {
        self.channels
            .lock()
            .expect("side channels poisoned")
            .remove(&self.alpn);
    }
}

#[derive(Debug, Error)]
pub enum TransportError {
    /// The endpoint was closed (shutdown or rebind); an accept loop ends.
    #[error("endpoint closed")]
    Closed,
    /// Every dial attempt timed out.
    #[error("connect timed out after {0:?}")]
    Timeout(Duration),
    #[error("encoded message is too large")]
    MessageTooLarge,
    /// Anything else, with the underlying error's text.
    #[error("transport failed: {0}")]
    Failed(String),
}

pub type SendFuture<'a> =
    Pin<Box<dyn Future<Output = Result<MessageAck, TransportError>> + Send + 'a>>;

pub trait MessageTransport: Send + Sync {
    fn send<'a>(&'a self, target: &'a EndpointAddr, message: &'a MessageEnvelope)
        -> SendFuture<'a>;
}

/// Deterministic transport used by daemon tests. It models successful remote
/// receipt without requiring sockets, discovery, or media dependencies.
#[derive(Clone, Default)]
pub struct FakeTransport {
    sent: Arc<Mutex<Vec<MessageEnvelope>>>,
    /// Every send with its target endpoint id, for tests that route between stores.
    sent_to: Arc<Mutex<Vec<(String, MessageEnvelope)>>>,
    endpoint_tickets: Arc<Mutex<std::collections::HashMap<String, Vec<u8>>>>,
}

impl FakeTransport {
    pub fn messages(&self) -> Vec<MessageEnvelope> {
        self.sent.lock().expect("fake transport poisoned").clone()
    }

    /// Take every send so far, with its target endpoint id.
    pub fn take_sent_to(&self) -> Vec<(String, MessageEnvelope)> {
        std::mem::take(&mut *self.sent_to.lock().expect("fake transport poisoned"))
    }

    /// Test double for `endpoint_ticket`: what `endpoint_ticket_for` returns
    /// for this identity.
    pub fn set_endpoint_ticket(&self, identity: &str, ticket: Vec<u8>) {
        self.endpoint_tickets
            .lock()
            .expect("fake transport poisoned")
            .insert(identity.to_string(), ticket);
    }

    pub fn endpoint_ticket(&self, identity: &str) -> Option<Vec<u8>> {
        self.endpoint_tickets
            .lock()
            .expect("fake transport poisoned")
            .get(identity)
            .cloned()
    }
}

impl MessageTransport for FakeTransport {
    fn send<'a>(
        &'a self,
        target: &'a EndpointAddr,
        message: &'a MessageEnvelope,
    ) -> SendFuture<'a> {
        Box::pin(async move {
            if encode_frame(message)
                .map_err(|_| TransportError::MessageTooLarge)?
                .len()
                > MAX_FRAME_BYTES + 4
            {
                return Err(TransportError::MessageTooLarge);
            }
            self.sent
                .lock()
                .expect("fake transport poisoned")
                .push(message.clone());
            self.sent_to
                .lock()
                .expect("fake transport poisoned")
                .push((target.id.to_string(), message.clone()));
            Ok(MessageAck {
                message_id: message.message_id.clone(),
                status: AckStatus::Accepted,
            })
        })
    }
}

/// Real reliable Iroh stream transport. The remote application protocol is
/// responsible for authentication and its delivery acknowledgment.
pub struct IrohTransport {
    endpoint: Endpoint,
    alpn: Vec<u8>,
    /// Side-channel ALPN forwarders: inbound connections whose ALPN matches
    /// a registered entry are handed to the sender instead of the message
    /// path (e.g. MoQ sessions for 1:1 live calls).
    side_channels: Arc<Mutex<HashMap<Vec<u8>, mpsc::Sender<Connection>>>>,
    gossip: Arc<Mutex<Option<Arc<GossipService>>>>,
}

impl IrohTransport {
    /// Binds with `key` (random when `None`) on `port` (ephemeral when
    /// `None`). Public callers use [`IrohTransport::builder`].
    pub(crate) async fn bind_with(
        key: Option<[u8; 32]>,
        port: Option<u16>,
    ) -> Result<Self, TransportError> {
        match Self::try_bind(key, port).await {
            Ok(transport) => Ok(transport),
            // A pinned port can be taken (another profile, a stale process);
            // fall back to ephemeral rather than fail to start.
            Err(error) if port.is_some() => {
                tracing::warn!(target: "idfon.core", port = ?port, error = %error, "endpoint port unavailable; binding ephemeral");
                Self::try_bind(key, None).await
            }
            Err(error) => Err(error),
        }
    }

    async fn try_bind(key: Option<[u8; 32]>, port: Option<u16>) -> Result<Self, TransportError> {
        // A self-hosted/enterprise relay overrides N0 (relay selection:
        // parameter > configuration > default, docs/relay-config.md). A relay
        // is the fallback when direct UDP is impossible (enterprise NAT); its
        // WebSocket transport uses TCP 443, which such networks allow.
        let relay = crate::relay::resolve();
        // BBR3, as iroh-live's MoQ preset sets: call media rides this endpoint
        // (one endpoint per device), and messages and blobs do fine on it.
        let transport_config = iroh::endpoint::QuicTransportConfig::builder()
            .congestion_controller_factory(std::sync::Arc::new(
                noq_proto::congestion::Bbr3Config::default(),
            ))
            .build();
        let mut builder = crate::relay::apply(
            Endpoint::builder(presets::N0)
                .alpns(vec![MESSAGE_ALPN.to_vec()])
                .transport_config(transport_config),
            &relay,
        );
        if let Some(key) = key {
            builder = builder.secret_key(SecretKey::from_bytes(&key));
        }
        if let Some(port) = port {
            builder = builder
                .clear_ip_transports()
                .bind_addr(SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)))
                .map_err(|error| TransportError::Failed(error.to_string()))?
                .bind_addr(SocketAddr::from((Ipv6Addr::UNSPECIFIED, port)))
                .map_err(|error| TransportError::Failed(error.to_string()))?;
        }
        // Relay-only (enterprise NAT: direct UDP unavailable). Last, so it also
        // drops the pinned-port IP transports above; the endpoint then
        // publishes no IP addrs, forcing peers onto the relay.
        if relay.only {
            builder = builder.clear_ip_transports();
        }
        let endpoint = builder
            .bind()
            .await
            .map_err(|error| TransportError::Failed(error.to_string()))?;
        Ok(Self {
            endpoint,
            alpn: MESSAGE_ALPN.to_vec(),
            side_channels: Arc::new(Mutex::new(HashMap::new())),
            gossip: Arc::new(Mutex::new(None)),
        })
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Registers a side-channel ALPN: inbound connections speaking `alpn`
    /// are forwarded to `sender`. The ALPN is added to the endpoint's
    /// accepted set immediately; dropping the returned guard unregisters
    /// the forwarder (the ALPN stays accepted — re-register on next use).
    pub fn enable_gossip(&self) -> Arc<GossipService> {
        let mut slot = self.gossip.lock().expect("gossip state poisoned");
        if let Some(service) = slot.as_ref() {
            return service.clone();
        }
        let service = Arc::new(GossipService::start(self));
        *slot = Some(service.clone());
        service
    }

    pub fn add_side_channel(
        &self,
        alpn: &[u8],
        sender: mpsc::Sender<Connection>,
    ) -> SideChannelGuard {
        {
            let mut channels = self.side_channels.lock().expect("side channels poisoned");
            channels.insert(alpn.to_vec(), sender);
            let mut accepted = vec![MESSAGE_ALPN.to_vec()];
            accepted.extend(channels.keys().cloned());
            self.endpoint.set_alpns(accepted);
        }
        SideChannelGuard {
            alpn: alpn.to_vec(),
            channels: self.side_channels.clone(),
        }
    }

    pub async fn shutdown(self) {
        self.endpoint.close().await;
    }

    /// Dials `target` on an arbitrary ALPN and opens one bi-stream. The message
    /// path's `send` hardcodes [`MESSAGE_ALPN`]; side-channel consumers (e.g. the
    /// MCP bridge) use this to speak their own ALPN. Byte framing is the
    /// caller's concern.
    pub async fn open_bi_stream(
        &self,
        target: &EndpointAddr,
        alpn: &[u8],
    ) -> Result<(Connection, SendStream, RecvStream), TransportError> {
        let (connection, send, recv) =
            connect_attempts(EXCHANGE_ATTEMPTS, EXCHANGE_TIMEOUT, || async {
                let connection = self
                    .endpoint
                    .connect(target.clone(), alpn)
                    .await
                    .map_err(|error| TransportError::Failed(error.to_string()))?;
                let (send, recv) = connection
                    .open_bi()
                    .await
                    .map_err(|error| TransportError::Failed(error.to_string()))?;
                Ok((connection, send, recv))
            })
            .await?;
        Ok((connection, send, recv))
    }

    /// Accepts authenticated message frames and returns application acknowledgments.
    pub async fn serve<F, Fut>(&self, handler: F) -> Result<(), TransportError>
    where
        F: Fn(MessageEnvelope) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<MessageAck, TransportError>> + Send + 'static,
    {
        self.serve_with_peer(move |message, _remote_id| handler(message))
            .await
    }

    /// Like [`Self::serve`], but also gives the handler the sender's dial
    /// address as this endpoint knows it after the handshake: the
    /// authenticated remote id plus the paths iroh has for it. A receiver
    /// can use it to answer a sender it has no record of (contact requests,
    /// docs/protocol.md).
    pub async fn serve_with_remote_addr<F, Fut>(&self, handler: F) -> Result<(), TransportError>
    where
        F: Fn(MessageEnvelope, EndpointAddr) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<MessageAck, TransportError>> + Send + 'static,
    {
        let endpoint = self.endpoint.clone();
        self.serve_with_peer(move |message, remote_id| {
            let endpoint = endpoint.clone();
            let handler = handler.clone();
            async move {
                let address = match endpoint.remote_info(remote_id).await {
                    Some(info) => EndpointAddr::from_parts(
                        remote_id,
                        info.into_addrs().map(|address| address.into_addr()),
                    ),
                    None => EndpointAddr::new(remote_id),
                };
                handler(message, address).await
            }
        })
        .await
    }

    /// Like [`Self::serve`], but also gives the handler Iroh's authenticated
    /// remote endpoint ID. Protocols that carry a signed endpoint ID should
    /// compare it with this value before accepting the message.
    pub async fn serve_with_peer<F, Fut>(&self, handler: F) -> Result<(), TransportError>
    where
        F: Fn(MessageEnvelope, EndpointId) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<MessageAck, TransportError>> + Send + 'static,
    {
        loop {
            let incoming = self
                .endpoint
                .accept()
                .await
                .ok_or(TransportError::Closed)?;
            let handler = handler.clone();
            let alpn = self.alpn.clone();
            let side_channels = self.side_channels.clone();
            // The handshake is awaited inside the task: a half-open or
            // abandoned handshake (stray UDP, a competing multi-path dial)
            // must not head-of-line block every later connection for its full
            // ~30 s timeout — that was the intermittent 30 s message stall.
            tokio::spawn(async move {
                let connection = match incoming.await {
                    Ok(connection) => connection,
                    Err(error) => {
                        // Expected noise, per iroh's Incoming::accept docs: the
                        // QUIC socket receives unsolicited datagrams that abort
                        // handshakes. Sends retry, so delivery is unaffected.
                        tracing::warn!(target: "idfon.core", error = %error, "message connection handshake aborted (background UDP noise)");
                        return;
                    }
                };
                let remote_id = connection.remote_id();
                // Side-channel ALPNs (e.g. live-call MoQ sessions) are handed to
                // whoever registered them; nobody home → close. They never reach
                // the message path below (which owns the connection).
                let forwarder = side_channels
                    .lock()
                    .expect("side channels poisoned")
                    .get(connection.alpn())
                    .cloned();
                if let Some(sender) = forwarder {
                    if sender.send(connection).await.is_err() {
                        tracing::warn!("side channel closed, inbound connection dropped");
                    }
                    return;
                }
                let result = async {
                    if connection.alpn() != alpn {
                        return Err(TransportError::Failed("ALPN mismatch".into()));
                    }
                    let (mut send, mut recv) = connection
                        .accept_bi()
                        .await
                        .map_err(|error| TransportError::Failed(error.to_string()))?;
                    let frame = recv
                        .read_to_end(MAX_FRAME_BYTES + 4)
                        .await
                        .map_err(|error| TransportError::Failed(error.to_string()))?;
                    let message: MessageEnvelope = decode_frame(&frame).map_err(|error| {
                        TransportError::Failed(format!("invalid message: {error}"))
                    })?;
                    let ack = handler(message, remote_id).await?;
                    let ack_frame =
                        encode_frame(&ack).map_err(|_| TransportError::MessageTooLarge)?;
                    send.write_all(&ack_frame)
                        .await
                        .map_err(|error| TransportError::Failed(error.to_string()))?;
                    send.finish()
                        .map_err(|error| TransportError::Failed(error.to_string()))?;
                    connection.closed().await;
                    Ok::<(), TransportError>(())
                }
                .await;
                if let Err(error) = result {
                    tracing::warn!(target: "idfon.core", error = %error, "message protocol failed");
                    tracing::warn!("message protocol failed: {error}");
                    connection.close(1u8.into(), b"message rejected");
                }
            });
        }
    }
}

impl IrohTransport {
    /// The front door: `IrohTransport::builder().secret_key(k).spawn().await?`
    /// binds an endpoint and keeps it accepting, so protocols registered on
    /// it (`idfon_h1::serve_router`, `idfon_h3`, `idfon_channel`, MCP) get
    /// their connections without a hand-run accept loop.
    pub fn builder() -> NodeBuilder {
        NodeBuilder::default()
    }
}

/// Options for binding an endpoint. Nothing here reads the environment.
#[derive(Debug, Default, Clone)]
pub struct NodeBuilder {
    key: Option<[u8; 32]>,
    port: Option<u16>,
}

impl NodeBuilder {
    /// The endpoint's secret key (its identity). Default: a fresh random key.
    pub fn secret_key(mut self, key: [u8; 32]) -> Self {
        self.key = Some(key);
        self
    }

    /// The endpoint's identity (same as `secret_key(identity.secret_bytes())`).
    pub fn identity(self, identity: &crate::IdentityKey) -> Self {
        self.secret_key(identity.secret_bytes())
    }

    /// Pin the UDP port so the address survives restarts (falls back to an
    /// ephemeral port if taken). Default: ephemeral.
    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// Pin the port from the `IDFON_ENDPOINT_PORT` setting (env var, else
    /// `endpoint.port` in `~/.idfon/config.toml`), if set. Opt-in: the
    /// builder reads no configuration unless asked.
    pub fn port_from_config(mut self) -> Self {
        if let Some(port) = idfon_config::get("IDFON_ENDPOINT_PORT").and_then(|v| v.trim().parse().ok()) {
            self.port = Some(port);
        }
        self
    }

    /// Bind only. You then run [`IrohTransport::serve`] (or
    /// [`IrohTransport::serve_with_peer`]) yourself.
    pub async fn bind(self) -> Result<IrohTransport, TransportError> {
        IrohTransport::bind_with(self.key, self.port).await
    }

    /// Bind and accept in the background. Message frames are refused; use
    /// [`Self::spawn_with`] to handle them.
    pub async fn spawn(self) -> Result<Node, TransportError> {
        self.spawn_with(|_message, _remote| async {
            Err::<MessageAck, _>(TransportError::Failed("this node accepts no messages".into()))
        })
        .await
    }

    /// Bind and accept in the background, handing message frames to
    /// `handler` (with the authenticated remote endpoint id).
    pub async fn spawn_with<F, Fut>(self, handler: F) -> Result<Node, TransportError>
    where
        F: Fn(MessageEnvelope, EndpointId) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<MessageAck, TransportError>> + Send + 'static,
    {
        let transport = Arc::new(self.bind().await?);
        let serving = Arc::clone(&transport);
        let task = tokio::spawn(async move {
            if let Err(error) = serving.serve_with_peer(handler).await {
                tracing::debug!(target: "idfon.core", error = %error, "node accept loop ended");
            }
        });
        Ok(Node { transport, task })
    }
}

/// A bound endpoint with its accept loop running. Derefs to
/// [`IrohTransport`]; [`Node::transport`] gives the `Arc` that protocol
/// crates take. Dropping it stops accepting (the endpoint itself closes
/// when the last `Arc` goes).
pub struct Node {
    transport: Arc<IrohTransport>,
    task: tokio::task::JoinHandle<()>,
}

impl Node {
    pub fn transport(&self) -> &Arc<IrohTransport> {
        &self.transport
    }
}

impl std::ops::Deref for Node {
    type Target = IrohTransport;

    fn deref(&self) -> &IrohTransport {
        &self.transport
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Owns the active endpoint and swaps it only after the replacement is bound.
pub struct TransportManager {
    current: RwLock<HashMap<String, Arc<IrohTransport>>>,
}

impl TransportManager {
    /// Binds the default identity's endpoint with `key` on `port` (pinned
    /// so paired apps keep its address; `None` = ephemeral). Additional
    /// identities always bind ephemeral, so a pinned port cannot conflict.
    pub async fn bind(key: Option<[u8; 32]>, port: Option<u16>) -> Result<Self, TransportError> {
        let mut transports = HashMap::new();
        transports.insert(
            "default".into(),
            Arc::new(IrohTransport::bind_with(key, port).await?),
        );
        Ok(Self {
            current: RwLock::new(transports),
        })
    }

    pub fn endpoint_id(&self, identity: &str) -> Option<String> {
        self.current
            .try_read()
            .ok()?
            .get(identity)
            .map(|transport| transport.endpoint().id().to_string())
    }

    pub fn endpoint_ticket(&self, identity: &str) -> Option<Vec<u8>> {
        serde_json::to_vec(
            &self
                .current
                .try_read()
                .ok()?
                .get(identity)?
                .endpoint()
                .addr(),
        )
        .ok()
    }

    pub async fn current(&self, identity: &str) -> Option<Arc<IrohTransport>> {
        self.current.read().await.get(identity).cloned()
    }

    pub async fn add_identity(
        &self,
        identity: &str,
        key: [u8; 32],
    ) -> Result<String, TransportError> {
        // Extra identities stay ephemeral: the pinned port belongs to the
        // default identity only.
        let transport = Arc::new(IrohTransport::bind_with(Some(key), None).await?);
        let endpoint_id = transport.endpoint().id().to_string();
        self.current
            .write()
            .await
            .insert(identity.into(), transport);
        Ok(endpoint_id)
    }

    pub async fn serve<F, Fut>(&self, identity: &str, handler: F) -> Result<(), TransportError>
    where
        F: Fn(MessageEnvelope) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<MessageAck, TransportError>> + Send + 'static,
    {
        loop {
            let current = self
                .current(identity)
                .await
                .ok_or_else(|| TransportError::Failed("identity transport not found".into()))?;
            match current.serve(handler.clone()).await {
                // A rebind closed the old endpoint: serve the new one.
                Err(TransportError::Closed) => continue,
                result => return result,
            }
        }
    }

    /// [`Self::serve`] with the sender's dial address
    /// ([`IrohTransport::serve_with_remote_addr`]).
    pub async fn serve_with_remote_addr<F, Fut>(
        &self,
        identity: &str,
        handler: F,
    ) -> Result<(), TransportError>
    where
        F: Fn(MessageEnvelope, EndpointAddr) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<MessageAck, TransportError>> + Send + 'static,
    {
        loop {
            let current = self
                .current(identity)
                .await
                .ok_or_else(|| TransportError::Failed("identity transport not found".into()))?;
            match current.serve_with_remote_addr(handler.clone()).await {
                // A rebind closed the old endpoint: serve the new one.
                Err(TransportError::Closed) => continue,
                result => return result,
            }
        }
    }

    pub async fn rebind(
        &self,
        identity: &str,
        key: Option<[u8; 32]>,
    ) -> Result<String, TransportError> {
        let replacement = Arc::new(IrohTransport::bind_with(key, None).await?);
        let id = replacement.endpoint().id().to_string();
        let old = self
            .current
            .write()
            .await
            .insert(identity.into(), replacement);
        if let Some(old) = old {
            old.endpoint().close().await;
        }
        Ok(id)
    }

    pub async fn send(
        &self,
        identity: &str,
        target: &EndpointAddr,
        message: &MessageEnvelope,
    ) -> Result<MessageAck, TransportError> {
        self.current(identity)
            .await
            .ok_or_else(|| TransportError::Failed("identity transport not found".into()))?
            .send(target, message)
            .await
    }
}

impl MessageTransport for IrohTransport {
    fn send<'a>(
        &'a self,
        target: &'a EndpointAddr,
        message: &'a MessageEnvelope,
    ) -> SendFuture<'a> {
        Box::pin(async move {
            let frame = encode_frame(message).map_err(|_| TransportError::MessageTooLarge)?;
            // One bounded attempt = connect + open + write + ack read (see
            // [`connect_attempts`] for why a bare attempt is unsafe).
            connect_attempts(EXCHANGE_ATTEMPTS, EXCHANGE_TIMEOUT, || async {
                let started = std::time::Instant::now();
                let connection = self
                    .endpoint
                    .connect(target.clone(), &self.alpn)
                    .await
                    .map_err(|error| TransportError::Failed(error.to_string()))?;
                let connect_ms = started.elapsed().as_millis() as u64;
                let (mut send, mut recv) = connection
                    .open_bi()
                    .await
                    .map_err(|error| TransportError::Failed(error.to_string()))?;
                send.write_all(&frame)
                    .await
                    .map_err(|error| TransportError::Failed(error.to_string()))?;
                send.finish()
                    .map_err(|error| TransportError::Failed(error.to_string()))?;
                let write_ms = started.elapsed().as_millis() as u64;
                let ack_frame = recv
                    .read_to_end(MAX_FRAME_BYTES)
                    .await
                    .map_err(|error| TransportError::Failed(error.to_string()))?;
                let ack: MessageAck = decode_frame(&ack_frame).map_err(|error| {
                    TransportError::Failed(format!("invalid acknowledgment: {error}"))
                })?;
                if ack.message_id != message.message_id {
                    return Err(TransportError::Failed(
                        "acknowledgment message ID mismatch".into(),
                    ));
                }
                let total_ms = started.elapsed().as_millis() as u64;
                tracing::debug!(
                    target: "idfon.core",
                    message_id = %message.message_id,
                    connect_ms,
                    write_ms,
                    total_ms,
                    "message exchange timings"
                );
                connection.close(0u8.into(), b"message sent");
                Ok(ack)
            })
            .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use idfon_protocol::{MessageContent, PeerAuth};

    #[test]
    fn connect_attempts_gives_up_fast_when_dials_hang() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let started = std::time::Instant::now();
            let result: Result<u8, TransportError> =
                connect_attempts(2, Duration::from_millis(50), std::future::pending).await;
            assert!(result.is_err());
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "a hung dial must not stall for iroh's internal retry"
            );
        });
    }

    #[test]
    fn connect_attempts_retries_after_a_hung_dial() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let mut calls = 0u32;
            let result: Result<u8, TransportError> =
                connect_attempts(3, Duration::from_millis(50), || {
                    calls += 1;
                    async move {
                        if calls == 1 {
                            std::future::pending::<()>().await;
                        }
                        Ok(7)
                    }
                })
                .await;
            assert_eq!(result.unwrap(), 7);
        });
    }

    #[test]
    fn transport_manager_rebinds_to_a_new_endpoint() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let manager = TransportManager::bind(Some([1; 32]), None)
                .await
                .unwrap();
            let first = manager.endpoint_id("default").unwrap();
            let second = manager.rebind("default", Some([2; 32])).await.unwrap();
            assert_ne!(first, second);
            manager.rebind("default", Some([1; 32])).await.unwrap();
        });
    }

    #[test]
    fn iroh_transport_round_trips_a_message_ack() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let receiver = IrohTransport::builder().bind().await.unwrap();
            let sender = IrohTransport::builder().bind().await.unwrap();
            let receiver = std::sync::Arc::new(receiver);
            receiver.endpoint().online().await;
            sender.endpoint().online().await;
            let task_receiver = std::sync::Arc::clone(&receiver);
            let task = tokio::spawn(async move {
                task_receiver
                    .serve(|message| async move {
                        Ok(MessageAck {
                            message_id: message.message_id,
                            status: AckStatus::Accepted,
                        })
                    })
                    .await
            });
            let message = MessageEnvelope {
                message_id: "msg-iroh".into(),
                sender: PeerAuth {
                    peer_id: "peer".into(),
                    endpoint_id: "endpoint".into(),
                    signature: "sig".into(),
                },
                content: MessageContent::Text {
                    text: "hello".into(),
                },
                idempotency_key: "key-iroh".into(),
                capability_ticket: None,
                conversation: None,
                trace: None,
                telemetry: None,
                context: None,
                introduction: None,
            };
            let ack = sender
                .send(&receiver.endpoint().addr(), &message)
                .await
                .unwrap();
            assert_eq!(ack.message_id, message.message_id);
            assert_eq!(ack.status, AckStatus::Accepted);
            task.abort();
            let _ = task.await;
            sender.endpoint().close().await;
            receiver.endpoint().close().await;
        });
    }

    #[test]
    fn room_topics_are_stable_and_room_scoped() {
        assert_eq!(room_topic("room-a"), room_topic("room-a"));
        assert_ne!(room_topic("room-a"), room_topic("room-b"));
    }

    #[test]
    fn fake_transport_records_authenticated_messages() {
        let transport = FakeTransport::default();
        let message = MessageEnvelope {
            message_id: "msg-1".into(),
            sender: PeerAuth {
                peer_id: "peer".into(),
                endpoint_id: "endpoint".into(),
                signature: "sig".into(),
            },
            content: MessageContent::Text {
                text: "hello".into(),
            },
            idempotency_key: "key-1".into(),
            capability_ticket: None,
            conversation: None,
            trace: None,
            telemetry: None,
            context: None,
            introduction: None,
        };
        let target = EndpointAddr::from_parts(
            "0000000000000000000000000000000000000000000000000000000000000000"
                .parse()
                .unwrap(),
            vec![],
        );
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(transport.send(&target, &message))
            .unwrap();
        assert_eq!(transport.messages(), vec![message]);
    }
}
