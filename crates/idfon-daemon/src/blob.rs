use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, OnceLock,
    },
};

use idfon_core::transport::{IrohTransport, SideChannelGuard};
use iroh::protocol::{ProtocolHandler, Router};
use iroh::{address_lookup::memory::MemoryLookup, Endpoint};
use iroh_blobs::{
    store::fs::FsStore, ticket::BlobTicket, BlobFormat, BlobsProtocol, ALPN as BLOBS_ALPN,
};

/// The daemon's blob store, served on identity endpoints. Blobs ride the
/// identity endpoint (a side channel on `BLOBS_ALPN`) instead of an endpoint
/// of their own: a relay in webhook mode (idfon-edge docs/relay-auth.md)
/// admits only known endpoint ids, and a random-key blob endpoint never is.
struct BlobProvider {
    store: FsStore,
    protocol: BlobsProtocol,
    /// Identity transports serving `BLOBS_ALPN`, by endpoint id. The transport
    /// is kept to notice a rebind (a new transport for the same id).
    served: HashMap<iroh::EndpointId, (Arc<IrohTransport>, SideChannelGuard)>,
    /// Fallback provider endpoint for the fake transport (tests,
    /// `--transport fake`), which has no identity endpoint to ride.
    fallback: Option<(Endpoint, Router)>,
}

/// One provider per store root (the daemon has one; tests each bring their own).
static PROVIDERS: OnceLock<tokio::sync::Mutex<HashMap<PathBuf, BlobProvider>>> = OnceLock::new();

/// Shared runtime for the blocking `run_*` entry points. The blob provider's
/// endpoint/router tasks live on this runtime; a runtime dropped after
/// `block_on` returns would tear them down and leave the blob unservable.
static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("tokio runtime"))
}

/// Forward `BLOBS_ALPN` connections on `transport` to the blob protocol.
fn serve_on(provider: &mut BlobProvider, transport: &Arc<IrohTransport>) {
    let id = transport.endpoint().id();
    if let Some((served, _)) = provider.served.get(&id) {
        if Arc::ptr_eq(served, transport) {
            return;
        }
    }
    let (tx, mut rx) = tokio::sync::mpsc::channel::<iroh::endpoint::Connection>(8);
    let guard = transport.add_side_channel(BLOBS_ALPN, tx);
    let protocol = provider.protocol.clone();
    runtime().spawn(async move {
        while let Some(connection) = rx.recv().await {
            let protocol = protocol.clone();
            tokio::spawn(async move {
                let _ = protocol.accept(connection).await;
            });
        }
    });
    provider.served.insert(id, (transport.clone(), guard));
}

/// Adds `data` to the store and returns a ticket that names `transport`'s
/// endpoint (the identity endpoint), or a fallback endpoint without one.
pub async fn put(
    root: PathBuf,
    data: Vec<u8>,
    transport: Option<Arc<IrohTransport>>,
) -> anyhow::Result<(BlobTicket, usize)> {
    // One store per process, created lazily and reused. It locks blobs.db:
    // opening a second FsStore on the same root while this one is alive
    // deadlocks, and the store must stay alive anyway to serve fetchers. The
    // lock is held across add_slice to serialize store access.
    let mut providers = PROVIDERS
        .get_or_init(|| tokio::sync::Mutex::new(HashMap::new()))
        .lock()
        .await;
    if !providers.contains_key(&root) {
        let store = FsStore::load(&root).await?;
        let protocol = BlobsProtocol::new(store.as_ref(), None);
        providers.insert(
            root.clone(),
            BlobProvider {
                store,
                protocol,
                served: HashMap::new(),
                fallback: None,
            },
        );
    }
    let provider = providers.get_mut(&root).expect("provider just set");
    let endpoint = match &transport {
        Some(transport) => {
            serve_on(provider, transport);
            transport.endpoint().clone()
        }
        None => match &provider.fallback {
            Some((endpoint, _)) => endpoint.clone(),
            None => {
                let endpoint = idfon_core::relay::endpoint_builder().bind().await?;
                let router = Router::builder(endpoint.clone())
                    .accept(BLOBS_ALPN, provider.protocol.clone())
                    .spawn();
                provider.fallback = Some((endpoint.clone(), router));
                endpoint
            }
        },
    };
    let content = provider
        .store
        .blobs()
        .add_slice(&data)
        .with_named_tag(format!("resource-{}", blake3::hash(&data)))
        .await?;
    // The ticket carries the address, so it must be minted after the endpoint
    // knows one (same wait as the other bind sites). A ticket built mid-connect
    // has no route and forces the receiver into the retry loop in `fetch`.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), endpoint.online()).await;
    let ticket = BlobTicket::new(endpoint.addr(), content.hash, content.format);
    Ok((ticket, data.len()))
}

/// Fetches `ticket` into `root`, dialing from `endpoint` (the requesting
/// identity's endpoint) or, without one, from a throwaway endpoint.
pub async fn fetch(
    ticket: String,
    root: PathBuf,
    endpoint: Option<Endpoint>,
) -> anyhow::Result<PathBuf> {
    let ticket: BlobTicket = ticket.parse()?;
    // A collection ticket (what `sendme send` prints) names a HashSeq, not a
    // file: `export` would write the 32-byte entries. Refuse instead of
    // handing the caller that as the payload.
    if ticket.format() != BlobFormat::Raw {
        anyhow::bail!(
            "ticket names a {:?} blob; only single-file (Raw) tickets are supported",
            ticket.format()
        );
    }
    let output = root.join(format!("{}.blob", ticket.hash()));
    // A completed transfer is the cache for all later chunk requests: check it
    // before dialing or opening the store, so a cache hit costs one local read
    // and no network, no blobs.db lock, and no retry sleep.
    if cache_matches(&output, ticket.hash()) {
        return Ok(output);
    }
    // The ticket carries the provider's own addresses (direct and relay), and
    // `connect` dials them as given, so the fetch does not depend on pkarr
    // having propagated. Retries cover a provider that is still coming online.
    // ponytail: 6 attempts x 1s.
    let mut last = None;
    for attempt in 0..6 {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        let result = match &endpoint {
            Some(endpoint) => fetch_with(&ticket, endpoint, &root, &output).await,
            None => {
                let lookup = MemoryLookup::from_endpoint_info([ticket.addr().clone()]);
                let endpoint = idfon_core::relay::endpoint_builder()
                    .address_lookup(lookup)
                    .bind()
                    .await?;
                let result = fetch_with(&ticket, &endpoint, &root, &output).await;
                endpoint.close().await;
                result
            }
        };
        match result {
            Ok(path) => return Ok(path),
            Err(error) => last = Some(error),
        }
    }
    Err(last.expect("at least one attempt"))
}

/// True only for a cache file whose bytes hash to the blob's hash. Exports can
/// be interrupted, and older builds wrote the target non-atomically, so the
/// file's presence alone proves nothing.
fn cache_matches(path: &Path, hash: iroh_blobs::Hash) -> bool {
    use std::io::Read;
    let Ok(mut file) = std::fs::File::open(path) else {
        return false;
    };
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        match file.read(&mut buf) {
            Ok(0) => return hasher.finalize().as_bytes() == hash.as_bytes(),
            Ok(n) => {
                hasher.update(&buf[..n]);
            }
            Err(_) => return false,
        }
    }
}

/// Waits until a direct IP path is selected, or `limit` elapses. The relay
/// path already works; this only decides whether the transfer starts before or
/// after hole punching wins.
async fn wait_for_direct_path(
    conn: &iroh::endpoint::Connection,
    limit: std::time::Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        if conn
            .paths()
            .iter()
            .any(|path| path.is_ip() && path.is_selected())
        {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

async fn fetch_with(
    ticket: &BlobTicket,
    endpoint: &Endpoint,
    root: &PathBuf,
    output: &Path,
) -> anyhow::Result<PathBuf> {
    static NEXT_TMP: AtomicU64 = AtomicU64::new(0);
    let store = FsStore::load(root).await?;
    let result = async {
        // Dial, then give hole punching a moment before the transfer starts:
        // a direct path found here keeps a short transfer off the relay.
        let conn = endpoint.connect(ticket.addr().clone(), BLOBS_ALPN).await?;
        let direct = wait_for_direct_path(&conn, std::time::Duration::from_secs(2)).await;
        tracing::debug!(target: "idfon.blob", direct, "blob fetch connected");
        // The transfer has no internal timeout; a stuck connection must not
        // hang the daemon request (and worker) indefinitely.
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            store.remote().fetch(conn.clone(), ticket.hash()),
        )
        .await??;
        // Copy the verified blob out ourselves instead of `export`: write to a
        // unique temp file, fsync, then rename. A reader therefore never sees a
        // half-written cache file, and concurrent fetches cannot interleave.
        let tmp = root.join(format!(
            "{}.blob.tmp.{}.{}",
            ticket.hash(),
            std::process::id(),
            NEXT_TMP.fetch_add(1, Ordering::Relaxed)
        ));
        let write = async {
            let mut reader = store.blobs().reader(ticket.hash());
            let mut file = tokio::fs::File::create(&tmp).await?;
            tokio::io::copy(&mut reader, &mut file).await?;
            file.sync_all().await?;
            drop(file);
            tokio::fs::rename(&tmp, output).await?;
            Ok::<_, std::io::Error>(())
        }
        .await;
        if let Err(error) = write {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(error.into());
        }
        drop(conn);
        Ok::<PathBuf, anyhow::Error>(output.to_path_buf())
    }
    .await;
    // `Drop` shuts the store down in the background: pending DB commits are
    // lost and blobs.db stays locked for a moment (the per-request retry loop
    // used to eat that as a 1s sleep). Close it deliberately; the cache file is
    // already durable, so a failure (or a stuck shutdown) must not fail the
    // fetch or hang the request.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), store.shutdown()).await;
    result
}

pub fn run_fetch(
    ticket: String,
    root: PathBuf,
    endpoint: Option<Endpoint>,
) -> anyhow::Result<PathBuf> {
    // block_on on a fresh thread: the daemon calls this from tokio workers,
    // where blocking panics ("cannot start a runtime from within a runtime").
    std::thread::spawn(move || runtime().block_on(fetch(ticket, root, endpoint)))
        .join()
        .map_err(|_| anyhow::anyhow!("blob worker panicked"))?
}

pub fn run_put(
    root: PathBuf,
    data: Vec<u8>,
    transport: Option<Arc<IrohTransport>>,
) -> anyhow::Result<(BlobTicket, usize)> {
    std::thread::spawn(move || runtime().block_on(put(root, data, transport)))
        .join()
        .map_err(|_| anyhow::anyhow!("blob worker panicked"))?
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fails before any network work: a collection ticket is never kept.
    #[test]
    fn rejects_collection_ticket() {
        let id = iroh::SecretKey::generate().public();
        let hashseq = BlobTicket::new(
            iroh::EndpointAddr::new(id),
            iroh_blobs::Hash::new(b"entries"),
            BlobFormat::HashSeq,
        );
        let error = runtime()
            .block_on(fetch(
                hashseq.to_string(),
                std::env::temp_dir().join("idfon-blob-guard-test"),
                None,
            ))
            .unwrap_err();
        assert!(error.to_string().contains("Raw"), "{error}");
    }

    /// A second fetch of the same ticket must come from the verified cache
    /// file, not the network and not the store's own copy (a chunked transfer
    /// asks for the same blob once per chunk).
    #[test]
    fn second_fetch_uses_verified_cache() {
        runtime().block_on(async {
            let base =
                std::env::temp_dir().join(format!("idfon-blob-cache-test-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&base);
            // Provider side, built by hand so we can shut it down.
            let provider = idfon_core::relay::endpoint_builder().bind().await.unwrap();
            let provider_store = FsStore::load(base.join("blobs")).await.unwrap();
            let protocol = BlobsProtocol::new(provider_store.as_ref(), None);
            let router = iroh::protocol::Router::builder(provider.clone())
                .accept(BLOBS_ALPN, protocol)
                .spawn();
            let data = vec![7u8; 512 * 1024];
            let content = provider_store.blobs().add_slice(&data).await.unwrap();
            let _ =
                tokio::time::timeout(std::time::Duration::from_secs(5), provider.online()).await;
            let ticket = BlobTicket::new(provider.addr(), content.hash, content.format);
            let fetch_root = base.join("fetched");

            let first = fetch(ticket.to_string(), fetch_root.clone(), None)
                .await
                .unwrap();
            // Keep only the cache file, so the store's local copy cannot be
            // what makes the second fetch succeed.
            let bytes = std::fs::read(&first).unwrap();
            std::fs::remove_dir_all(&fetch_root).unwrap();
            std::fs::create_dir_all(&fetch_root).unwrap();
            std::fs::write(fetch_root.join(first.file_name().unwrap()), &bytes).unwrap();

            router.shutdown().await.ok();
            provider.close().await;
            drop(provider_store);
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;

            let second = tokio::time::timeout(
                std::time::Duration::from_secs(3),
                fetch(ticket.to_string(), fetch_root.clone(), None),
            )
            .await
            .expect("cache miss: the second fetch needed the network")
            .unwrap();
            assert_eq!(second, fetch_root.join(format!("{}.blob", ticket.hash())));
            let _ = std::fs::remove_dir_all(&base);
        });
    }

    /// Blobs ride the identity endpoint: the ticket names the provider's
    /// identity endpoint id, and the receiver fetches from its own identity
    /// endpoint (so a webhook-mode relay that admits identities admits both).
    #[test]
    fn put_and_fetch_ride_identity_endpoints() {
        runtime().block_on(async {
            let base = std::env::temp_dir()
                .join(format!("idfon-blob-identity-test-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&base);
            let serve = |transport: Arc<IrohTransport>| {
                tokio::spawn(async move {
                    let _ = transport
                        .serve(|_| async {
                            Err(idfon_core::transport::TransportError::Failed(
                                "unused".into(),
                            ))
                        })
                        .await;
                })
            };
            let provider = Arc::new(IrohTransport::builder().bind().await.unwrap());
            let receiver = Arc::new(IrohTransport::builder().bind().await.unwrap());
            serve(provider.clone());
            serve(receiver.clone());

            let data = vec![9u8; 256 * 1024];
            let (ticket, _) = put(base.join("blobs"), data.clone(), Some(provider.clone()))
                .await
                .unwrap();
            assert_eq!(ticket.addr().id, provider.endpoint().id());

            let path = fetch(
                ticket.to_string(),
                base.join("fetched"),
                Some(receiver.endpoint().clone()),
            )
            .await
            .unwrap();
            assert_eq!(std::fs::read(path).unwrap(), data);
            let _ = std::fs::remove_dir_all(&base);
        });
    }

    /// A truncated or tampered cache file must not be served.
    #[test]
    fn cache_rejects_corrupt_file() {
        let root =
            std::env::temp_dir().join(format!("idfon-blob-cache-corrupt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let data = b"cache me";
        let hash = iroh_blobs::Hash::new(data);
        let path = root.join(format!("{hash}.blob"));
        std::fs::write(&path, data).unwrap();
        assert!(cache_matches(&path, hash));
        std::fs::write(&path, b"cache m3").unwrap();
        assert!(!cache_matches(&path, hash));
        let _ = std::fs::remove_dir_all(&root);
    }
}
