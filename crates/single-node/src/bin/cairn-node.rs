use cairn_device::io::FileDevice;
use cairn_single_node::{HttpNode, SingleNodeConfig, SingleNodeStore};
use std::{
    env,
    error::Error,
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

const DEFAULT_DEVICE_CAPACITY: u64 = 64 * 1024 * 1024;

#[cfg(unix)]
static STOP_REQUESTED: AtomicBool = AtomicBool::new(false);

#[cfg(unix)]
extern "C" fn request_stop(_: libc::c_int) {
    STOP_REQUESTED.store(true, Ordering::SeqCst);
}

#[cfg(unix)]
fn spawn_signal_watcher(sender: mpsc::Sender<()>) -> thread::JoinHandle<()> {
    unsafe {
        libc::signal(libc::SIGINT, request_stop as libc::sighandler_t);
        libc::signal(libc::SIGTERM, request_stop as libc::sighandler_t);
    }
    thread::spawn(move || {
        while !STOP_REQUESTED.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(50));
        }
        let _ = sender.send(());
    })
}

fn main() -> Result<(), Box<dyn Error>> {
    let options = Options::parse(env::args().skip(1))?;
    if let Some(parent) = options
        .catalog
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    if let Some(parent) = options
        .data
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }

    let config = SingleNodeConfig::new(&options.catalog, &options.data, options.actor_id);
    if !options.data.exists() {
        FileDevice::create_preallocated(&options.data, DEFAULT_DEVICE_CAPACITY)?;
    }
    // Bootstrap is idempotent. Run it on every start so a crash after SQLite
    // creates its schema but before metadata initialization is recoverable.
    SingleNodeStore::bootstrap(&config, &options.collection, &options.file)?;
    let store = SingleNodeStore::open(config)?;
    let listener = TcpListener::bind(options.listen)?;
    eprintln!("cairn-node listening on {}", listener.local_addr()?);
    let (stop_sender, stop_receiver) = mpsc::channel();
    #[cfg(unix)]
    let signal_watcher = spawn_signal_watcher(stop_sender);
    #[cfg(not(unix))]
    let _signal_watcher = stop_sender;
    HttpNode::new(store).run(listener, stop_receiver)?;
    #[cfg(unix)]
    signal_watcher
        .join()
        .map_err(|_| "signal watcher panicked")?;
    Ok(())
}

struct Options {
    catalog: PathBuf,
    data: PathBuf,
    listen: String,
    actor_id: u64,
    collection: String,
    file: String,
}

impl Options {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, Box<dyn Error>> {
        let mut catalog = None;
        let mut data = None;
        let mut listen = String::from("127.0.0.1:8080");
        let mut actor_id = 1;
        let mut collection = String::from("default");
        let mut file = String::from("default");
        let mut args = args.peekable();
        while let Some(flag) = args.next() {
            let mut value = || {
                args.next()
                    .ok_or_else(|| format!("missing value for {flag}"))
            };
            match flag.as_str() {
                "--catalog" => catalog = Some(PathBuf::from(value()?)),
                "--data" => data = Some(PathBuf::from(value()?)),
                "--listen" => listen = value()?,
                "--actor-id" => actor_id = value()?.parse()?,
                "--collection" => collection = value()?,
                "--file" => file = value()?,
                "--help" | "-h" => {
                    println!(
                        "usage: cairn-node --catalog PATH --data PATH [--listen ADDR] [--actor-id N] [--collection NAME] [--file NAME]"
                    );
                    std::process::exit(0);
                }
                other => return Err(format!("unknown option {other}").into()),
            }
        }
        let catalog = catalog.ok_or("--catalog is required")?;
        let data = data.ok_or("--data is required")?;
        validate_path(&catalog)?;
        validate_path(&data)?;
        Ok(Self {
            catalog,
            data,
            listen,
            actor_id,
            collection,
            file,
        })
    }
}

fn validate_path(path: &Path) -> Result<(), Box<dyn Error>> {
    if path.as_os_str().is_empty() {
        return Err("storage path must not be empty".into());
    }
    Ok(())
}
