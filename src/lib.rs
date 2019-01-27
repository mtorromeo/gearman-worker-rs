#![deny(missing_docs)]
//! # gearman-worker
//!
//! The `gearman-worker` crate provides a high level library to easily
//! implement gearman [`Worker`](struct.Worker.html)s.
//!
//! It handles registration of functions as jobs in the gearman queue
//! server, fetching of jobs and their workload.
//!
//! ## Usage
//!
//! ```ignore
//! use gearman_worker::WorkerBuilder;
//!
//! fn main() {
//!     let mut worker = WorkerBuilder::default().build();
//!     worker.connect().unwrap();
//!
//!     worker.register_function("greet", |input| {
//!         let hello = String::from_utf8_lossy(input);
//!         let response = format!("{} world!", hello);
//!         Ok(response.into_bytes())
//!     }).unwrap();
//!
//!     worker.run().unwrap();
//! }
//! ```

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::collections::HashMap;
use std::io;
use std::io::prelude::*;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::process;
use uuid::Uuid;

const CAN_DO: u32 = 1;
const CANT_DO: u32 = 2;
// const RESET_ABILITIES: u32 = 3;
const PRE_SLEEP: u32 = 4;
const NOOP: u32 = 6;
const GRAB_JOB: u32 = 9;
const NO_JOB: u32 = 10;
const JOB_ASSIGN: u32 = 11;
// const WORK_STATUS: u32 = 12;
const WORK_COMPLETE: u32 = 13;
const WORK_FAIL: u32 = 14;
const SET_CLIENT_ID: u32 = 22;
// const CAN_DO_TIMEOUT: u32 = 23;
// const ALL_YOURS: u32 = 24;
const WORK_EXCEPTION: u32 = 25;
// const WORK_DATA: u32 = 28;
// const WORK_WARNING: u32 = 29;
// const GRAB_JOB_UNIQ: u32 = 30;
// const JOB_ASSIGN_UNIQ: u32 = 31;
// const GRAB_JOB_ALL: u32 = 39;
// const JOB_ASSIGN_ALL: u32 = 40;

/// A packet received from the gearman queue server.
struct Packet {
    /// The packet type representing the request intent
    cmd: u32,
    /// The data associated with the request
    data: Vec<u8>,
}

type WorkResult = Result<Vec<u8>, Option<Vec<u8>>>;
type Callback = Box<Fn(&[u8]) -> WorkResult + 'static>;

struct CallbackInfo {
    callback: Callback,
    enabled: bool,
}

impl CallbackInfo {
    fn new<F: Fn(&[u8]) -> WorkResult + 'static>(callback: F) -> Self {
        Self {
            callback: Box::new(callback),
            enabled: true,
        }
    }
}

impl Packet {
    /// Decodes a packet from a stream received from the gearman server
    fn from_stream(stream: &mut TcpStream) -> io::Result<Self> {
        let mut magic = vec![0u8; 4];
        stream.read_exact(&mut magic)?;

        if magic != b"\0RES" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unexpected magic packet received from server",
            ));
        }

        let cmd = stream.read_u32::<BigEndian>()?;
        let size = stream.read_u32::<BigEndian>()?;
        let mut data = vec![0u8; size as usize];

        if size > 0 {
            stream.read_exact(&mut data)?;
        }

        Ok(Packet { cmd, data })
    }
}

struct Job {
    handle: String,
    function: String,
    workload: Vec<u8>,
}

impl Job {
    fn from_data(data: &[u8]) -> io::Result<Self> {
        let mut iter = data.split(|c| *c == 0);

        let handle = match iter.next() {
            Some(handle) => String::from_utf8_lossy(handle),
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Could not decode handle id",
                ));
            }
        };

        let fun = match iter.next() {
            Some(fun) => String::from_utf8_lossy(fun),
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Could not decode function name",
                ));
            }
        };

        let payload = &data[handle.len() + fun.len() + 2..];

        Ok(Self {
            handle: handle.to_string(),
            function: fun.to_string(),
            workload: payload.to_vec(),
        })
    }

    fn send_response(
        &self,
        server: &mut ServerConnection,
        response: &WorkResult,
    ) -> io::Result<()> {
        let (op, data) = match response {
            Ok(data) => (WORK_COMPLETE, Some(data)),
            Err(Some(data)) => (WORK_FAIL, Some(data)),
            Err(None) => (WORK_EXCEPTION, None),
        };

        let size = self.handle.len() + 1 + data.map_or(0, |b| b.len());

        let mut payload = Vec::with_capacity(size);
        payload.extend_from_slice(self.handle.as_bytes());
        if let Some(data) = data {
            payload.extend_from_slice(b"\0");
            payload.extend_from_slice(data);
        }
        server.send(op, &payload[..])
    }
}

/// The `Worker` processes jobs provided by the gearman queue server.
///
/// Building a worker requires a [`SocketAddr`](https://doc.rust-lang.org/std/net/enum.SocketAddr.html) to
/// connect to the gearman server (typically some ip address on port 4730).
///
/// The worker also needs a unique id to identify itself to the server.
/// This can be omitted letting the [`WorkerBuilder`](struct.WorkerBuilder.html) generate one composed
/// by the process id and a random uuid v4.
///
/// # Examples
///
/// Create a worker with all default options.
///
/// ```
/// use gearman_worker::WorkerBuilder;
/// let mut worker = WorkerBuilder::default().build();
/// ```
///
/// Create a worker with all explicit options.
///
/// ```
/// use gearman_worker::WorkerBuilder;
/// let mut worker = WorkerBuilder::new("my-worker-1", "127.0.0.1:4730".parse().unwrap()).build();
/// ```
pub struct Worker {
    /// the unique id of the worker
    id: String,
    server: ServerConnection,
    functions: HashMap<String, CallbackInfo>,
}

/// Helps building a new [`Worker`](struct.Worker.html)
pub struct WorkerBuilder {
    id: String,
    addr: SocketAddr,
}

struct ServerConnection {
    addr: SocketAddr,
    stream: Option<TcpStream>,
}

impl ServerConnection {
    fn new(addr: SocketAddr) -> Self {
        Self { addr, stream: None }
    }

    fn connect(&mut self) -> io::Result<()> {
        let stream = TcpStream::connect(self.addr)?;
        self.stream = Some(stream);
        Ok(())
    }

    fn read_header(&mut self) -> io::Result<Packet> {
        let mut stream = match &mut self.stream {
            Some(ref mut stream) => stream,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "Stream is not open...",
                ));
            }
        };

        Ok(Packet::from_stream(&mut stream)?)
    }

    fn send(&mut self, command: u32, param: &[u8]) -> io::Result<()> {
        let mut stream = match &self.stream {
            Some(ref stream) => stream,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "Stream is not open...",
                ));
            }
        };

        stream.write_all(b"\0REQ")?;
        stream.write_u32::<BigEndian>(command)?;
        stream.write_u32::<BigEndian>(param.len() as u32)?;
        stream.write_all(param)?;

        Ok(())
    }
}

impl Worker {
    /// Registers a `callback` function that can handle jobs with the specified `name`
    /// provided by the gearman queue server.
    ///
    /// The callback has the signature `Fn(&[u8]) -> WorkResult + 'static` receiving a
    /// slice of bytes in input, which is the workload received from the gearmand server.
    ///
    /// It can return `Ok(Vec<u8>)` ([`WORK_COMPLETE`][protocol]) where the vector of
    /// bytes is the result of the job that will be transmitted back to the server,
    /// `Err(None)` ([`WORK_EXCEPTION`][protocol]) which will tell the server that the
    /// job failed with an unspecified error or `Err(Some(Vec<u8>))` ([`WORK_FAIL`][protocol])
    /// which will also represent a job failure but will include a payload of the error
    /// to the gearmand server.
    ///
    /// [protocol]: http://gearman.org/protocol
    pub fn register_function<S, F>(&mut self, name: S, callback: F) -> io::Result<()>
    where
        S: AsRef<str>,
        F: Fn(&[u8]) -> WorkResult + 'static,
    {
        let name = name.as_ref();
        self.server.send(CAN_DO, &name.as_bytes())?;
        self.functions
            .insert(name.to_string(), CallbackInfo::new(callback));
        Ok(())
    }

    /// Unregisters a previously registered function, notifying the server that
    /// this worker is not available anymore to process jobs with the specified `name`.
    pub fn unregister_function<S>(&mut self, name: S) -> io::Result<()>
    where
        S: AsRef<str>,
    {
        let name = name.as_ref();
        if let Some(func) = self.functions.remove(&name.to_string()) {
            if func.enabled {
                self.server.send(CANT_DO, &name.as_bytes())?;
            }
        }
        Ok(())
    }

    /// Notify the gearman queue server that we are available/unavailable to process
    /// jobs with the specified `name`.
    pub fn set_function_enabled<S>(&mut self, name: S, enabled: bool) -> io::Result<()>
    where
        S: AsRef<str>,
    {
        let name = name.as_ref();
        match self.functions.get_mut(name) {
            Some(ref mut func) if func.enabled != enabled => {
                func.enabled = enabled;
                let op = if enabled { CAN_DO } else { CANT_DO };
                self.server.send(op, name.as_bytes())?;
            }
            Some(_) => eprintln!(
                "Function {} is already {}",
                name,
                if enabled { "enabled" } else { "disabled" }
            ),
            None => eprintln!("Unknown function {}", name),
        }
        Ok(())
    }

    /// Let the server know that the worker identifies itself with the associated `id`.
    pub fn set_client_id(&mut self) -> io::Result<()> {
        self.server.send(SET_CLIENT_ID, self.id.as_bytes())
    }

    fn sleep(&mut self) -> io::Result<()> {
        self.server.send(PRE_SLEEP, b"")?;
        let resp = self.server.read_header()?;
        match resp.cmd {
            n if n == NOOP => Ok(()),
            n => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Worker was sleeping. NOOP was expected but packet {} was received instead.",
                    n
                ),
            )),
        }
    }

    fn grab_job(&mut self) -> io::Result<Option<Job>> {
        self.server.send(GRAB_JOB, b"")?;
        let resp = self.server.read_header()?;
        match resp.cmd {
            n if n == JOB_ASSIGN => Ok(Some(Job::from_data(&resp.data[..])?)),
            n if n == NO_JOB => Ok(None),
            n => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Either JOB_ASSIGN or NO_JOB was expected but packet {} was received instead.",
                    n
                ),
            )),
        }
    }

    /// Ask the server to do some work. This will process one job that will be provided by
    /// the gearman queue server when available.
    pub fn do_work(&mut self) -> io::Result<u32> {
        let mut jobs = 0;

        if let Some(job) = self.grab_job()? {
            jobs += 1;
            match self.functions.get(&job.function) {
                Some(func) if func.enabled => {
                    job.send_response(&mut self.server, &(func.callback)(&job.workload))?
                }
                // gearmand should never pass us a job which was never advertised or unregistered
                Some(_) => eprintln!("Disabled job {:?}", job.function),
                None => eprintln!("Unknown job {:?}", job.function),
            }
        }

        Ok(jobs)
    }

    /// Process any available job as soon as the gearman queue server provides us with one
    /// in a loop.
    pub fn run(&mut self) -> io::Result<()> {
        loop {
            let done = self.do_work()?;
            if done == 0 {
                self.sleep()?;
            }
        }
    }

    /// Estabilish a connection with the queue server and send it the ID of this worker.
    pub fn connect(&mut self) -> io::Result<&mut Self> {
        self.server.connect()?;
        self.set_client_id()?;
        Ok(self)
    }
}

impl WorkerBuilder {
    /// Create a new WorkerBuilder passing all options explicitly.
    pub fn new<S: Into<String>>(id: S, addr: SocketAddr) -> Self {
        Self {
            id: id.into(),
            addr,
        }
    }

    /// Set a specific ID for this worker. This should be unique to all workers!
    pub fn id<S: Into<String>>(&mut self, id: S) -> &mut Self {
        self.id = id.into();
        self
    }

    /// Define the socket address to connect to.
    pub fn addr(&mut self, addr: SocketAddr) -> &mut Self {
        self.addr = addr;
        self
    }

    /// Build the [`Worker`](struct.Worker.html).
    pub fn build(&self) -> Worker {
        Worker {
            id: self.id.clone(),
            server: ServerConnection::new(self.addr),
            functions: HashMap::new(),
        }
    }
}

impl Default for WorkerBuilder {
    fn default() -> Self {
        let uniqid = Uuid::new_v4();
        Self::new(
            format!("{}-{}", process::id(), uniqid.to_hyphenated()),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4730),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufReader;
    use std::process::{Child, Command, Stdio};
    use std::thread;
    use std::time;

    fn run_gearmand() -> Child {
        let mut gearmand = Command::new("gearmand")
            .arg("-L")
            .arg("127.0.0.1")
            .arg("-p")
            .arg("14730")
            .arg("-l")
            .arg("stderr")
            .arg("--verbose")
            .arg("INFO")
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to stard gearmand");

        let gearmand_err = gearmand
            .stderr
            .take()
            .expect("Failed to capture gearmand's stderr");
        let mut reader = BufReader::new(gearmand_err);
        loop {
            let mut line = String::new();
            let len = reader.read_line(&mut line).unwrap();
            if len == 0 || line.contains("Listening on 127.0.0.1:14730") {
                break;
            }
        }

        gearmand
    }

    fn submit_job(func: &str) -> Child {
        let gearman_cli = Command::new("gearman")
            .arg("-Is")
            .arg("-p")
            .arg("14730")
            .arg("-f")
            .arg(func)
            .stdout(Stdio::piped())
            .spawn()
            .expect("Failed to submit gearman job");

        let wait = time::Duration::from_millis(250);
        thread::sleep(wait);

        gearman_cli
    }

    #[test]
    fn it_works() {
        let mut gearmand = run_gearmand();

        let addr = "127.0.0.1:14730".parse().unwrap();

        let mut worker = WorkerBuilder::default()
            .addr(addr)
            .id("gearman-worker-rs-1")
            .build();

        worker
            .connect()
            .expect("Failed to connect to gearmand server");

        worker
            .register_function("testfun", |_| {
                println!("testfun called");
                Ok(b"foobar".to_vec())
            })
            .expect("Failed to register test function");

        // worker.set_function_enabled("testfun", false).unwrap();

        // worker.unregister_function("testfun").unwrap();

        // worker.set_function_enabled("testfun", true).unwrap();

        let gearman_cli = submit_job("testfun");

        let done = worker.do_work().unwrap();
        assert_eq!(1, done);

        let output = gearman_cli
            .wait_with_output()
            .expect("Failed to retrieve job output");
        assert_eq!(b"foobar", output.stdout.as_slice());

        gearmand.kill().expect("Failed to kill gearmand");
    }
}
