extern crate uuid;
extern crate byteorder;

use std::error::Error;
use std::fmt;
use std::collections::HashMap;
use std::process;
use std::io;
use std::io::prelude::*;
use std::net::SocketAddr;
use std::net::TcpStream;
use uuid::{Uuid, UuidVersion};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

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
// const WORK_EXCEPTION: u32 = 25;
// const WORK_DATA: u32 = 28;
// const WORK_WARNING: u32 = 29;
// const GRAB_JOB_UNIQ: u32 = 30;
// const JOB_ASSIGN_UNIQ: u32 = 31;
// const GRAB_JOB_ALL: u32 = 39;
// const JOB_ASSIGN_ALL: u32 = 40;

pub struct Packet {
    cmd: u32,
    data: Vec<u8>,
}

#[derive(Debug)]
pub struct WorkError {
    function: String,
}

impl Error for WorkError {
    fn description(&self) -> &str {
        "Work on function failed"
    }
}

impl fmt::Display for WorkError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Work on function {} failed", self.function)
    }
}

type WorkResult<'a> = Result<&'a[u8], WorkError>;
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
    fn from_stream(stream: &mut TcpStream) -> io::Result<Self> {
        let mut magic = vec![0u8; 4];
        stream.read_exact(&mut magic)?;

        if magic != b"\0RES" {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Unexpected magic packet received from server"));
        }

        let cmd = stream.read_u32::<BigEndian>()?;
        let size = stream.read_u32::<BigEndian>()?;
        let mut data = vec![0u8; size as usize];

        if size > 0 {
            stream.read_exact(&mut data)?;
        }

        Ok(Packet {
            cmd,
            data,
        })
    }
}

pub struct Job {
    handle: String,
    function: String,
    workload: Vec<u8>,
}

impl Job {
    fn from_data(data: &[u8]) -> io::Result<Self> {
        let mut iter = data.split(|c| *c == 0);

        let handle = match iter.next() {
            Some(handle) => String::from_utf8_lossy(handle),
            None => return Err(io::Error::new(io::ErrorKind::InvalidData, "Could not decode handle id")),
        };

        let fun = match iter.next() {
            Some(fun) => String::from_utf8_lossy(fun),
            None => return Err(io::Error::new(io::ErrorKind::InvalidData, "Could not decode function name")),
        };

        let payload = &data[handle.len() + fun.len() + 2 .. ];

        Ok(Self {
            handle: handle.to_string(),
            function: fun.to_string(),
            workload: payload.to_vec(),
        })
    }

    fn send_response(&self, server: &mut ServerConnection, response: &WorkResult) -> io::Result<()> {
        let mut payload = Vec::new();
        payload.extend_from_slice(self.handle.as_bytes());
        match response {
            Ok(data) => {
                payload.extend_from_slice(b"\0");
                payload.extend_from_slice(data);
                server.send(WORK_COMPLETE, &payload[..])
            },
            Err(_) => server.send(WORK_FAIL, &payload[..])
        }
    }
}

pub struct Worker {
    id: String,
    server: ServerConnection,
    functions: HashMap<String, CallbackInfo>,
}

pub struct WorkerBuilder {
    id: Option<String>,
    server: ServerConnection,
}

pub struct ServerConnection {
    addr: SocketAddr,
    stream: Option<TcpStream>,
}

impl ServerConnection {
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            stream: None,
        }
    }

    fn connect(&mut self) -> io::Result<()> {
        let stream = TcpStream::connect(self.addr)?;
        self.stream = Some(stream);
        Ok(())
    }

    fn read_header(&mut self) -> io::Result<Packet> {
        let mut stream = match &mut self.stream {
            Some(ref mut stream) => stream,
            None => return Err(io::Error::new(io::ErrorKind::NotConnected, "Stream is not open...")),
        };

        Ok(Packet::from_stream(&mut stream)?)
    }

    fn send(&mut self, command: u32, param: &[u8]) -> io::Result<()> {
        let mut stream = match &self.stream {
            Some(ref stream) => stream,
            None => return Err(io::Error::new(io::ErrorKind::NotConnected, "Stream is not open...")),
        };

        stream.write_all(b"\0REQ")?;
        stream.write_u32::<BigEndian>(command)?;
        stream.write_u32::<BigEndian>(param.len() as u32)?;
        stream.write_all(param)?;

        Ok(())
    }
}

impl Worker {
    pub fn new(addr: SocketAddr) -> WorkerBuilder {
        WorkerBuilder {
            id: None,
            server: ServerConnection::new(addr),
        }
    }

    pub fn register_function<S, F>(&mut self, name: S, callback: F) -> io::Result<()>
    where
        S: AsRef<str>,
        F: Fn(&[u8]) -> WorkResult + 'static,
    {
        let name = name.as_ref();
        self.server.send(CAN_DO, &name.as_bytes())?;
        self.functions.insert(name.to_string(), CallbackInfo::new(callback));
        Ok(())
    }

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

    pub fn set_function_enabled<S>(&mut self, name: S, enabled: bool) -> io::Result<()>
    where
        S: AsRef<str>,
    {
        let name = name.as_ref();
        match self.functions.get_mut(name) {
            Some(ref mut func) if func.enabled != enabled => {
                func.enabled = enabled;
                let op = if enabled {CAN_DO} else {CANT_DO};
                self.server.send(op, name.as_bytes())?;
            },
            Some(_) => eprintln!("Function {} is already {}", name, if enabled {"enabled"} else {"disabled"}),
            None => eprintln!("Unknown function {}", name),
        }
        Ok(())
    }

    pub fn set_client_id(&mut self) -> io::Result<()> {
        self.server.send(SET_CLIENT_ID, self.id.as_bytes())
    }

    fn sleep(&mut self) -> io::Result<()> {
        self.server.send(PRE_SLEEP, b"")?;
        let resp = self.server.read_header()?;
        match resp.cmd {
            n if n == NOOP => Ok(()),
            n => Err(io::Error::new(io::ErrorKind::InvalidData, format!("Worker was sleeping. NOOP was expected but packet {} was received instead.", n))),
        }
    }

    fn grab_job(&mut self) -> io::Result<Option<Job>> {
        self.server.send(GRAB_JOB, b"")?;
        let resp = self.server.read_header()?;
        match resp.cmd {
            n if n == JOB_ASSIGN => Ok(Some(Job::from_data(&resp.data[..])?)),
            n if n == NO_JOB => Ok(None),
            n => Err(io::Error::new(io::ErrorKind::InvalidData, format!("Either JOB_ASSIGN or NO_JOB was expected but packet {} was received instead.", n))),
        }
    }

    pub fn do_work(&mut self) -> io::Result<u32> {
        let mut jobs = 0;

        if let Some(job) = self.grab_job()? {
            jobs += 1;
            match self.functions.get(&job.function) {
                Some(func) if func.enabled => job.send_response(&mut self.server, &(func.callback)(&job.workload))?,
                Some(_) => eprintln!("Disabled job {:?}", job.function),
                None => eprintln!("Unknown job {:?}", job.function),
            }
        }

        Ok(jobs)
    }

    pub fn run(&mut self) -> io::Result<()> {
        loop {
            let done = self.do_work()?;
            if done == 0 {
                self.sleep()?;
            }
        }
    }
}

impl WorkerBuilder {
    pub fn with_id<S: Into<String>>(mut self, id: S) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn connect(self) -> io::Result<Worker> {
        let id = match self.id {
            Some(id) => id.clone(),
            None => {
                let uniqid = Uuid::new(UuidVersion::Mac).unwrap_or_else(Uuid::new_v4);
                format!("{}-{}", process::id(), uniqid.hyphenated())
            },
        };
        let mut worker = Worker {
            id,
            server: self.server,
            functions: HashMap::new(),
        };
        worker.server.connect()?;
        worker.set_client_id()?;
        Ok(worker)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        use super::*;

        let addr = "127.0.0.1:4730".parse().unwrap();

        let mut worker = Worker::new(addr).with_id("gearman-worker-rs-1").connect().unwrap();

        worker.register_function("testfun", |_| {
            println!("testfun called");
            Ok(b"foobar")
        }).unwrap();

        worker.set_function_enabled("testfun", false).unwrap();

        worker.unregister_function("testfun").unwrap();

        worker.set_function_enabled("testfun", true).unwrap();

        // worker.run().unwrap();

        let done = worker.do_work().unwrap();
        assert_eq!(done, 1);
    }
}
