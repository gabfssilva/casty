//! How the threads of the transport hand work to the event loop without entering the interpreter.
//!
//! A thread of the transport never attaches to Python. On a build with a GIL, attaching waits for the loop to let the
//! GIL go, and the node task, which runs membership, placement, routing and replication for the whole node, would wait
//! with it. So a thread leaves what it has for the loop on a queue, and a byte on a pair of connected sockets wakes the
//! loop, which watches its end like any other socket and runs the queue on its own thread, in the order it was filled.
//! One byte stands for everything queued until the loop takes the queue: a busy node wakes the loop once per turn of it,
//! not once per call.
//!
//! The loop watches its end with `add_reader`, or with `sock_recv` on a loop that has no readers (the proactor loop of
//! Windows). The queue ends with its last `Inbox`: that end of the pair closes, and the loop, reading the end of the
//! stream, runs what is left and closes its own end.

use std::io::Write;
#[cfg(windows)]
use std::net::{Ipv4Addr, TcpListener, TcpStream};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};

use pyo3::exceptions::{PyBlockingIOError, PyNotImplementedError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::callback;
use crate::lock::Locked;

type Call = Box<dyn FnOnce(Python<'_>) -> PyResult<()> + Send>;

/// The calls waiting for the loop, and whether the loop was woken for them.
#[derive(Default)]
struct Pending {
    calls: Vec<Call>,
    rung: bool,
}

/// Where the threads of the transport leave calls for one loop. Clones share the queue.
#[derive(Clone)]
pub struct Inbox(Arc<Bell>);

struct Bell {
    pending: Arc<Mutex<Pending>>,
    socket: Socket,
}

#[cfg(unix)]
type Socket = UnixStream;
#[cfg(windows)]
type Socket = TcpStream;

impl core::fmt::Debug for Inbox {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.debug_struct("Inbox").finish_non_exhaustive()
    }
}

impl Inbox {
    /// A queue to `running_loop`, which watches it from here on. Called on the loop.
    pub fn open(py: Python<'_>, running_loop: &Bound<'_, PyAny>) -> PyResult<Self> {
        let (socket, watched) = pair(py)?;
        let pending = Arc::new(Mutex::new(Pending::default()));
        Reader::watch(running_loop, watched, Arc::clone(&pending))?;
        Ok(Self(Arc::new(Bell { pending, socket })))
    }

    /// Leave `call` for the loop, which runs it after everything left before it.
    pub fn send(&self, call: impl FnOnce(Python<'_>) -> PyResult<()> + Send + 'static) {
        let ring = {
            let mut pending = self.0.pending.locked();
            pending.calls.push(Box::new(call));
            !core::mem::replace(&mut pending.rung, true)
        };
        if ring {
            // A socket too full to take the byte holds bytes the loop has not read yet, so the loop wakes anyway. One
            // the loop has closed belongs to a loop that is gone, and nobody is left to run the call.
            let _ = (&self.0.socket).write(&[0]);
        }
    }
}

/// A connected pair of sockets: the end this side writes to, and the end the loop reads as a `socket.socket`, which
/// owns it from here on.
#[cfg(unix)]
fn pair(py: Python<'_>) -> PyResult<(Socket, Bound<'_, PyAny>)> {
    use std::os::fd::IntoRawFd;

    let (socket, watched) = UnixStream::pair()?;
    socket.set_nonblocking(true)?;
    Ok((socket, adopted(py, watched.into_raw_fd())?))
}

/// A connected pair of sockets: the end this side writes to, and the end the loop reads as a `socket.socket`, which
/// owns it from here on.
///
/// Windows has no pair of its own, so this is how Python builds `socket.socketpair` there: a listener on loopback, one
/// connection to it, and a check that the connection accepted is that one.
#[cfg(windows)]
fn pair(py: Python<'_>) -> PyResult<(Socket, Bound<'_, PyAny>)> {
    use std::os::windows::io::IntoRawSocket;

    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let socket = TcpStream::connect(listener.local_addr()?)?;
    let (watched, from) = listener.accept()?;
    if from != socket.local_addr()? {
        return Err(pyo3::exceptions::PyOSError::new_err(
            "another connection reached the loopback listener of the inbox first",
        ));
    }
    socket.set_nonblocking(true)?;
    // One byte at a time, each of which the loop is waiting for.
    socket.set_nodelay(true)?;
    Ok((socket, adopted(py, watched.into_raw_socket())?))
}

/// The socket of Python that takes over `handle`, which reads without blocking.
fn adopted<'py>(py: Python<'py>, handle: impl IntoPyObject<'py>) -> PyResult<Bound<'py, PyAny>> {
    let options = PyDict::new(py);
    options.set_item("fileno", handle)?;
    let watched = py
        .import("socket")?
        .getattr("socket")?
        .call((), Some(&options))?;
    watched.call_method1("setblocking", (false,))?;
    Ok(watched)
}

/// The end of an inbox on the loop, which runs the queue each time the socket wakes it.
#[pyclass(frozen, module = "casty._casty")]
struct Reader {
    pending: Arc<Mutex<Pending>>,
    socket: Py<PyAny>,
}

impl core::fmt::Debug for Reader {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.debug_struct("Reader").finish_non_exhaustive()
    }
}

impl Reader {
    fn watch(
        running_loop: &Bound<'_, PyAny>,
        socket: Bound<'_, PyAny>,
        pending: Arc<Mutex<Pending>>,
    ) -> PyResult<()> {
        let py = running_loop.py();
        let reader = Bound::new(
            py,
            Self {
                pending,
                socket: socket.clone().unbind(),
            },
        )?;
        match running_loop.call_method1("add_reader", (socket, &reader)) {
            Ok(_) => Ok(()),
            Err(refused) if refused.is_instance_of::<PyNotImplementedError>(py) => {
                Self::receive(&reader, running_loop)
            }
            Err(failed) => Err(failed),
        }
    }

    /// Wait for the next byte with `sock_recv`, on a loop that has no readers.
    fn receive(reader: &Bound<'_, Self>, running_loop: &Bound<'_, PyAny>) -> PyResult<()> {
        let receiving =
            running_loop.call_method1("sock_recv", (reader.get().socket.bind(reader.py()), 64))?;
        let task = running_loop.call_method1("create_task", (receiving,))?;
        let reader = reader.clone().unbind();
        callback::when_done(&task, move |py, task| {
            let reader = reader.bind(py);
            let open = task
                .call_method0("result")
                .and_then(|read| read.len())
                .is_ok_and(|read| read > 0);
            let running_loop = current_loop(py)?;
            reader.get().run(py, &running_loop);
            if open {
                Self::receive(reader, &running_loop)
            } else {
                reader.get().socket.bind(py).call_method0("close").map(drop)
            }
        })
    }

    /// Run what is queued, in order. A call that raises goes to the exception handler of the loop, as a callback that
    /// raised does, and the rest run.
    fn run(&self, py: Python<'_>, running_loop: &Bound<'_, PyAny>) {
        let calls = {
            let mut pending = self.pending.locked();
            pending.rung = false;
            core::mem::take(&mut pending.calls)
        };
        for call in calls {
            if let Err(failed) = call(py) {
                report(running_loop, &failed);
            }
        }
    }
}

#[pymethods]
impl Reader {
    /// The socket is readable: a byte came, or the end of the stream did.
    fn __call__(&self, py: Python<'_>) -> PyResult<()> {
        let socket = self.socket.bind(py);
        let open = match socket.call_method1("recv", (64,)) {
            Ok(read) => read.len()? > 0,
            Err(early) if early.is_instance_of::<PyBlockingIOError>(py) => true,
            Err(_) => false,
        };
        let running_loop = current_loop(py)?;
        self.run(py, &running_loop);
        if !open {
            running_loop.call_method1("remove_reader", (socket,))?;
            socket.call_method0("close")?;
        }
        Ok(())
    }
}

fn current_loop(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    py.import("asyncio")?.call_method0("get_running_loop")
}

fn report(running_loop: &Bound<'_, PyAny>, failed: &PyErr) {
    let py = running_loop.py();
    let context = PyDict::new(py);
    let reported = context
        .set_item(
            "message",
            "Exception in a call the transport of casty handed to the loop",
        )
        .and_then(|()| context.set_item("exception", failed.value(py)))
        .and_then(|()| running_loop.call_method1("call_exception_handler", (context,)));
    if let Err(unreported) = reported {
        unreported.write_unraisable(py, None);
    }
}
