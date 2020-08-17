use noria::consensus::ZookeeperAuthority;
use noria::SyncControllerHandle;
use slog::Drain;
use slog::Logger;
use slog_term::term_full;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
#[macro_use]
extern crate slog;
pub struct NoriaBackend {
    pub handle: SyncControllerHandle<ZookeeperAuthority, tokio::runtime::TaskExecutor>,
    pub executor: tokio::runtime::TaskExecutor,
    pub runtime: tokio::runtime::Runtime,
}

impl NoriaBackend {
    pub fn new() -> Result<NoriaBackend, std::io::Error> {
        let log = Logger::root(Mutex::new(term_full()).fuse(), o!());
        let zk_auth = ZookeeperAuthority::new("127.0.0.1:2181/hello")
            .expect("failed to connect to Zookeeper");

        debug!(log, "Connecting to Noria...");
        let rt = tokio::runtime::Runtime::new().unwrap();
        let executor = rt.executor();
        let mut ch = SyncControllerHandle::new(zk_auth, executor.clone())
            .expect("failed to connect to Noria controller");
        let inputs = ch.inputs().expect("couldn't get inputs from Noria");
        Ok(NoriaBackend {
            handle: ch,
            executor: executor,
            runtime: rt,
        })
    }
}

pub enum Message {
    NewJob(Job),
    Terminate,
}

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Message>,
    done: Arc<Mutex<AtomicBool>>, 
}

type Job = Box<FnBox + Send + 'static>;

pub trait FnBox {
    fn call_box(self: Box<Self>, conn: Arc<Mutex<NoriaBackend>>);
}

impl<F: FnOnce(Arc<Mutex<NoriaBackend>>)> FnBox for F {
    fn call_box(self: Box<F>, conn: Arc<Mutex<NoriaBackend>>) {
        (*self)(conn)
    }
}

impl ThreadPool {
    /// Create a new ThreadPool.
    ///
    /// The size is the number of threads in the pool.
    ///
    /// # Panics
    ///
    /// The `new` function will panic if the size is zero.
    pub fn new(size: usize, mut conn_vec: Vec<Arc<Mutex<NoriaBackend>>>) -> ThreadPool {
        assert!(size > 0);
        let mut workers = Vec::with_capacity(size);
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let done = Arc::new(Mutex::new(AtomicBool::new(false)));
        for id in 0..size {
            // create some threads and store them in the vector
            let conn = conn_vec.pop().unwrap();
            let worker = Worker::new(id, receiver.clone(), conn, done.clone());
            workers.push(worker);
        }

        ThreadPool { workers, sender, done,  }
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce(Arc<Mutex<NoriaBackend>>) + Send + 'static,
    {
        let job = Box::new(f);
        self.sender.send(Message::NewJob(job)).unwrap();
    }
}

pub struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub fn new(
        id: usize,
        receiver: Arc<Mutex<mpsc::Receiver<Message>>>,
        conn: Arc<Mutex<NoriaBackend>>,
        done: Arc<Mutex<AtomicBool>>, 
    ) -> Worker {

        let thread = thread::spawn(move ||  
        loop {     
            if done.lock().unwrap().load(Ordering::Relaxed) == true {
                break; 
            }
            let message = {
                let r = receiver.lock().unwrap(); 
                r.recv().unwrap()
            };

            match message {
                Message::NewJob(job) => { 
                    
                    job.call_box(conn.clone());
                }
                Message::Terminate => {
                    break;
                }
            }
        });
        Worker {
            id,
            thread: Some(thread),
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        for _ in &mut self.workers {
            self.sender.send(Message::Terminate).unwrap();
        }

        self.done.lock().unwrap().store(true, Ordering::Relaxed); 

        for worker in &mut self.workers {
            println!("Shutting doen worker {}", worker.id);
            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}
