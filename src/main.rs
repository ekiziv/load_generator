// Run this program once the users have been initiated
// Read from the file the name of the existing tables

// It needs to be able to write to a specific table in Noria
// It needs to poll specific views in Noria.

#[macro_use]
extern crate slog;
extern crate slog_term;
extern crate rand;
extern crate chrono;



use rand::seq::SliceRandom;

use std::io::{BufRead, BufReader};
use slog::Drain;
use slog::Logger;
use slog_term::term_full;
use std::fs::File;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc;
use std::thread;
use noria::{SyncControllerHandle, ZookeeperAuthority};
use noria::{DataType, SyncTable};
use fake::{Fake};
use std::collections::HashMap;

use chrono::Local;

pub struct NoriaBackend {
    pub handle: SyncControllerHandle<ZookeeperAuthority, tokio::runtime::TaskExecutor>,
    pub executor: tokio::runtime::TaskExecutor,
    pub runtime: tokio::runtime::Runtime,
}

impl NoriaBackend {
    pub fn new() -> Result<NoriaBackend, std::io::Error> {
        let  log = Logger::root(Mutex::new(term_full()).fuse(), o!());
        let zk_auth = ZookeeperAuthority::new("127.0.0.1:2181/hello")
        .expect("failed to connect to Zookeeper");

        debug!(log, "Connecting to Noria...");
        let rt = tokio::runtime::Runtime::new().unwrap();
        let executor = rt.executor();
        let mut ch = SyncControllerHandle::new(zk_auth, executor.clone())
        .expect("failed to connect to Noria controller");
        println!("Looking for inputs");
        let inputs = ch
            .inputs()
            .expect("couldn't get inputs from Noria");
        println!("inputs: {:?}", inputs);
        Ok(NoriaBackend {
            handle: ch,
            executor: executor,
            runtime: rt,
        })
    }
}


fn main() {
    // read table names
    let mut names: Vec<String> = Vec::new();
    let buffered = BufReader::new(File::open("/Users/eleonorakiziv/rust/websubmit-rs/websubmit-rs/info.txt").unwrap());
    for line in buffered.lines() {
        let name = line.unwrap().to_string();
        names.push(name.trim_matches('\"').to_string());
    }
    if names.len() < 1 {
        panic!("not enough names to run");
    }

    // instantiate Noria
    let backend = Arc::new(Mutex::new(
        NoriaBackend::new()
        .unwrap()
    ));

    let size: u64 = names.len() as u64;
    let qids: Vec<u64> = (0..size).collect();
    let num_requests = 50;

    // fetch table handlers
    let mut table_handlers: HashMap<String, SyncTable> = {
        let mut bg = backend.lock().unwrap();
        names.clone()
            .into_iter()
            .map(|e| {
                let table_name = format!("answers_{}", e.clone());
                let handle = &mut (bg.handle.table(&table_name).unwrap().into_sync());
                (e.clone(), handle.clone())
            })
            .collect()
    };

    // fetch view
    println!("here are table_handlers: {:?}", table_handlers.clone());

    let mut threads = Vec::new();
    let (tx, rx) = mpsc::channel();
    let wid = thread::spawn(move || {
        write(tx, names, qids, &backend, num_requests, &mut table_handlers)
    });

    threads.push(wid);

    // let rid = thread::spawn(move || {
    //     read(rx)
    // });
    // threads.push(rid, num_rq);

    // one thread for reading

    // waiting for both threads to finish
    for thread in threads {
        thread.join().expect("oops! the child thread panicked");
    }


}

fn write(
    tx: Sender<(String, u64)>,
    names: Vec<String>,
    qids: Vec<u64>,
    backend: &Arc<Mutex<NoriaBackend>>,
    num_rq: u64,
    handlers: &mut HashMap<String, SyncTable>
    ) {
    use fake::faker::lorem::en::*;
    let mut i = 0;
    while i < num_rq {

    // pick a random user, write to its table
        let name = names.choose(&mut rand::thread_rng()).unwrap();
        let qid: &u64 = qids.choose(&mut rand::thread_rng()).unwrap();
        let answer: String = Sentence(5..7).fake();
        let timestamp = Local::now().naive_local();
        let ts: DataType = DataType::Timestamp(timestamp);
        let rec: Vec<DataType> = vec![
            (*name).clone().into(),
            0.into(),
            (*qid).clone().into(),
            answer.clone().into(),
            ts.into(),
        ];
        let table = handlers.get_mut(name).unwrap();
        (table).insert(rec).expect("failed to insert into answers table");
        println!("Inserted");
        let tuple = ((*name).clone(), (*qid).clone());
        tx.send(tuple).unwrap();
        i += 1;
    }
}

