// Run this program once the users have been initiated
// Read from the file the name of the existing tables

// It needs to be able to write to a specific table in Noria
// It needs to poll specific views in Noria.

#[macro_use]
extern crate slog;
extern crate chrono;
extern crate rand;
extern crate slog_term;

use chrono::Local;
use chrono::NaiveDateTime;
use fake::Fake;
use noria::{DataType, SyncTable, SyncView};
use noria::{SyncControllerHandle, ZookeeperAuthority};
use rand::seq::SliceRandom;
use slog::Drain;
use slog::Logger;
use slog_term::term_full;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::io::{BufRead, BufReader};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{thread, time};

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
        println!("Looking for inputs");
        let inputs = ch.inputs().expect("couldn't get inputs from Noria");
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
    let buffered = BufReader::new(
        File::open("/home/ekiziv/websubmit-rs/info.txt").unwrap(),
    );
    for line in buffered.lines() {
        let name = line.unwrap().to_string();
        names.push(name.trim_matches('\"').to_string());
    }
    if names.len() < 1 {
        panic!("not enough names to run");
    }

    // instantiate Noria
    let backend = Arc::new(Mutex::new(NoriaBackend::new().unwrap()));
    
    let num_requests = 3000;
    let qids: Vec<u64> = (0..num_requests).collect();


    // fetch table handlers
    let mut table_handlers: HashMap<String, SyncTable> = {
        let mut bg = backend.lock().unwrap();
        names
            .clone()
            .into_iter()
            .map(|e| {
                let table_name = format!("answers_{}", e.clone());
                let handle = &mut (bg.handle.table(&table_name).unwrap().into_sync());
                (e.clone(), handle.clone())
            })
            .collect()
    };

    // fetch view
    let mut view: SyncView = {
        let mut bg = backend.lock().unwrap();
        bg.handle
            .view("answers_by_q_and_emailkey")
            .unwrap()
            .into_sync()
    };

    let mut threads = Vec::new();
    let (tx, rx) = mpsc::channel();
    let wid = thread::spawn(move || write(tx, names, qids, num_requests, &mut table_handlers));

    threads.push(wid);

    let rid = thread::spawn(move || read(rx, num_requests, &mut view));
    threads.push(rid);

    // waiting for both threads to finish
    for thread in threads {
        thread.join().expect("oops! the child thread panicked");
    }

    let mut users_view: SyncView = {
        let mut bg = backend.lock().unwrap();
        bg.handle.view("all_users").unwrap().into_sync()
    };
    let res = users_view.lookup(&[(0 as u64).into()], true).unwrap();
    assert_eq!(res.len(), 50);
}

fn write(
    tx: Sender<(String, u64)>,
    names: Vec<String>,
    qids: Vec<u64>,
    num_rq: u64,
    handlers: &mut HashMap<String, SyncTable>,
) {
    use fake::faker::lorem::en::*;
    let mut i = 0;
    while i <= num_rq {
        let start = Instant::now();
        // pick a random user, write to its table
        let name = names.choose(&mut rand::thread_rng()).unwrap();
        let qid: &u64 = qids.choose(&mut rand::thread_rng()).unwrap();
        let answer: String = Sentence(5..7).fake();

        // record the time since the previous request
        let new_ts = Local::now().naive_local();
        println!("{:?}", new_ts);
        let ts: DataType = DataType::Timestamp(new_ts);

        let rec: Vec<DataType> = vec![
            (*name).clone().into(),
            0.into(),
            (*qid).clone().into(),
            answer.clone().into(),
            ts.into(),
        ];
        let table = handlers.get_mut(name).unwrap();
        (table)
            .insert(rec)
            .expect("failed to insert into answers table");
        // let to_sleep =
        //     time::Duration::new(0, 100_000_000).checked_sub(Instant::now().duration_since(start));
        // if to_sleep.is_some() {
        //     thread::sleep(to_sleep.unwrap());
        // }
        let tuple = ((*name).clone(), (*qid).clone());
        tx.send(tuple).unwrap();
        i += 1;
    }
}

fn read(rx: Receiver<(String, u64)>, num_rq: u64, view: &mut SyncView) {
    let mut end_times: Vec<(NaiveDateTime, NaiveDateTime)> = Vec::new();
    let mut i = 0;

    while i <= num_rq {
        if i == 500 {
            println!("!!!!!!!!!!!!!NOW!!!!!!!!!!!!!!!!")
        }
        let (email, q) = rx.recv().unwrap();
        let mut res = Vec::new();

        while res.len() < 1 {
            res = view
                .lookup(&[email.clone().into(), q.into()], true)
                .expect("failed to look up the user in answers_by_q_and_emailkey");
        }
        res //
            .into_iter()
            .map(|r| {
                if let DataType::Timestamp(ts) = r[4] {
                    Some(ts)
                } else {
                    None
                }
            })
            .for_each(|ts| {
                let times = (ts.unwrap(), Local::now().naive_local());
                end_times.push(times);
            });

        i += 1;
    }
    println!("Done!");

    end_times.sort_by_key(|k| k.0);

    let mut latency_file = OpenOptions::new()
        .write(true)
        .create(true)
        .open("latency.txt")
        .unwrap();
    let mut interval_file = OpenOptions::new()
        .write(true)
        .create(true)
        .open("intervals.txt")
        .unwrap();

    let mut prev = end_times[0].0;
    println!("length of end_times: {:?}", end_times.len());
    for (start, end) in end_times.into_iter() {
        let interval = start.signed_duration_since(prev).num_milliseconds();
        if interval < 0 {
            println!(
                "time since the last element is negative. start: {:?}, prev: {:?}",
                start.clone(),
                prev.clone()
            );
        }
        prev = start;
        let latency = end.signed_duration_since(start).num_milliseconds();
        let _err = write!(&mut latency_file, "{}\n", latency);
        let _err = write!(&mut interval_file, "{}\n", interval);
    }
}
