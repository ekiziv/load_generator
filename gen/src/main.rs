// Run this program once the users have been initiated
// Read from the file the name of the existing tables

// It needs to be able to write to a specific table in Noria
// It needs to poll specific views in Noria.
#![feature(vec_remove_item)]
#![feature(async_closure)]
#[macro_use]
extern crate slog;
#[macro_use]
extern crate chan;
extern crate chrono;
extern crate crossbeam_queue;
extern crate dashmap;
extern crate rand;
extern crate slog_term;
extern crate stream_cancel;

use chrono::Local;
use chrono::NaiveDateTime;
use fake::Fake;
use futures::{self, Future, Stream};
use noria::prelude::SyncTable;
use noria::{DataType, SyncView};
use noria::{SyncControllerHandle, ZookeeperAuthority};
use rand::seq::SliceRandom;
use slog::Drain;
use slog::Logger;
use slog_term::term_full;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use threadpool::ThreadPool;

use crossbeam_channel::{select, unbounded, Receiver, Sender};
use crossbeam_queue::ArrayQueue;
use dashmap::DashMap;

use rand::Rng;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::io::{BufRead, BufReader};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver as MPSCReceiver, Sender as MPSCSender};
use std::sync::{Arc, Mutex};
use std::thread;
use stream_cancel::{Trigger, Valve};

pub struct NoriaBackend {
    pub handle: SyncControllerHandle<ZookeeperAuthority, tokio::runtime::TaskExecutor>,
    pub executor: tokio::runtime::TaskExecutor,
    pub runtime: tokio::runtime::Runtime,
}

const FACTOR: u64 = 5;
const NUM_RQ: u64 = 1000;
const EVERY: Duration = Duration::from_millis(100);
const NUM_THREADS: usize = 4;

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
    let mut names: Vec<String> = Vec::new();
    let buffered = BufReader::new(
        File::open("/Users/eleonorakiziv/rust/websubmit-rs/websubmit-rs/info.txt").unwrap(),
    );
    let mut info_map = HashMap::new();
    for line in buffered.lines() {
        let l = line.unwrap().to_string();
        let info: Vec<&str> = l.trim_matches('\"').split('*').collect();
        let name = info[0].to_string();
        names.push(name.clone());
        info.iter()
            .filter(|&v| *v != name.clone())
            .map(|i| i.parse::<u32>().unwrap())
            .for_each(|u| {
                info_map
                    .entry(name.clone())
                    .or_insert_with(Vec::new)
                    .push(u)
            });
    }
    if names.len() < 1 {
        panic!("not enough names to run");
    }

    // instantiate Noria
    let lease_backend = Arc::new(Mutex::new(NoriaBackend::new().unwrap()));
    let write_backend = Arc::new(Mutex::new(NoriaBackend::new().unwrap()));
    let main_backend = Arc::new(Mutex::new(NoriaBackend::new().unwrap()));
    let (a_s1, a_r1) = unbounded();
    let (a_s2, a_r2) = (a_s1.clone(), a_r1.clone());

    for name in names.clone().into_iter() {
        a_s1.send((name, false)).unwrap();
    }

    let qids: Vec<u64> = (0..((FACTOR + 1) * NUM_RQ)).collect();

    // fetch view
    let mut view: SyncView = {
        let mut bg = main_backend.lock().unwrap();
        bg.handle
            .view("answers_by_q_and_emailkey")
            .unwrap()
            .into_sync()
    };

    // check how many answers there are altogether
    let users_view: SyncView = {
        let mut bg = lease_backend.lock().unwrap();
        bg.handle.view("answers_by_lec").unwrap().into_sync()
    };
    let (trigger, valve) = Valve::new(); // to stop the ticker in lease thread
    let mut threads = Vec::new();
    let (tx, rx) = mpsc::channel(); // to communicate between read and write threads
    let (signal_sender, signal_receiver) = mpsc::channel(); // sends a signal once the first 1000 writes have been done
    let (p, c) = chan::sync(1);
    let users = names.clone();
    let wid = thread::spawn(|| {
        write(
            tx,
            users,
            qids,
            trigger,
            write_backend,
            a_s1,
            a_r2,
            signal_sender,
            p,
        )
    });

    threads.push(wid);

    let rid = thread::spawn(move || read(rx, &mut view));
    threads.push(rid);

    let tid = thread::spawn(move || {
        do_every(
            lease_backend,
            info_map,
            names.clone(),
            &valve,
            users_view,
            a_s2,
            a_r1,
            signal_receiver,
            c,
        )
    });
    threads.push(tid);

    // waiting for both threads to finish
    for thread in threads {
        thread.join().expect("oops! the child thread panicked");
    }
    println!("I am officially done!");
}

fn write(
    tx: MPSCSender<(String, u64)>,
    names: Vec<String>,
    qids: Vec<u64>,
    trigger: Trigger,
    backend: Arc<Mutex<NoriaBackend>>,
    sender: Sender<(String, bool)>,
    receiver: Receiver<(String, bool)>,
    signal: MPSCSender<bool>,
    done: chan::Sender<bool>,
) {
    let mut handlers: HashMap<String, SyncTable> = {
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
    let mut bg = backend.lock().unwrap();

    // first wave
    use fake::faker::lorem::en::*;
    let mut i = 0;
    while i <= NUM_RQ {
        // pick a random user, write to its table
        let name = names.choose(&mut rand::thread_rng()).unwrap();
        let qid: &u64 = qids.choose(&mut rand::thread_rng()).unwrap();
        let answer: String = Sentence(5..7).fake();

        // record the time since the previous request
        let new_ts = Local::now().naive_local();
        let ts: DataType = DataType::Timestamp(new_ts);

        let rec: Vec<DataType> = vec![
            (*name).clone().into(),
            0.into(),
            (*qid).clone().into(),
            answer.clone().into(),
            ts.into(),
        ];
        let table = handlers.get_mut(name).unwrap();
        table
            .insert(rec)
            .expect("failed to insert into answers table");
        let tuple = ((*name).clone(), (*qid).clone());
        tx.send(tuple).unwrap();
        i += 1;
    }
    println!("__________________________________________________________");
    println!("Start unsubscription");
    println!("__________________________________________________________");
    i = 0;
    signal.send(true).unwrap();
    while i <= FACTOR * NUM_RQ {
        // let name = names.choose(&mut rand::thread_rng()).unwrap();
        select! {
          recv(receiver) -> msg => {
            let info = msg.unwrap();
            let updated = info.1;
            let name = info.0;
            if updated {
              // update the table handler in the hm
              let table_name = format!("answers_{}", name.clone());
              let handle = &mut bg.handle.table(&table_name).unwrap().into_sync();
              handlers.insert(name.clone(), handle.clone());
            }

            let table = handlers.get_mut(&name).unwrap();

            let qid: &u64 = qids.choose(&mut rand::thread_rng()).unwrap();
            let answer: String = Sentence(5..7).fake();
            let ts: DataType = DataType::Timestamp(Local::now().naive_local());

            let rec: Vec<DataType> = vec![
                name.clone().into(),
                0.into(),
                (*qid).clone().into(),
                answer.clone().into(),
                ts.into(),
            ];
            table
                .insert(rec)
                .expect("failed to insert into answers table");

            let tuple = (name.clone(), (*qid).clone());
            tx.send(tuple).unwrap();
            sender.send((name, false)).unwrap();
            i += 1;
          },
          default => println!("skipping a turn in write"),
        }
    }
    println!("WRITE DONE!");

    drop(trigger);
}

fn read(rx: MPSCReceiver<(String, u64)>, view: &mut SyncView) {
    let mut end_times: Vec<(NaiveDateTime, NaiveDateTime)> = Vec::new();
    let mut i = 0;

    while i <= (FACTOR + 1) * NUM_RQ {
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
    println!("READ DONE!");

    end_times.sort_by_key(|k| k.0);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open("end_times.txt")
        .unwrap();

    for (start, end) in end_times.into_iter() {
        write!(&mut file, "{:?}#{:?}\n", start, end).expect("failed to write");
    }
}

fn do_every(
    backend: Arc<Mutex<NoriaBackend>>,
    map: HashMap<String, Vec<u32>>,
    names: Vec<String>,
    _valve: &Valve,
    view: SyncView,
    sender: Sender<(String, bool)>,
    receiver: Receiver<(String, bool)>,
    start: MPSCReceiver<bool>,
    done: chan::Receiver<bool>,
) {
    start.recv().unwrap();
    let name_imported = Arc::new(Mutex::new(ArrayQueue::new(names.len())));
    let info_map = Arc::new(Mutex::new(map));

    let lease_action = move || -> Result<(), failure::Error> {
        println!("Lease action");
        let unsubscribe = flip_coin();
        let start = Instant::now();
        if unsubscribe {
            select! {
              recv(receiver) -> msg => {
                let info = msg.unwrap();
                let user = info.0;

                // get corresponding table ids
                let tables: Vec<u32> = {
                  let mut info_map = info_map.lock().unwrap();
                  info_map.remove(&user).unwrap()
                };

                // get user's data and unsubscribe
                let data = {
                  let mut bg = backend.lock().unwrap();
                  let data = bg
                  .handle
                  .export_data(tables.clone())
                  .expect("failed to export data from Noria");
                  for table in tables.into_iter() {
                    bg.handle
                    .unsubscribe(table)
                    .expect("failed to unsubscribe");
                  }
                  data
                };

                // push to ds that stores (name, data) to resubscribe
                {
                  let name_im = name_imported.lock().unwrap();
                  name_im.push((user, data)).expect("failed to insert");
                }

                println!(
                "func time unsub: {:?}",
                Instant::now()
                .checked_duration_since(start)
                .unwrap()
                .as_millis()
                );
              },
              default() => println!("skipping turn"),
            }
        } else {
            let name_im = name_imported.lock().unwrap();
            if name_im.is_empty() {
                return Ok(());
            }
            let (user, d) = name_im.pop().unwrap();
            drop(name_im);

            // import the data
            let new_tables: Vec<u32> = {
                let mut bg = backend.lock().unwrap();
                bg.handle.import_data(d).unwrap()
            };

            {
                let mut info_map = info_map.lock().unwrap();
                info_map.insert(user.clone(), new_tables);
            }

            sender.send((user, true)).unwrap();
            println!(
                "func time resub: {:?}",
                Instant::now()
                    .checked_duration_since(start)
                    .unwrap()
                    .as_millis()
            );
        }
        Ok(())
    };
    let tick = chan::tick_ms(100);

    let pool = ThreadPool::new(NUM_THREADS);
    let mut cloned: Vec<_> = (0..5000)
        .into_iter()
        .map(|_| lease_action.clone())
        .collect();
    let mut count = 0;

    loop {
        chan_select! {
          default => {},
          tick.recv() => {
            count += 1;
            println!("time");
            if count == 10 {
              count =0 ;
            }
            let la = cloned.pop().unwrap();
            pool.execute(move || {
              async || la();
            })
          },
          done.recv() => {drop(tick); pool.join(); println!("I am done!"); return},
        }
    }
}

fn flip_coin() -> bool {
    let mut rng = rand::thread_rng();
    let x: f64 = rng.gen();
    if x < 0.5 {
        true
    } else {
        false
    }
}
