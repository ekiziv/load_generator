// Run this program once the users have been initiated
// Read from the file the name of the existing tables

// It needs to be able to write to a specific table in Noria
// It needs to poll specific views in Noria.
use slog::Drain;
use slog::Logger;
use slog_term::term_full;
use std::io::{BufRead, BufReader};
use std::collections::HashSet;
use std::fs::File;
use std::sync::{Mutex, Arc};
use std::time::Duration;
use std::thread;
use noria::{DurabilityMode, PersistenceParameters};
use noria::builder::Builder;


fn main() {
  let mut names: HashSet<String> = HashSet::new();

  // read in from a file
  let buffered = BufReader::new(File::open("info.txt")?);
  for line in buffered.lines() {
    names.insert(line.strip())
  }

  // Setting up Noria
  let  log = Logger::root(Mutex::new(term_full()).fuse(), o!());
  let mut b = Builder::default();
  b.set_sharding(None);
  b.log_with(log.clone());
  b.set_persistence(PersistenceParameters::new(
      DurabilityMode::MemoryOnly,
      Duration::from_millis(1),
      Some(String::from("websubmit")),
      1,
  ));

  let mut sh = b.start_simple().unwrap();
  thread::sleep( Duration::from_millis(200));

  let backend = Arc::new(Mutex::new(sh));
    
}
