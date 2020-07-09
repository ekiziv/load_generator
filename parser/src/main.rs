use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Utc};
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::io::{BufRead, BufReader};

fn main() {
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

    // read the times when the unsubscription happens
    let mut unsub = Vec::new();
    let unsub_times = BufReader::new(
        File::open("/Users/eleonorakiziv/rust/websubmit-rs/websubmit-rs/un_times.txt").unwrap(),
    );
    for line in unsub_times.lines() {
        unsub.push(line.unwrap());
    }

    let mut s_time: NaiveDateTime = Utc::now().naive_local();
    let mut e_time: NaiveDateTime = Utc::now().naive_local();
    let mut i = 0;
    let line_count = unsub.len();
    for line in unsub.into_iter() {
        if i == 0 {
            let start = line.split('#').nth(0).unwrap();
            println!("Start is {:?}", start);
            s_time = NaiveDateTime::parse_from_str(start, "%Y-%m-%dT%H:%M:%S%.f")
                .expect("failed to parse from string");
        } else if i == line_count - 1 {
            let end = line.split('#').nth(1).unwrap();
            println!("End is {:?}", end);
            e_time = NaiveDateTime::parse_from_str(end, "%Y-%m-%dT%H:%M:%S%.f")
                .expect("failed to parse from string end");
        }
        i += 1
    }
    println!("Done parsing unsub");

    // get all the end_times
    let mut end_times: Vec<(NaiveDateTime, NaiveDateTime)> = Vec::new();
    let buffered_end_times = BufReader::new(
        File::open("/Users/eleonorakiziv/rust/websubmit-rs/load_generator/end_times.txt").unwrap(),
    );
    for line in buffered_end_times.lines() {
        let unwrapped = line.unwrap().clone();
        let start = unwrapped.split('#').nth(0).unwrap();
        let start_time = NaiveDateTime::parse_from_str(start, "%Y-%m-%dT%H:%M:%S%.f")
            .expect("failed to parse from string");

        let end = unwrapped.split('#').nth(1).unwrap();
        let end_time = NaiveDateTime::parse_from_str(end, "%Y-%m-%dT%H:%M:%S%.f")
            .expect("failed to parse from string");
        end_times.push((start_time, end_time));
    }
    println!("Done parsing end_times");

    let mut prev = end_times[0].0;
    let mut start_recorded = false;
    let mut end_recorded = false;
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
        if s_time.lt(&start) && !start_recorded {
            write!(&mut latency_file, "{}\n", 0).expect("failed to write into latency file");
            start_recorded = true;
            println!(
                "recorded start, s_time is {:?} and start is {:?}",
                s_time, start
            );
        }
        if e_time.lt(&end) && !end_recorded {
            write!(&mut latency_file, "{}\n", 0).expect("failed to write into latency file");
            end_recorded = true;
            println!("recorded end, e_time is {:?} and end is {:?}", e_time, end);
        }
        let latency = end.signed_duration_since(start).num_milliseconds();
        write!(&mut latency_file, "{}\n", latency).expect("failed to write into latency file");
        write!(&mut interval_file, "{}\n", interval).expect("failed to write into interval file");
    }
}
