extern crate hdrhistogram;
use chrono::{NaiveDateTime, Utc};
use hdrhistogram::Histogram;
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
    let mut cdf = OpenOptions::new()
        .write(true)
        .create(true)
        .open("cdf.txt")
        .unwrap();

    let mut hist = Histogram::<u64>::new(1).unwrap();

    // // read the times when the unsubscription happens
    // let (un_start, un_end) = extract_action_time_interval(
    //     "/Users/eleonorakiziv/rust/websubmit-rs/websubmit-rs/un_times.txt",
    // );
    // let (re_start, re_end) = extract_action_time_interval(
    //     "/Users/eleonorakiziv/rust/websubmit-rs/websubmit-rs/re_times.txt",
    // );

    // get all the end_times
    let mut end_times: Vec<(NaiveDateTime, NaiveDateTime)> = Vec::new();
    let buffered_end_times = BufReader::new(
        File::open("/home/ekiziv/load_generator/end_times.txt")
            .unwrap(),
    );
    for line in buffered_end_times.lines() {
        let unwrapped = line.unwrap().clone();
        let start = unwrapped.split('#').nth(0).unwrap();
        let end = unwrapped.split('#').nth(1);
        if end.is_none() {
            continue;
        }
        let start_time = NaiveDateTime::parse_from_str(start, "%Y-%m-%dT%H:%M:%S%.f"); 
        if start_time.is_err() {
            continue; 
        }
        let end_time = NaiveDateTime::parse_from_str(end.unwrap(), "%Y-%m-%dT%H:%M:%S%.f"); 
        if end_time.is_err() {
            continue; 
        }
        end_times.push((start_time.unwrap(), end_time.unwrap()));
    }

    let mut prev = end_times[0].0;
    // let mut start_un_recorded = false;
    // let mut end_un_recorded = false;
    // let mut start_re_recorded = false;
    // let mut end_re_recorded = false;

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
        hist += latency as u64;

        // if un_start < start {
        //     if !start_un_recorded {
        //         write!(&mut latency_file, "{}\n", 0).expect("failed to write into latency file");
        //         start_un_recorded = true;
        //         println!("recorded start un next to {:?}", start);
        //     }
        //     if un_end < end && !end_un_recorded {
        //         write!(&mut latency_file, "{}\n", 0).expect("failed to write into latency file");
        //         end_un_recorded = true;
        //         println!(
        //             "recorded end un e_time is {:?} and end is {:?}",
        //             un_end, end
        //         );
        //     }
        // }
        // if re_start < start {
        //     if !start_re_recorded {
        //         write!(&mut latency_file, "{}\n", 0).expect("failed to write into latency file");
        //         start_re_recorded = true;
        //         println!("recorded start re next to {:?}", start);
        //     }
        //     println!("latency: {:?}", latency);
        //     if re_end < end && !end_re_recorded {
        //         write!(&mut latency_file, "{}\n", 0).expect("failed to write into latency file");
        //         end_re_recorded = true;
        //         println!(
        //             "recorded end re e_time is {:?} and end is {:?}",
        //             re_end, end
        //         );
        //     }
        // }

        write!(&mut latency_file, "{}\n", latency).expect("failed to write into latency file");
        write!(&mut interval_file, "{}\n", interval).expect("failed to write into interval file");
    }
    println!("# of samples: {}", hist.len());
    println!("95'th percentile: {}", hist.value_at_quantile(0.95));
    for v in hist.iter_recorded() {
        write!(
            &mut cdf,
            "{}*{}*{}\n", 
            v.value_iterated_to(),
            v.percentile(),
            v.count_at_value()
        )
        .expect("failed to write to cdf");
    }
}

fn extract_action_time_interval(file: &str) -> (NaiveDateTime, NaiveDateTime) {
    let mut unsub = Vec::new();
    let unsub_times = BufReader::new(File::open(file).unwrap());
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
    (s_time, e_time)
}
