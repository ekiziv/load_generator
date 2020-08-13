extern crate regex; 

use regex::Regex; 
use std::io::{BufRead, BufReader};
use std::fs::File;


fn main() {
    
    let output  = BufReader::new(
        File::open("/home/ekiziv/load_generator/output.txt")
            .unwrap(),
    );
    let mut actual = 0;
    let mut total = 0;
    let re = Regex::new(r"timer: (\d{0,10})").unwrap(); 
    for line in output.lines() {
        let unwrapped = line.unwrap().clone();
        if unwrapped.contains("Lease action") {
            actual += 1; 
        }
        if unwrapped.contains("timer") {
            for cap in  re.captures_iter(&unwrapped) {
                total = cap[1].parse().unwrap(); 
            }
        }
    }
    println!("actual: {}", actual); 
    println!("total: {}", total); 
    println!("percentage: {}", actual as f64 /total as f64); 


}
