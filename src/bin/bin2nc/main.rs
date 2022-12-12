use std::path::PathBuf;

use clap::Parser;
use ggg_rs;

#[derive(Debug, Parser)]
struct Cli {
    runlog: PathBuf
}

fn main() {
    let clargs = Cli::parse();
    let runlog = ggg_rs::runlogs::Runlog::open(&clargs.runlog).unwrap();
    for (i, data_rec) in runlog.into_iter().enumerate() {
        println!("Line {i}: {data_rec:?}\n");
    }
}
