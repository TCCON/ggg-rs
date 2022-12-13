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
        let spec = ggg_rs::opus::read_spectrum_from_runlog_rec(&data_rec).unwrap();
        println!(
            "Read spectrum {} {}, first point = ({}, {}), tenth point = ({}, {})",
            i+1, &data_rec.spectrum_name,
            spec.freq[0], spec.spec[0],
            spec.freq[9], spec.spec[9]
        );
    }
}
