use std::path::PathBuf;

mod template_strings;
mod config;

fn main() {

}

struct Cli {
    private_nc_file: PathBuf,
    config_file: Option<PathBuf>,
}