//! Command line entry point for ggg-rs source generation.
//!
//! Usage: `xtask sourcegen [--check]`. Without `--check`, regenerates any stale
//! modules; with it, only reports them and exits with a nonzero status if any
//! are stale.
use std::process::ExitCode;

const USAGE: &str = "usage: xtask sourcegen [--check]";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let check_only = match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["sourcegen"] => false,
        ["sourcegen", "--check"] => true,
        _ => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };

    let units_dir = xtask::units_dir();
    let result = if check_only {
        xtask::check(&units_dir)
    } else {
        xtask::update(&units_dir)
    };

    match result {
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
        Ok(paths) if check_only && !paths.is_empty() => {
            eprintln!("These units modules are out of sync with the template:");
            for p in paths {
                eprintln!("  {}", p.display());
            }
            eprintln!("Run `make sourcegen` to regenerate them.");
            ExitCode::FAILURE
        }
        Ok(paths) => {
            if paths.is_empty() {
                println!("All units modules are up to date.");
            }
            for p in paths {
                println!("regenerated {}", p.display());
            }
            ExitCode::SUCCESS
        }
    }
}
