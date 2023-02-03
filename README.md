# GGG-Rust

Ports of existing GGG code or additional utilities written in Rust.

## Installation

### Rust

First, check if you have a Rust toolchain installed on your system by running `rustup show`.
If you see output like this:

```
Default host: x86_64-apple-darwin
rustup home:  /Users/laughner/.rustup

installed toolchains
--------------------

stable-x86_64-apple-darwin (default)
1.61.0-x86_64-apple-darwin

active toolchain
----------------

stable-x86_64-apple-darwin (default)
rustc 1.66.0 (69f9c33d7 2022-12-12)
```

with something listed under "active toolchain", then you do have Rust installed. If not, or if
you get something similar to `rustup: command not found`, then you will need to install Rust.

To install `rustup`, follow [these instructions](https://www.rust-lang.org/tools/install).

### GGG-RS

To install the programs in this repo, clone the repo. We recommend placing it in your GGGPATH to
keep all GGG code together, so assuming you have the `GGGPATH` environmental variable set, do:

```
git clone git@github.com:TCCON/ggg-rs.git $GGGPATH/src-rs
```

For now, because this is a private repo, you must have an SSH key pair to authenticate to GitHub 
configured to work for the github.com domain. 

Once the repo is cloned, you can install the programs from it under your GGGPATH alongside the 
standard Fortran programs, by running:

```
cargo install --path . --root $GGGPATH
```

from the directory containing this README.


## Features

There are some optional features of this crate that can be enabled using `cargo`'s `--features` flag,
e.g. `cargo build --features static`

* `static` - the netCDF library used by this crate can either link to an already built netCDF shared object
  library on your system *or* build its own netCDF and HDF5 libraries from source. The latter takes longer
  and requires `cmake` be installed on your system, but can get around some incompatible netCDF issues. 
  If you have trouble building this because of the netCDF requirement, try building with `--features static`.

## Compilation errors

### Mac

**Missing xcrun**: if you get a failure to compile on Mac, check the earlier output for a line like:

```
note: xcrun: error: invalid active developer path (/Library/Developer/CommandLineTools), missing xcrun at: /Library/Developer/CommandLineTools/usr/bin/xcrun
```

If you see this, try reinstalling the developer tools using `xcode-select --install`. This can happen even if you had previously installed the developer tools, but then upgraded MacOS since then.