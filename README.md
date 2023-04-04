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
cargo install --features static --path . --root $GGGPATH
```

from the directory containing this README. This will install the programs in `$GGGPATH/bin`.

If you do not wish to install these programs alongside your standard GGG programs, you may pass a different
argument to `--root`. Note that it will always install in the `bin` subdirectory inside the path given as
the argument to `--root`.

## Features

There are some optional features of this crate that can be enabled using `cargo`'s `--features` flag,
e.g. `cargo build --features static`

* `static` - the netCDF library used by this crate can either link to an already built netCDF shared object
  library on your system *or* build its own netCDF and HDF5 libraries from source. The latter takes longer
  and requires `cmake` be installed on your system, but can get around some incompatible netCDF issues.
  We recommend using this feature in the `cargo install` command above because generally building the netCDF
  library this way gives fewer issues than trying to link to a system netCDF library. However, if you have
  a well-behaved system installation of netCDF, you can try installing without this feature.

## Compilation or runtime errors

### Any OS

**Failed custom build command for hdf5-sys or netcdf-sys**: this usually means that the compiler could not find the netCDF and/or HDF5 libraries on your
computer. This should only show up if you do not use `--features static` in the installation command. There are two solutions:

1. Install the netCDF C library via a system package manager (e.g. `apt` on Ubuntu/Debian, `brew` on Macs with [Homebrew](https://brew.sh/) installed).
  If running on a supercomputing cluster, check if there is a netCDF module you can load.
1. Have `cargo` build its own HDF5 and netCDF libraries by adding `--features static` to the `cargo install` command.
  Note that this requires `cmake` be installed on your system. 

**No libhdf5.so file**: if you get a message like:

```
error while loading shared libraries: libhdf5.so.101: cannot open shared object file: No such file or directory
```

while trying to run a program like `bin2nc`, this probably means that the system netCDF can't be linked to properly. We recommend
building with `--features static` to avoid this issue.

### Mac

**Missing xcrun**: if you get a failure to compile on Mac, check the earlier output for a line like:

```
note: xcrun: error: invalid active developer path (/Library/Developer/CommandLineTools), missing xcrun at: /Library/Developer/CommandLineTools/usr/bin/xcrun
```

If you see this, try reinstalling the developer tools using `xcode-select --install`. This can happen even if you had previously installed the developer tools, but then upgraded MacOS since then.


**ld linker error for netcdf**: if you get something like the following lines at the end of your compilation:

```
ld: warning: directory not found for option '-L$HOME/opt/homebrew/Cellar/netcdf/4.8.1/lib/lib'
ld: library not found for -lnetcdf
```

then this means that somehow your netCDF library path is not set correctly. The simplest fix is usually to use `--features static` as
described in the Features section.