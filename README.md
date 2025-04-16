# GGG-Rust

Ports of existing GGG code or additional utilities written in Rust.

## Installation

### Rust

First, check if you have a Rust toolchain installed on your system by running `rustup show`.
If you see output like this:

```
Default host: x86_64-unknown-linux-gnu
rustup home:  /home/user/.rustup

installed toolchains
--------------------

stable-x86_64-unknown-linux-gnu (default)
1.61.0-x86_64-unknown-linux-gnu

installed targets for active toolchain
--------------------------------------

x86_64-unknown-linux-gnu
x86_64-unknown-linux-musl

active toolchain
----------------

stable-x86_64-unknown-linux-gnu (default)
rustc 1.81.0 (eeb90cda1 2024-09-04)
```

with something listed under "active toolchain", then you do have Rust installed. If not, or if
you get something similar to `rustup: command not found`, then you will need to install Rust.
(Note: the version number is not critical.)

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
standard Fortran programs, by running `make` from the directory containing this README.
This will install the programs in `$GGGPATH/bin`.

Note that you will see warning similar to the following at the end of the compilation:
```
warning: be sure to add `$GGGPATH/bin` to your PATH to be able to run the installed binaries
```
You can ignore this warning; it is issued because the Rust compiled assumes that you want to be
able to run the programs it installed without giving the full path to them.
Since we call GGG programs with the full path, e.g. `$GGGPATH/bin/gsetup`, this is not an issue.

### Install options

There are a number of environmental variables that you can set to modify how GGG-RS is installed.

- `GGGRS_FEATURES`: controls which programs are compiled. See the **Features** paragraph next, and the
[Features](#features) section below.
- `GGGRS_NCDIR`: controls how the netCDF and HDF5 libraries are linked to the programs that need them.
See the **HDF5/netCDF libraries** paragraphs later in this section.
- `GGG_ENV_TOOL`: controls which tool (`micromamba`, `mamba`, `conda`) is used to create a conda/mamba
environment containing the HDF5 and netCDF libraries.
See the **HDF5/netCDF libraries** paragraphs later in this section.

**Features:** The `GGGRS_FEATURES` variable can be set to override what components of GGG-RS are built and installed.
It should be a comma-separated list of the feature names in the [Features](#features) section, below.
Note that the `static` feature is controlled by the value of `GGGRS_NCDIR`, so you should not add it to this list in normal use cases.
By default `GGGRS_FEATURES` will be `"netcdf"` to include building programs that work with netCDF files.
To include plotting programs as well, you would set `GGGRS_FEATURES=netcdf,plotting`.
Or, to disable the netCDF programs, set `GGGRS_FEATURES=""`.

**HDF5/netCDF libraries:** To compile several programs that work with netCDF files, GGG-RS needs the HDF5 and netCDF libraries.
Broadly speaking, there are two ways to provide them:

1. have them installed on your system, or
2. allow GGG-RS to compile the libraries from source.

The first option is preferred, as gives faster compilations.
To support that, we offer various options to install these libraries in conda/mamba environments.
The second option may be preferred if the first option fails.
However, it requires that you have `cmake` installed on your system and available on your `PATH`.

The `GGGRS_NCDIR` variable controls this.
The logic is as follows:

- If `GGGRS_NCDIR` is not set, then we first look for an environment at `$GGGPATH/install/.condaenv`.
If that exists, we use the HDF5 and netCDF libraries under there.
If not, then we will create an environment at `.condaenv` within this repo.
- If `GGGRS_NCDIR` equals `"AUTO"`, then we always create a `.condaenv` environment under this repo
and use that.
- If `GGGRS_NCDIR` equals `"STATIC"`, then we will build the libraries from source as part of
compiling GGG-RS. This requires `cmake` to be available on your system.
- Otherwise, `GGGRS_NCDIR` is assumed to be a path to a directory containing `lib/libhdf5*` and
`lib/libnetcdf*`.

If we need to create an environment under this repo, then we have the option to use `micromamba`, `mamba`, or `conda`.
If `GGG_ENV_TOOL` is not specified, we try the tools in that order.
Otherwise, `GGG_ENV_TOOL` should be equal to whichever of those tools you want to use, e.g. to
force this to use `conda`, set `GGG_ENV_TOOL=conda`.

### Advanced GGG-RS install


If you need more control over how the installation is done, you can call `cargo` directly.
The following command is a starting point:

```
cargo install --features netcdf --path . --root $GGGPATH
```

If you do not wish to install these programs alongside your standard GGG programs, you may pass a different
argument to `--root`. Note that it will always install in the `bin` subdirectory inside the path given as
the argument to `--root`.

Note that if you do _not_ include the `static` feature, then the HDF5 and netCDF libraries must be discoverable
on your system.
The Rust wrappers for these libraries will try to find them, but if that fails, you will get a compilation error.
To fix that, check the `Makefile` for how the `HDF5_DIR`, `NETCDF_DIR`, and `RUSTFLAGS` environmental variables are defined.
In general, `HDF5_DIR` must point to a directory that has `lib/libhdf5.so` under it, and `NETCDF_DIR` must likewise
point to a directory with `lib/libnetcdf.so` under it.
`RUSTFLAGS` must then be set to add this directory to the runtime path of the programs to ensure that the
shared libraries are found at runtime.

If you choose to use this route, please be aware that our ability to support customized installations is minimal.

## Features

There are some optional features of this crate that can be enabled using `cargo`'s `--features` flag,
e.g. `cargo build --features static`

* `static`: the netCDF library used by this crate can either link to an already built netCDF shared object
  library on your system *or* build its own netCDF and HDF5 libraries from source. The latter takes longer
  and requires `cmake` be installed on your system, but can get around some incompatible netCDF issues.
  We recommend using this feature in the `cargo install` command above because generally building the netCDF
  library this way gives fewer issues than trying to link to a system netCDF library. However, if you have
  a well-behaved system installation of netCDF, you can try installing without this feature.
* `plotting`: this requires some additional dependencies, but will also compile programs that allow you to
  plot some GGG output files. (Currently, the only plotting program is `plot-spt` for spectral fit files.)
* `inprogress`: including this feature will compile programs that are not complete.
  It is intended for developers who need to test these programs; normal users should not activate this feature
  as the programs gated behind it will produce incomplete or unvalidated output.

## Compilation or runtime errors

### Any OS

#### Failed custom build command for hdf5-sys or netcdf-sys

This usually means that the compiler could not find the netCDF and/or HDF5 libraries on your computer.
See the [Install options](#install-options) section for ways to specify the location of the libraries or install them.


#### No libhdf5.so file

If you get a message like:
```
error while loading shared libraries: libhdf5.so.101: cannot open shared object file: No such file or directory
```
while trying to run a program like `bin2nc`, this probably means that the system netCDF can't be linked to properly.
This may happen if the binaries do not have the search path correctly embedded in them.
You can test this by activating the environment that these libraries were installed into and trying to run the program again.
If that works, then this is indeed the problem.

Usually, this should not happen if you built the programs using `make`.
If you ran `cargo` yourself, then this likely means that your `RUSTFLAGS` environmental variable was not correctly set.
Ensure that you set `RUSTFLAGS` as shown in the `Makefile`.
If you did compile with `make`, please [open an issue](https://github.com/TCCON/ggg-rs/issues) and include the output of the `uname -a` command.

### Mac

#### Missing xcrun
If you get a failure to compile on Mac, check the earlier output for a line like:

```
note: xcrun: error: invalid active developer path (/Library/Developer/CommandLineTools), missing xcrun at: /Library/Developer/CommandLineTools/usr/bin/xcrun
```

If you see this, try reinstalling the developer tools using `xcode-select --install`. This can happen even if you had previously installed the developer tools, but then upgraded MacOS since then.


#### ld linker error for netcdf
If you get something like the following lines at the end of your compilation:

```
ld: warning: directory not found for option '-L$HOME/opt/homebrew/Cellar/netcdf/4.8.1/lib/lib'
ld: library not found for -lnetcdf
```

this means that somehow your netCDF library path is not set correctly.
If this happens while compiling with `make`, please [open an issue](https://github.com/TCCON/ggg-rs/issues).
