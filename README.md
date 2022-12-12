# GGG-Rust

Ports of existing GGG code or additional utilities written in Rust.

## Features

There are some optional features of this crate that can be enabled using `cargo`'s `--features` flag,
e.g. `cargo build --features static`

* `static` - the netCDF library used by this crate can either link to an already built netCDF shared object
  library on your system *or* build its own netCDF and HDF5 libraries from source. The latter takes longer
  and requires `cmake` be installed on your system, but can get around some incompatible netCDF issues. 
  If you have trouble building this because of the netCDF requirement, try building with `--features static`.
