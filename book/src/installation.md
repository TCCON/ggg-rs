# Overview

GGG-RS is an extension to [the GGG retrieval](https://github.com/TCCON/GGG) that provides updated version of several of the post-processing programs.
The long term intention is to also make this a library of functions useful for working with GGG-related files.

## Installation

At present, installing GGG-RS requires that you be able to build it from source.
This requires, at a minimum, a [Rust toolchain](https://rustup.rs/) installed.
If you wish to compile the programs that work with netCDF files, you will also need either

- one of the `micromamba`, `mamba`, or `conda` package managers, or
- the `cmake` build tool.

These are necessary to install or build the netCDF and HDF5 C libraries.
Detailed instructions and installation options are provided in the [README](https://github.com/TCCON/ggg-rs).

## Documentation

This book primarily focues on the command line programs provided by GGG-RS.
As the library is made available, the APIs will be documented through [docs.rs](https://docs.rs/) (for the Rust library) and `readthedocs.io` (for the Python library).

Each command line program will provide help when given the `-h` or `--help` flags.
That help should be your first resource to understand how the programs work, as it will be the most up-to-date.
The chapters in this book go into more detail about advanced usage of the programs.
If you find something in this book that is out of date or unclear, please [open an issue](https://github.com/TCCON/ggg-rs/issues) with the `documentation` tag.