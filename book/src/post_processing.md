# Post processing programs

Post processing programs are used after completing GFIT runs on all windows.
These programs perform a combination of collating and averaging data from the different
windows and applying the necessary _post hoc_ corrections to produce the best quality data.

We are currently in a transitional period, where the post processing programs are still
provided in a mix of languages.
Some remain the original Fortran versions from GGG2020, others have been replaces with Rust
versions, and the private netCDF writer remains written in Python.
The intention is to transition away from the Python netCDF writer in GGG2020.2,
but to retain a mix of Fortran and Rust programs at that time.
Whether all post processing programs transition to Rust depends on whether there is a need
for more flexibility in all programs.

## EM27/SUN users

Those who use GGG to process EM27/SUN data must be aware that EGI (the wrapper program to
streamline processing of EM27/SUN data with GGG) is _also_ in a transitional phase.
The [original EGI](https://tccon-wiki.caltech.edu/Main/EGI) does not use GGG-RS programs,
and instead patches some of the existing GGG Fortran programs to work with EM27/SUN data,
as well as works around some limitations of the Fortran post processing code by swapping
out some of the configuration files on disk.
This works, but is inconvenient when you need to process both TCCON and EM27/SUN data.

A full rewrite of EGI, [EGI-RS](https://github.com/TCCON/egi-rs) is in progress.
This is intended to be easier to maintain and modular, allowing smaller parts of the
GGG workflow to be run independently with EGI-RS automation as needed.
EGI-RS _does_ use the programs provided by GGG-RS, and in fact relies on several of them
to simplify switching between TCCON and EM27/SUN configurations.
Throughout this section, the EM27/SUN standard use sections will be referring to the EGI-RS
use.
Those still using the original EGI should be aware that while the _role_ of each program
in the EM27/SUN post processing is the same as its original Fortran predecessor, the
specifics of how it is run may differ.
