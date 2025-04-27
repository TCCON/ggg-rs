# write_public_netcdf

## Basic usage

`write_public_netcdf` converts the private (a.k.a. engineering) netCDF files into smaller, more user-friendly, files distributed to most TCCON users.
This includes:

- limiting the variables to the most useful,
- removing `flag != 0` data, 
- limiting the data in the file based on the desired data latency, and
- expanding the averaging kernels and prior profiles to be one-per-spectrum.

In the simplest use, running `write_public_netcdf PRIVATE_NC_FILE`, where `PRIVATE_NC_FILE` is a path to the private file you wish to convert, is all that is required.
For other options, use the `--help` flag.
If you require a custom configuration for what variables to copy, see the [Configuration](/write_public_netcdf/configuration.html) section.

### Use in TCCON standard processing

Individual TCCON sites **should not need to use this program** under normal circumstances.
This program will be run at Caltech on the concatenated and quality controlled private netCDF files, and the resulting public netCDF files will be made available through [tccondata.org](https://tccondata.org).
This function is provided as part of GGG-RS for sites that have, for example, low latency or custom products delivered to specific users as non-standard TCCON data, but wish to provide the data in the user-friendly public format instead of the much more intimidating private file format.
Presently, you will need to follow the instructions [on the TCCON wiki](https://tccon-wiki.caltech.edu/Main/GeneratingPublicFilesGGG2020) to generate a concatenated and quality controlled private file, then run this program on the resulting file.
Note that access permission is required for this wiki page to track who is generating GGG public files.

### Use in EM27/SUN standard processing

As there is not yet an equivalent of the TCCON data pipeline at Caltech for EM27/SUN data processed with GGG, operators will likely want to use this program to generate public files of their data for upload to whatever data repository they host from.
Presently, you will need to follow the instructions [on the TCCON wiki](https://tccon-wiki.caltech.edu/Main/GeneratingPublicFilesGGG2020) to generate a concatenated and quality controlled private file, then run this program on the resulting file.
Note that access permission is required for this wiki page to track who is generating GGG public files.