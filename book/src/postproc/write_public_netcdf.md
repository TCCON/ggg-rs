# write_public_netcdf

## Purpose

`write_public_netcdf` converts the private (a.k.a. engineering) netCDF files into smaller, more user-friendly, files distributed to most TCCON users.
This includes:

- limiting the variables to the most useful,
- removing `flag != 0` data, 
- optionally limiting the data in the file based on the desired data latency, and
- expanding the averaging kernels and prior profiles to be one-per-spectrum.

```admonish warning
If you previously used the Python netCDF writer, you may be used to it defaulting to respecting a data latency (a.k.a. release lag)
defined in a site information JSON file.
This version of the netCDF writer defaults to no data latency; that is, it assumes that you want to include all data from the given
private file in the new public file.
See the examples below for how to apply a data latency to withhold the newest data from the public file.
```

## Examples

The simplest use will convert the `PRIVATE_NC_FILE` into a public format file.
This assumes that the `PRIVATE_NC_FILE` filename begins with the two-character site ID for your site.
The public file will be in the same directory as the private file, and its name will reflect the site ID and the date range of the `flag == 0` data:

```bash
$GGGPATH/bin/write_public_netcdf PRIVATE_NC_FILE
```

To avoid renaming the public file to match the dates of `flag == 0` data, use the `--no-rename-by-dates` flag.
This will replace "private" in the extension with "public", so if `PRIVATE_NC_FILE` was `pa_ggg_benchmark.private.qc.nc`, the public file would be named `pa_ggg_benchmark.public.qc.nc`:

```bash
$GGGPATH/bin/write_public_netcdf --no-rename-by-dates PRIVATE_NC_FILE
```

Both of the above examples will use the standard TCCON configuration for which variables to copy.
To use the extended TCCON configuration (which will include gases from the secondary detector), add the `--extended` flag:


```bash
$GGGPATH/bin/write_public_netcdf --extended PRIVATE_NC_FILE
```

If you need to customize which variables are copied, you must create your own configuration TOML file and pass it to the `--config` option:


```bash
$GGGPATH/bin/write_public_netcdf --config CUSTOM_CONFIG.toml PRIVATE_NC_FILE
```

For information on the configuration file format, see [its section of this book](/write_public_netcdf/configuration.html).

To withhold the newest data from the public file, you can use the `--data-latency-date` or `--data-latency-days` options
to specify either a number of days in the past from today or a specific date after which to withhold data.

```bash
$GGGPATH/bin/write_public_netcdf --data-latency-date 2025-01-01 PRIVATE_NC_FILE
$GGGPATH/bin/write_public_netcdf --data-latency-days 120 PRIVATE_NC_FILE
```

The first one will withhold data with a ZPD time after midnight UTC on 1 Jan 2025 from the public file.
The second will withhold data with a ZPD time after midnight UTC 120 days ago from the public file:
if run on 1 May 2025 (UTC), this would also have 1 Jan 2025 as the cutoff date.

## Use in TCCON standard processing

Individual TCCON sites **should not need to use this program** under normal circumstances.
This program will be run at Caltech on the concatenated and quality controlled private netCDF files, and the resulting public netCDF files will be made available through [tccondata.org](https://tccondata.org).
This function is provided as part of GGG-RS for sites that have, for example, low latency or custom products delivered to specific users as non-standard TCCON data, but wish to provide the data in the user-friendly public format instead of the much more intimidating private file format.
Presently, you will need to follow the instructions [on the TCCON wiki](https://tccon-wiki.caltech.edu/Main/GeneratingPublicFilesGGG2020) to generate a concatenated and quality controlled private file, then run this program on the resulting file.
Note that access permission is required for this wiki page to track who is generating GGG public files.

## Use in EM27/SUN standard processing

As there is not yet an equivalent of the TCCON data pipeline at Caltech for EM27/SUN data processed with GGG, operators will likely want to use this program to generate public files of their data for upload to whatever data repository they host from.
Presently, you will need to follow the instructions [on the TCCON wiki](https://tccon-wiki.caltech.edu/Main/GeneratingPublicFilesGGG2020) to generate a concatenated and quality controlled private file, then run this program on the resulting file.
Note that access permission is required for this wiki page to track who is generating GGG public files.
