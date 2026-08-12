# create_sunrun_rs

## Purpose

`create_sunrun_rs` generates a "sunrun" file for a list of spectra.
A sunrun file lists the location and other ancillary parameters about a list of spectra,
and is used to generate a runlog, which contains the information needed to run GFIT.

This supersedes the previous `create_sunrun` Fortran program.
The intent of that program was to provide a single, standardized program that served all users' needs.
However, there still remained cases where users needed more flexibility, resulting in continued proliferation
of alternative `create_sunrun_from_*` programs or other solutions.
`create_sunrun_rs` is a new attempt to provide enough flexibility in a configuration file to avoid the
need for multiple, site-specific programs.

## Examples

To create a sunrun for the Park Falls benchmark from the list found
in `current_results` directory:

```bash
$GGGPATH/bin/create_sunrun_rs $GGGPATH/install/current_results/pa_ggg_benchmark.gnd
```

This creates `$GGGPATH/sunruns/gnd/pa_ggg_benchmark.gop`.
The `gnd` subdirectory is inferred from the first letter
of the list extension ("g" = "gnd" for ground).
The sunrun takes the name of the list file with the extension
changed to "?op", where "?" is the first letter of the list
file extension.

The positional input is a list of spectra.
This can be created with [`list_spectra`](./list_spectra.md).
The list of spectra must be time ordered; note that if you have
two or more detectors, simply listing the spectra will not put them
in the correct order, since the spectra from the detectors must be
interleaved in time order.

It also requires a TOML file in `$GGGPATH/tccon`.
It will look for the file `$GGGPATH/tccon/??_sunrun.dat` where `??` is the
first two characters of the list file (`pa` in this example).
For information about the structure of this file, see
[the configuration section](./create_sunrun_rs/configuration.md)

```admonish note
If you used the Fortran version of `create_sunrun` before, you may be surprised
that this example can take a full path to the list file.
The Fortran version of the program used the first two characters of the input
argument itself to determine what `??_sunrun.dat` to look for.
This meant that passing, e.g. `/home/user/ggg/lists/pa_ggg_benchmark.gnd`
would try to find `/h_sunrun.dat` - not what you what!
The Rust version uses the first two letters of the list file's
_base name_, so passing a full path to that file will work as expected.
```


## Use in TCCON standard processing

`create_sunrun_rs` will replace `create_sunrun` in a future GGG version.
At that time, all TCCON sites will need to use this program before `create_runlog`.

## Use in EM27/SUN standard processing

Like TCCON, EM27/SUN users will adopt `create_sunrun_rs` as part of EGI in a future version.