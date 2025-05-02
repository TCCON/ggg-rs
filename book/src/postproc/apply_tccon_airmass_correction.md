# apply_tccon_airmass_correction

## Purpose

`apply_tccon_airmass_correction` does two things:

1. Converts gas column densities (in molecules per area) to column averages by dividing
   by the O2 column and multiplying by the mean O2 atmospheric dry mole fraction.
2. Applies a solar zenith angle-dependent correction to those column averages that require
   it, as defined by a configuration file.

This can operate either on individual windows' column densities or on column densities calculated
by averaging together all the windows for a given gas.
The former is considered to be more accurate, as it allows for different airmass dependence per window
(which depends on the spectroscopy in that window), but the latter is preserved for backwards compatibility.

## Examples

This program requires two arguments: a path to a file defining the airmass corrections and
a path to either a `.vsw` file (created by `collate_tccon_results`) or `.vav` file (created
by `average_results`):

```bash
$GGGPATH/bin/apply_tccon_airmass_correction CORRECTION_FILE VSW_OR_VAV_FILE
```

The `CORRECTION_FILE` will usually be one of those supplied with GGG, in the `$GGGPATH/tccon` subdirectory.
See [the configuration section](/postproc/corrections/airmass_correction_file.html) for the details of this
file's format if you need to modify one or create your own.

## Use in TCCON standard processing

For TCCON standard processing, the `CORRECTION_FILE` _must_ be `$GGGPATH/tccon/corrections_airmass_preavg.dat`,
as these are the airmass correction factors derived for the required TCCON data version.
It must be run on the `vsw` file output by `collate_tccon_results`, as the TCCON standard processing uses per-window
airmass corrections.
This is automatically configured in the `post_processing.sh` file, therefore standard users should rely on the
`post_processing.sh` script to run the required post processing steps in the correct order.

## Use in EM27/SUN standard processing

As of GGG2020, EM27/SUNs still use per-gas airmass corrections, rather than per-window.
Therefore, this must be run on the `.vav` file output by `average_results` using the EM27/SUN-specific
airmass corrections included with EGI-RS.
If using EGI-RS correctly, it will automatically create a `post_processing.sh` file with the correct
post processing order for an EM27/SUN, so normal users should rely on that.
