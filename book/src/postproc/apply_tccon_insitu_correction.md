# apply_tccon_insitu_correction

## Purpose

`apply_tccon_insitu_correction` has a single purpose, that is to apply a scalar divisor scale factor
to specific column-average quantities.
This is typically used to ensure that these quantities are tied to the same metrological scale as
comparable _in situ_ data.

## Examples

This program requires two arguments: a path to a file defining the scaling corrections and
a path to either a `.vav.ada` file, created by `apply_tccon_airmass_correction`:

```bash
$GGGPATH/bin/apply_tccon_insitu_correction CORRECTION_FILE VAV_ADA_FILE
```

The `CORRECTION_FILE` will usually be one of those supplied with GGG, in the `$GGGPATH/tccon` subdirectory.
See [the configuration section](/postproc/corrections/insitu_correction_file.html) for the details of this
file's format if you need to modify one or create your own.

## Use in TCCON standard processing

For TCCON standard processing, the `CORRECTION_FILE` _must_ be `$GGGPATH/tccon/corrections_insitu_postavg.dat`,
as these are the _in situ_ correction factors derived for the required TCCON data version.
It must be run on the `.vav.ada` file output by `apply_tccon_airmass_correction`.
This is automatically configured in the `post_processing.sh` file, therefore standard users should rely on the
`post_processing.sh` script to run the required post processing steps in the correct order.

## Use in EM27/SUN standard processing

EM27/SUNs have their own _in situ_ correction factors.
These correction factors are provided with EGI-RS, and automatically added to the `$GGGPATH/tccon` directory
when running the `em27-init` program included with EGI-RS.
If using EGI-RS correctly, it will automatically create a `post_processing.sh` file with the correct
post processing order for an EM27/SUN, so normal users should rely on that.
