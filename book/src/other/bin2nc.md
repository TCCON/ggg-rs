# bin2nc

`bin2nc` is a utility to convert binary spectra into netCDF format.
This is intended to support publishing spectra if desired.

## Examples

Convert the spectra listed in the runlog `RUNLOG` to individual netCDF files in `OUTPUT_DIR`:

```bash
$GGGPATH/bin2nc $GGGPATH/runlogs/gnd/RUNLOG OUTPUT_DIR
```

This requires that the directories for the spectra be listed in `$GGGPATH/config/data_part.lst`.
If instead you want to indicate that the spectra can be found in `SPEC_DIR1` and `SPEC_DIR2` directly, use:

```bash
$GGGPATH/bin2nc --spec-dir SPEC_DIR1 --spec-dir SPEC_DIR2 $GGGPATH/runlogs/gnd/RUNLOG OUTPUT_DIR
```

To output a single netCDF file containing all the spectra from the runlog, use the `--single-file` flag:

```bash
$GGGPATH/bin2nc --single-file $GGGPATH/runlogs/gnd/RUNLOG OUTPUT_DIR
```

This does require that all the spectra for the same detector have the same frequency grid.

## Use in TCCON and EM27/SUN standard processing

`bin2nc` is not part of TCCON or EM27/SUN standard processing.
It is provided as a utility for users who wish to make their spectra more readily available.