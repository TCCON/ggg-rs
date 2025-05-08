# change_ggg_files

## Purpose

`change_ggg_files` adjusts the default settings for where to save Jacobian and spectral fit files in the `.ggg` files created by `gsetup`.
This can be used to either disable that output or redirect it into subdirectories organized by window.

## Examples

Assuming you are in the run directory containing your `.ggg` files, the following command will
edit both the averaging kernel and spectral fit lines to set the maximum number written to 0:

```bash
$GGGPATH/bin/change_ggg_files --spt-output-limit 0 --ak-output-limit 0
```

If instead you wanted to output the spectra fit files to an `./spt` subdirectory in the run directory
and organize that with further subdirectories by window and prefixed with a "z", you can use:

```bash
$GGGPATH/bin/change_ggg_files --make-output-dirs --spt-output-pattern "./spt/{WINDOW}/z"
```

The `--make-output-dirs` will create the directories needed, which GFIT itself does _not_ do.
`{WINDOW}` will be replaced with the window name, e.g. "o2_7885" for the O2 window.

## Use in TCCON and EM27/SUN standard processing

`change_ggg_files` is not part of standard processing.
It is provided as a utility program for GGG users who need to adjust the output of Jacobian and spectral fit files in bulk.
