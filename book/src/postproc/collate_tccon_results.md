# collate_tccon_results

## Basic usage

`collate_tccon_results` combines output from the various `.col` files in a GGG run directory with ancillary data from the runlog and `.ray` file into a single file.
It also computed retrieved quantities from the `.col` files if needed.
Which `.col` files are read is determined by a `multiggg.sh` file, which is expected to contain calls to `gfit` (one per line) for each window processed.
An example of a the first few lines of a `multiggg.sh` file is:

```text
/home/user/ggg/bin/gfit luft_6146.pa_ggg_benchmark.ggg>/dev/null
/home/user/ggg/bin/gfit hf_4038.pa_ggg_benchmark.ggg>/dev/null
/home/user/ggg/bin/gfit h2o_4565.pa_ggg_benchmark.ggg>/dev/null
/home/user/ggg/bin/gfit h2o_4570.pa_ggg_benchmark.ggg>/dev/null
```

Unlike the standard `collate_results`, `collate_tccon_results` does not rely on the ZPD times of spectra to determine whether successive spectra in the runlog
(from different detectors) should have their outputs grouped into a single line in the output file.
Instead, it uses the spectrum names.

## Examples

The most common way to run this is from inside a GGG run directory (i.e., a directory containing the `multiggg.sh`, `.ggg`, `.mav`, and `.ray` files created by `gsetup`).
In that case, you will call it with a single positional argument, `v` to create a file containg vertical column densities or `t` to create one containing VMR scale factors.
The output file will have the same name as the runlog pointed to in the `.ggg` and `.col` files, with the extension `.vsw` or `.tsw`:

```bash
$GGGPATH/bin/collate_tccon_results v
```

If you need to run this program from outside of a GGG run directory, you can use the `--multiggg-file` option to point to the `multiggg.sh` file to read windows from.
In this case, the output will be written to the same directory as the `multiggg.sh` file:

```bash
$GGGPATH/bin/collate_tccon_results --multiggg-file /data/ggg/xx20250101_20250301/multiggg.sh
```

This program relies on being able to determine a "primary" detector in order to know which spectra represent a new observation.
If you have a nonstandard setup that does not use "a" as the character in the spectrum name to represent the InGaAs detector, you can use
the `--primary-detector` option to specify a different character.
For example, if it should look for spectra with "g" as the detector indicator:

```bash
$GGGPATH/bin/collate_tccon_results --primary-detector g v
```

## Use in TCCON standard processing

Most users will use this as part of running the `post_processing.sh` script to create the initial `.vsw` and `.tsw` files.
However, in any case, it will always be the first program run after GFIT as the other post processing programs need a post
processed file as input (i.e., they do not read from the `.col` files.)

## Use in EM27/SUN standard processing

`collate_tccon_results` is used identically when processing EM27/SUN data as when processing TCCON data.
Unlike with the Fortran version of `collate_results`, it does not need adapted to account for the shorter time between successive observations.

