# average_tccon_results

## Purpose

`average_tccon_results` averages together multiple windows of the same target gas together.
For example, we retrieve CO<sub>2</sub> in two windows, centered on 6220 cm<sup>-1</sup> and 6339 cm<sup>-1</sup>.
The XCO<sub>2</sub> product is the average of these two windows.

Compared to the generic `average_results`, `average_tccon_results` has built in defaults to avoid averaging
windows from different detectors.
That is, it will not average the CO window at 4290 cm<sup>-1</sup> measured on the InGaAs detector with
the mid-IR windows around 2100 cm<sup>-1</sup> measured on an InSb detector.

## Examples

Use this by calling it with an `?sw` file as the sole argument.
For example, to average Xgas values, do:

```bash
$GGGPATH/bin/average_tccon_results pa_ggg_benchmark.vsw.ada
```

which will output `pa_ggg_benchmark.vav.ada`.

This can also apply to column densities in a `.vsw` file:

```bash
$GGGPATH/bin/average_tccon_results pa_ggg_benchmark.vsw
```

outputting `pa_ggg_benchmark.vav`.

This can also be applied to non-column values like VSFs:

```bash
$GGGPATH/bin/average_tccon_results pa_ggg_benchmark.tsw
```

## Use in TCCON standard processing

TCCON standard processing calculates average column densities from the `.vsw` file,
VSFs from the `.tsw` file, and column averages from the `.vsw.ada` file.
It also requires that you use the fixed window-to-window scale factors, which must
be in the header of the `.Xsw` file as a line beginning with `sf=`.

## Use in EM27/SUN standard processing

EM27/SUN standard processing is similar to TCCON, but in GGG2020, the EM27/SUNs still
use the GGG2014 order of averaging before airmass correcting.
Therefore, EGI overrides the normal post processing to only run averaging on the `.vsw`
and `.tsw` files.
Note that GGG2020 assumes that the window-to-window scale factors should be the same for
TCCON and EM27/SUNs.
This should be tested in the future with more EM27/SUN data available.
