# query_output

`query_output` is a utility to quickly check values in a GGG post-processing output file, e.g.
a file with an extension beginning with `.vsw`, `.tsw`, `.vav`, or `.tav`.

## Examples

To print the `day`, `xco2`, and `xco2_error` columns from the `pa_ggg_benchmark.vav.ada.aia` file created during the post-install test:

```bash
$GGGPATH/bin/query_output $GGGPATH/install/current_results/pa_ggg_benchmark.vav.ada.aia day xco2 xco2_error
```

## Limitations

Currently, this must be reading a GGG2020.1 post-processing file (i.e., one that includes an "o2dmf" auxiliary column).

## Use in TCCON or EM27/SUN standard processing

`query_output` is not used in either TCCON or EM27/SUN standard processing.