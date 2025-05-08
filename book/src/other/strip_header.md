# strip_header

## Purpose

`strip_header` interprets the first line of a GGG input or output file as starting with the number of header lines,
and uses that to print everything after the header (by default) or just the header.
This is intended to help with command line workflows to concatenate multiple GGG files.

## Examples

Assume you want to concatenate the entries in `RUNLOG2` to the end of `RUNLOG1`.
You do so with:

```bash
$GGGPATH/bin/strip_header RUNLOG2 >> RUNLOG1
```

This uses the standard shell append operator, `>>`, to append the output from the `strip_header` command
to `RUNLOG1`.

## Use in TCCON or EM27/SUN standard processing

`strip_header` is not used in TCCON or EM27/SUN standard processing.