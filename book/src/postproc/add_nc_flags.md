# add_nc_flags

## Purpose

`add_nc_flags` is an additional program that can be used to add additional flags to a private netCDF file.
This is intended to _supplement_ the default flagging done by `write_netcdf`, which reflects both the permitted
value ranges defined in your site's `??_qc.dat` and `??_manual_flagging.dat` files (found under `$GGGPATH/tccon`).
In particular, this is useful if you need to include more complex logic, such as only flagging based on a combination
of variables.

## Examples

### Quick flagging

The `quick` subcommand allows you to specify the filter criteria based on a single variable via the command line.
This first example will flag any data where the residual in the O2 window is above 0.5, and will modify the existing
netCDF file:

```bash
$GGGPATH/bin/add_nc_flags quick \
  --in-place \
  --filter-var o2_7885_rmsocl \
  --greater-than 0.5 \
  --nc-file PRIVATE_NC_FILE
```

Note that we have separated the command into multiple lines solely for readability; you can write this as a single line.

If instead you did not want to modify the existing netCDF file, but instead create a copy, use the `--output` flag:

```bash
$GGGPATH/bin/add_nc_flags quick \
  --output NEW_NC_FILE \
  --filter-var o2_7885_rmsocl \
  --greater-than 0.5 \
  --nc-file PRIVATE_NC_FILE
```

This will not create `NEW_NC_FILE` if no data needed to be flagged.
If you want to enforce that a new file is always created, add the `--always-copy` flag:

```bash
$GGGPATH/bin/add_nc_flags quick \
  --output NEW_NC_FILE \
  --always-copy \
  --filter-var o2_7885_rmsocl \
  --greater-than 0.5 \
  --nc-file PRIVATE_NC_FILE
```

If you wanted to limit the flags to a specific time period, use `--time-less-than` and `--time-greater-than`.
Note that the values must be given in UTC:


```bash
$GGGPATH/bin/add_nc_flags quick \
  --output NEW_NC_FILE \
  --filter-var o2_7885_rmsocl \
  --greater-than 0.5 \
  --time-greater-than 2025-04-01T12:00 \
  --time-less-than 2025-05-01 \
  --nc-file PRIVATE_NC_FILE
```

This will only apply flags data with a ZPD time greater than or equal to 12:00Z on 1 Apr 2025 and less than or equal to
00:00Z on 1 May 2025.
Note that in the less than argument we omit the hour and minute.

There are many more options, see the command line help for a full list.

### TOML-based flagging

For more complicated flagging, you can define your flagging criteria in a [TOML file](https://toml.io/en/).
You can create an example file with the `toml-template` subcommand:

```bash
$GGGPATH/bin/add_nc_flags toml-template example.toml
```

This will create `example.toml` in the current directory.

Once you have defined your filters, you apply this file with the `toml` subcommand.
As with the `quick` subcommand, flags can be applied to the existing file:

```bash
$GGGPATH/bin/add_nc_flags toml TOML_FILE --in-place --nc-file PRIVATE_NC_FILE
```

or to a copy of it:

```bash
$GGGPATH/bin/add_nc_flags toml TOML_FILE --output NEW_NC_FILE --nc-file PRIVATE_NC_FILE
```

For details on the TOML file settings, see the [following section](/postproc/add_nc_flags_toml.html).

## Use in TCCON standard processing

This program is not used by default in TCCON post processing.
(That is, it will not be included in the `post_processing.sh` script.)
Users are welcome to use it separately to flag out data with known problems from the private netCDF files before uploading to Caltech.

## Use in EM27/SUN standard processing

EGI-RS will include a line in the `post_processing.sh` script to run this program on the private netCDF file.
The intention is for users to add extra data checks to deal with EM27/SUN-specific issues that may affect the data.
Additionally, EGI-RS may add certain required filters in the future as the use of GGG for EM27/SUN retrievals matures.
