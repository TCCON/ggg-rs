# apply_nc_fills

## Purpose

`apply_nc_fills` can be used to convert values representating null data in a
netCDF file to the defined netCDF fill values.
That way, programs and packages like [Panoply](https://www.giss.nasa.gov/tools/panoply/)
or Python's [netCDF4 package](https://unidata.github.io/netcdf4-python/) will
correctly mask them when read in.
The netCDF writer can do this automatically, but you may still need to use this
separate program if, for example:

- you used the GGG2020 Python netCDF writer and didn't realize that it won't automatically
  mask the null values from your `xx_sunrun.dat`, or
- you have other null values that come in from an ancillary instrument that need to be
  accounted for.

## Examples

This program uses a TOML file to specify what variables to filter.
(We may add a `quick` option in the future that takes the input on the command line,
but for now, the TOML file is required.)
You can generate an example TOML file with the `toml-template` subcommand, e.g.:

```bash
$GGGPATH/bin/apply_nc_fills toml-template my-fills.toml
```

This command will create a new file, `my-fills.toml` with some examples of how to
specify which values to replace.

To actually replace the values, use the `toml` subcommand.
If you want to keep the original file and create a new file with the fill values inserted
(recommended), use the `--output` option:

```bash
$GGGPATH/bin/apply_nc_fills toml \
    --nc-file $GGGPATH/install/current_results/pa20040721_20041222.private.nc \
    --output $GGGPATH/install/current_results/pa20040721_20041222.private.with-fills.nc \
    pa_fills.toml
```

This would use the rules in `pa_fills.toml` to replace bad values in `pa20040721_20041222.private.nc`
with fill values in `pa20040721_20041222.private.with-fills.nc`.

Alternatively, if you want to just modify a file without making a copy, use the `--in-place` flag:

```bash
$GGGPATH/bin/apply_nc_fills toml \
    --nc-file $GGGPATH/install/current_results/pa20040721_20041222.private.nc \
    --in-place \
    pa_fills.toml
```

This would edit `pa20040721_20041222.private.nc` directly.

## TOML format

The TOML file will be a sequence of `[[replace]]` blocks.
Each one defines a rule to replace values in one variable with netCDF fill values.

Each block _must_ have the field `varname`, which gives the name of the variable in
the netCDF file is applies to.

Each block _must_ have _one_ of the following sets of fields:

- `approx`: values that are within floating point precision of this value are replaced with fills.
- `gt` and `lt`: values that are between these values (`gt <= x <= lt`) are replaced with fills.
- `equal`: values that are exactly equal to this value are replaced with fills.

`approx` and `gt + lt` can be used to filter floating point variables.
`equal` can be used to filter integer variables.

Each block _may_ have the following:

- `time_greater_than`: limits this rule to data for times greater than or equal to this value.
- `time_less_than`: limits this rule to data for times less than or equal to this value.

Each of these must be in the format "YYYY-MM-DDThh:mm:ss", e.g. 12:34:56 on 31 Jan 2000 would
be `2000-01-31T12:34:56`.

Multiple blocks can be specified for the same variable.
This way, if there are different fill values for different time periods, or just multiple fill
values over the whole file, they can be handled.

### TOML example snippets

This first example will simple replace any values approximately equal to 60.0 in the `tins`
variable with fill values:

```toml
[[replace]]
varname = "tins"
approx = 60.0
```

This second example similarly replaces values approximately equal to -99.0 in the `pout`
variable with fills, but only between 1 Jan 2010 and 12 June 2015:

```toml
[[replace]]
varname = "pout"
approx = -99.0
time_greater_than = "2010-01-01T00:00:00"
time_less_than = "2015-06-12T00:00:00"
```

We could edit this example to replace the same values, but for _all_ times after 1 Jan 2010
by omitting the `time_less_than` field:

```toml
[[replace]]
varname = "pout"
approx = -99.0
time_greater_than = "2010-01-01T00:00:00"
```

If the approximate equality check is missing some value due to weird floating point rounding,
or if you just have a spectrum of fill values, use `gt` and `lt` to replace a range of values.
This example will replace all values between -1.5 and -0.5 in the `fvsi` variable:

```toml
[[replace]]
varname = "fvsi"
gt = -1.5
lt = -0.5
```

```admonish note
Both `lt` and `gt` are required.
You can mimic an open-ended range by setting the open end to a very positive or negative value.
```

Finally, if you need to insert fills into an integer variable, use the `equal` field:

```toml
# I don't know why you would put fill values in the day of year.
# But there just aren't that many integer variables in TCCON netCDF files.
[[replace]]
varname = "day"
equal = 367
```
