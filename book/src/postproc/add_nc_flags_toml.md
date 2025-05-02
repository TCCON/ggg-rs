# `add_nc_flags` TOML filter configuration

## Filters and groups

The TOML files used to define flags for `add_nc_flags` use the terms "group" and "filter" as follows:

- A "filter" defines an allowed range for a single variable.
  It may specify and upper or lower limit, an allowed range, or an excluded range.
- A "group" consists of one or more filters.

An observation in the netCDF file will be flagged if any of the groups defined in the TOML file
indicate that it should be.
A group will indicate an observation should be flagged if all of the filters in that group indicate
it should be flagged.

The first example shows how you would define a TOML file that duplicates the filter we used
in the [quick filter example](/postproc/add_nc_flags.html#quick-flagging):

```toml
[[groups]]
[[groups.filters]]
filter_var = "o2_7885_rmsocl"
greater_than = 0.5
```

This defines a single check: if value of the `o2_7885_rmsocl` variable in the netCDF file is
`>= 0.5`, that observation will be flagged.

Now suppose that we wanted to flag observations only if `o2_7885_rmsocl >= 0.5` _and_ `o2_7885_cl < 0.05`,
perhaps to remove observations where our instrument was not tracking the sun well.
To do this, we add a second filter to this group like so:


```toml
[[groups]]
[[groups.filters]]
filter_var = "o2_7885_rmsocl"
greater_than = 0.5

[[groups.filters]]
filter_var = "o2_7885_cl"
less_than = 0.05
```

Now, because these both come under the same `[[group]]` heading, observations will only be flagged if both conditions are true.

What if we wanted to do an or, that is, filter if either one of two (or more) conditions are true?
That requires multiple groups:

```toml
[[groups]]
[[groups.filters]]
filter_var = "o2_7885_rmsocl"
greater_than = 0.5

[[groups]]
[[groups.filters]]
filter_var = "o2_7885_sg"
greater_than = 0.1
```

Because each filter comes after its own `[[group]]` header, they fall in separate groups.
If either group has all its filters return true, then the observation will be flagged.
In this case, that means that an observation with `o2_7885_rmsocl >= 0.5` _or_ `o2_7885_sg <= 0.1`
will be flagged.

What if we wanted to flag an observation with an `o2_7885_sg` value too far from zero in either direction?
We can do that with the `value_mode` key, like so:


```toml
[[groups]]
[[groups.filters]]
filter_var = "o2_7885_sg"
greater_than = 0.1
less_than = -0.1
value_mode = "outside"
```

This will cause an observation to be flagged if `o2_7885_sg <= -0.1` or `o2_7885_sg >= +0.1`.
If we do not specify `value_mode`, the default is "inside", which in this case would flag 
if `-0.1 <= o2_7885_sg <= +0.1`.

## Limiting to times

The TOML file allows you to specify that it should only apply to a specific time frame with the `[timespan]` section.
This allows three keys: `time_less_than`, `time_greater_than`, and `time_mode`.
For example, perhaps you wish to filter on continuum level only between two times when you know your instrument
was not tracking the sun correctly.
You could do so with:

```toml
[[groups]]
[[groups.filters]]
filter_var = "o2_7885_cl"
less_than = 0.05

[timespan]
time_greater_than = "2025-01-01T00:00:00"
time_less_than = "2025-05-01T00:00:00"
```

Note that the times must be in UTC and given in the full "yyyy-mm-ddTHH:MM:SS" format; unlike the `quick` command line
option, you cannot truncate these to just "yyyy-mm-dd" or "yyyy-mm-ddTHH:MM".
`time_mode`, similar to `value_mode` in the filters, allows you to only flag observations outside of the given time
range, rather than inside it:

```toml
[[groups]]
[[groups.filters]]
filter_var = "o2_7885_cl"
less_than = 0.05

[timespan]
time_greater_than = "2025-01-01T00:00:00"
time_less_than = "2025-05-01T00:00:00"
time_mode = "outside"
```

This will apply the filter to any data before 1 Jan 2025 and after 1 May 2025, whereas the previous example would
apply to data between those two dates.

## Changing the flag

### Flag value

By default, when `add_nc_flags` applies a flag, it does so by adding 9000 to the value of the `flag` variable for
that observation.
This preserves any of the standard flags from variables defined in the `??_qc.dat` file.
By TCCON convention, a 9 in the thousands place of the flag represents a generic "other" manual flag.
If you wish to use one of the other values to distinguish the nature of the problem, you can define a `[flags]` section:

```toml
[flags]
flag = 8

# The configuration must always include a [[groups]] entry with at least one filter.
[[groups]]
[[groups.filters]]
filter_var = "o2_7885_cl"
less_than = 0.05
```

This will add 8000 to the flag instead of 9000; i.e., it will put an 8 in the thousands place.

```admonish note
The value of `flag` must be between 1 and 9, since it must fit into the thousands place.
Some values have existing meanings. Currently these are defined in
[a JSON file bundled with the private netCDF writer](https://github.com/TCCON/py_tccon_netcdf/blob/main/write_tccon_netcdf/release_flag_definitions.json).
When the private netCDF writer is incorporated into GGG-RS, that definition file will be moved into this repository. 
```

### Behavior for existing flags

We can also adjust how `add_nc_flags` behaves if there is already a value in the thousands place.
By default, it will error.
We can change this by setting the `existing_flags` key in the `[flags]` section.
For example, to keep existing values in the thousands place of the flag (which would be set by either
the time periods defined in your `$GGGPATH/tccon/??_manual_flagging.dat` or a previous run of `add_nc_flags`):

```toml
[flags]
flag = 8
existing_flags = "skip"

# The configuration must always include a [[groups]] entry with at least one filter.
[[groups]]
[[groups.filters]]
filter_var = "o2_7885_cl"
less_than = 0.05
```

The possible (case insensitive) values for `existing_flags` are:

- `"error"` (default) - error if any of the observations to be flagged already have a non-zero value in the flag's thousands place
- `"skip"` - if an observation to be flagged already has a value in the flag's thousands place, leave the existing value.
- `"skipeq"` - if the value in the thousands place is 0 is will be replaced, otherwise `add_nc_flags` will error unless the value
  matches what it would insert.
- `"overwrite"` - replace any existing value in the flag's thousands place.

### Flag type

The default behavior, as mentioned [above](#flag-value), is to modify the thousands place of the flag for observations to be flagged.
`add_nc_flags` can also edit the ten thousands place, which is used for release flags.
To do so, set `flag_type` to `"release"` in the `[flags]` section:


```toml
[flags]
flag = 8
flag_type = "release"

# The configuration must always include a [[groups]] entry with at least one filter.
[[groups]]
[[groups.filters]]
filter_var = "o2_7885_cl"
less_than = 0.05
```

```admonish warning
Release flags are intended to be set by Caltech personnel based on input from the reviewers during data QA/QC.
If you use `add_nc_flags` to set release flags in other circumstances, this can lead to significant confusion
when trying to establish the provenance of certain flags.
Please do not use `flag_type = "release"` unless you have received specific guidance to do so!
```
