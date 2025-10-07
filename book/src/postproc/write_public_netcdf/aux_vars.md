# Auxiliary variables

Auxiliary variables are those which are not directly related to one of the target Xgases but which provide useful information about the observations. Common examples are time, latitude, longitude, solar zenith angle, etc.
These are defined in the `aux` section of the TOML file as an [array of tables](https://toml.io/en/v1.0.0#array-of-tables).

The simplest way to define an auxiliary variable to copy is to give the name of the private variable in the netCDF file and what value to use as the long name:

```toml
[[aux]]
private_name = "solzen"
long_name = "solar zenith angle"
```

This will copy the variable `solzen` from the private netCDF file along with all its attributes _except_ `standard_name` and `precision`, add the `long_name` attribute, and put the variable's data (subsetting to `flag == 0` data) into the public file as `solzen`.
Note that the `long_name` value should follow the [CF conventions meaning](https://cfconventions.org/cf-conventions/cf-conventions.html#long-name).
We prefer `long_name` over `standard_name` because the [available standard names](https://cfconventions.org/Data/cf-standard-names/current/build/cf-standard-name-table.html) do not adequately describe remotely sensed quantities.

If instead you wanted to rename the variable in the public file, you can add the `public_name` field:

```toml
[[aux]]
private_name = "solzen"
public_name = "solar_zenith_angle"
long_name = "solar zenith angle"
```

This would rename the variable to `solar_zenith_angle` in the public file, but otherwise behave identically to above.

You can also control the attributes copied through two more fields, `attr_overrides` and `attr_to_remove`.
`attr_overrides` is a TOML table of attibute names and values that will be added to the public variable.
If an attribute is listed in the private file with the same name as an override, the override value in the config takes precedence.
The latter is an array of attribute names to skip copying if present.
(If one of these attributes is not present in the private file, nothing happens.)
An example:

```toml
[[aux]]
private_name = "day"
long_name = "day of year"
attr_overrides = {units = "Julian day", description = "1-based day of year"}
attr_to_remove = ["vmin", "vmax"]
```

This will add or replace the attributes `units` and `description` in the public file with those given here, and ensure that the `vmin` and `vmax` attributes are not copied.
Take note, specifying `attr_to_remove` overrides the default list of `standard_name` and `precision`; this can be useful if you want to retain those (you can do so by specifying `attr_to_remove = []`), but if you want to exclude them, you must add them to your list.

Finally, by default any auxiliary variable listed here must be found in the private netCDF file, or the public writer stops with an error.
To change this behavior so that a variable is optional, add the `required = false` field to an aux variable:

```toml
[[aux]]
private_name = "day"
long_name = "day of year"
required = false
```

Each auxiliary variable to copy will have its own `[[aux]]` section, for example:

```toml
[[aux]]
private_name = "time"
long_name = "zero path difference UTC time"

[[aux]]
private_name = "year"
long_name = "year"

[[aux]]
private_name = "day"
long_name = "day of year"
attr_overrides = {units = "Julian day", description = "1-based day of year"}

[[aux]]
private_name = "solzen"
long_name = "solar zenith angle"
```