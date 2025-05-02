# Computed variables

Computed variables are similar to auxiliary variables in that they are not directly associated with a single Xgas.
Unlike auxiliary variables, these cannot be simply copied from the private netCDF file.
Instead, they must be computed from one or more private variables.
Because of that, there are a specific set of these variables pre-defined by the public writer.
Currently only one computed variable type exists, "prior_source".
You can specify it in the configuration as follows:

```toml
[[computed]]
type = "prior_source"
```

By default, this creates a public variable named "apriori_data_source".
You can change this with the `public_name` field, e.g.:

```toml
[[computed]]
type = "prior_source"
public_name = "geos_source_set"
```