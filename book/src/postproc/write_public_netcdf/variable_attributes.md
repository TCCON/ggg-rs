# Variable attributes

By default attributes on private variables are copied to their public counterparts.
This can be controlled with the `var_attrs_to_omit` global configuration option, or
the individual `attr_overrides` option used for variables or in discovery.

## Global options

The global option `var_attrs_to_omit` specifies a list of variable attributes that, by default,
are not copied.
For instance the standard configurations all specify:

```toml
var_attrs_to_omit = ["standard_name", "precision", "oof_scaling"]
```

This means that, by default, no variable will have a "standard_name", "precision",
or "oof_scaling" attribute in the public files, even if it does in the private files.
You can override this for individual cases with `attr_overrides`; see below.

```admonish info
`standard_name` is omitted because the CF conventions require the value to come from
a list of well-defined names, none of which work well for column variables, so we
use `long_name` everywhere instead.
We remove the other two because they are internal-use attributes only.
```

## Attribute overrides

Several places in the configuration use a `attr_overrides` or similar field.
This field allows you to set the value that the attribute will have or modify
how or if it is copied.
An example attributes override block might look like:

```toml +attr_overrides
[attr_overrides]
flag_values = [1, 2]
flag_meanings = "bad_data very_bad_data"
note = { omit = true }
standard_name = { omit = false }
```

Here, we see that `flag_values` and `flag_meanings` have values of an array and string, respectively.
This means that, regardless of whether or not these attributes exist on the variable being copied,
they will have these values in the public file.
Then, both `note` and `standard_name` have a TOML table as their values.
(A table in TOML is like a dictionary in Python or a map in other languages.)
The table allows more control over how the attribute is copied than just specifying a value.
Currently the only options available are:

- `omit`: overrides the global `var_attrs_to_omit` option. Here, `note` will _not_ be copied
  (because `omit = true`), while `standard_name` _will_ be copied if present, even if it was
  specified to be omitted by default in the `var_attrs_to_omit` list.
- `value`: allows you to give an override value. That is, `units = "ppm"` and
  `units = { value = "ppm" }` are equivalent. At present, there is little use for this syntax
  compared to directly specifying the value, but in the future, this may be useful to combine
  with other options.