# Global attributes

What global attributes (i.e., attributes from the root group of the netCDF file) to copy to the public file are defined by the `[global_attributes]` section.
This section contains two lists of strings:

- `must_copy` lists the names of attributes that must be available in the private netCDF file, or an error is raised.
- `copy_if_present` lists the names of attributes to copy if available in the private netCDF file, but to not raise an error for if missing.

An abbreviated example from the TCCON standard configuration is:

```toml
[global_attributes]
must_copy = [
    "source",
    "description",
    "file_creation",
]
copy_if_present = [
    "long_name",
    "location",
]
```

At present, there is no way to manipulate attributes' values during the copying process, nor add arbitrary attributes.
In general, attributes should be added to the private netCDF file, then copied to the public file.
This ensures that attributes are consistent between the two files.
However, in the future we may add the ability to define some special cases.

```admonish warning
The `history` attribute is a special case, it will always be created or appended to following the
[CF conventions](http://cfconventions.org/Data/cf-conventions/cf-conventions-1.12/cf-conventions.html#description-of-file-contents),
no matter what the configuration says.
To avoid conflicts with this built in behavior, do not specify `history` as an attribute to copy in the configuration file.
```