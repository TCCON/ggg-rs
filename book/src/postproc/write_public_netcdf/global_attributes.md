# Global attributes

What global attributes (i.e., attributes from the root group of the netCDF file) to copy to the public file are defined by the `[global_attributes]` section.
This section contains the following subsections:

- `must_copy` lists the names of attributes that must be available in the private netCDF file, or an error is raised.
- `copy_if_present` lists the names of attributes to copy if available in the private netCDF file, but to not raise an error for if missing.
- `copy_and_edit` maps attribute names to how they are to be edited. See below for available operations.

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

At present, there is no way to add arbitrary attributes.
In general, attributes should be added to the private netCDF file, then copied to the public file.
This ensures that attributes are consistent between the two files.

```admonish warning
The `history` attribute is a special case, it will always be created or appended to following the
[CF conventions](http://cfconventions.org/Data/cf-conventions/cf-conventions-1.12/cf-conventions.html#description-of-file-contents),
no matter what the configuration says.
To avoid conflicts with this built in behavior, do not specify `history` as an attribute to copy in the configuration file.
```

## Editing attributes

The `copy_and_edit` section maps attribute names to descriptions of how to edit them.
The different ways to edit are defined by the `"type"` key in these entries.
For example, the TCCON extended configuration defines:

```toml
[global_attributes.copy_and_edit]
data_doi = { type = "doi_to_extended" }
data_reference = { type = "doi_to_extended" }
```

The `type` key definitions follow.

### doi_to_extended

`type = "doi_to_extended"` will look for a DOI in the attribute and, assuming it is formatted like a TCCON data DOI,
convert it from the standard DOI to one appropriate for the extended data files.
If the DOI does not match the TCCON data DOI format closely enough, this will raise an error, so only apply it to
attributes that contain such a DOI.
The DOI _can_ be part of a larger string, such as the full reference.

This has one optional field, `required`, which determines if the attribute must be present.
The default is `required = true`, meaning it is an error if the attribute is missing.
Here is an example setting it to `false`:

```toml
[global_attributes.copy_and_edit]
data_reference = { type = "doi_to_extended", required = false }
```