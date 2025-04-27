# Configuration

The public netCDF writer must strike a balance between being strict enough to ensure that the required variable for standard TCCON usage are included in normal operation, but also be flexible enough to allow non-standard usage.
To enable more flexible use, the writer by default requires the standard TCCON variables be present, but can be configured to change the required variables.

The configuration file uses [TOML format](https://toml.io/en/).
The configuration file can be broadly broken down into five sections:

- auxiliary variables,
- derived variables,
- Xgas variable sets,
- Xgas discovery, and
- default settings.