# Compatibility

Some GGG-RS programs can be configured to change their output or expected input to be compatible with previous versions of GGG.
Programs that support this behavior will have a `--compat` command line option.
If that option is not specified, they will take their default from the `GGGRS_COMPAT` environmental variable.
If neither the command line option nor the environmental variable are set, then GGG-RS will not make any
adjustments to ensure compatibility.

## Compatibility options

- `current`: no special modifications will be made to I/O.
- `stable`: an alias for the most recent GGG release; currently GGG2020.
- `ggg2020`: post-processing files will be kept compatible with the GGG2020 post-processing Fortran programs.
  Specifically, the O2 global mean dry mole fraction will not be written as a 26th auxiliary data column.

## Deprecation policy

GGG-RS will only guarantee compatibility with the last GGG release, major or minor.
For example, once GGG2020.1 released, support for GGG2020.0 is not guaranteed.

Compatibility back to the last major release _may_ be supported, if the complexity of
doing so does not detract from code maintainability.
For example, support for GGG2020 may be maintained after the GGG2020.1 release, but only
if doing so does not make maintaining the GGG-RS code unfeasibly complicated.