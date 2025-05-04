# Debugging tips

## Configuration parsing errors

If you are trying to use a custom configuration and the writer gives an error that it cannot "deserialize" the file,
that means that there is either a TOML syntax error or another typo.
To narrow down where the problem is, incrementally comment out parts of the configuration file and run the writer
with the `--check-config-only` flag.
When this can parse the configuration, it will print an internal representation of it to the screen.
Thus, when it starts working, whatever section you commented out is the likely culprit.

## Unexpected output (such as missing or duplicated variables)

If you seem to be missing variables in the output, have the same variable try to be written twice,
or other problems when using a custom configuration, first run the writer with the `--check-config-only`
flag and carefully examine the printed parsed version of the configuration.
This can help check if the configuration is being interpreted as you intended,
especially when using the [include feature](/postproc/write_public_netcdf/includes.html)

## Checking on Xgas discovery

If variables are not being copied correctly, increase the verbosity of `write_public_netcdf` by adding `-v`
or `-vv` to the command line. The first will activate debug output, which includes a lot of information about
Xgas discovery. `-vv` will also activate trace-level logging, which will output even more information about the
configuration as the program read it.