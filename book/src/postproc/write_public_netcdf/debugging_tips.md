# Debugging tips

## Checking on Xgas discovery

If variables are not being copied correctly, increase the verbosity of `write_public_netcdf` by adding `-v`
or `-vv` to the command line. The first will activate debug output, which includes a lot of information about
Xgas discovery. `-vv` will also activate trace-level logging, which will output even more information about the
configuration as the program read it.