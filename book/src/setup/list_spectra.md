# list_spectra

## Purpose

`list_spectra` will print the names of spectra given on the command line in the proper order to go into a list file needed to use `create_sunrun`.
Spectra must be listed such that, for dual-detector instruments, the pairs of spectra from both detectors at a given time appear together, e.g.:

```text
xx20250408s0e00a.0001
xx20250408s0e00d.0001
xx20250408s0e00a.0002
xx20250408s0e00d.0002
xx20250408s0e00a.0003
xx20250408s0e00d.0003
```

Note how this is _not_ the typical lexicographic order you would get from calling `ls`.

## Examples

To list all spectra in the hypothetical directory `/data/tccon/spectra` beginning with `xx`, use:

```bash
$GGGPATH/bin/list_spectra /data/tccon/spectra/xx*
```

If listing a large number of spectra, you may run into an error that references a maximum number of command line arguments reached.
This is not a problem with `list_spectra`, it is an issue with the shell or OS.
This occurs because if you run the command above, the _shell_ expands the `*` pattern from a single argument into however many arguments match that pattern.
If, for example, you had three spectra (`xx20250408s0e00a.0001`, `xx20250408s0e00a.0002`, `xx20250408s0e00a.0003`) in the above directory,
what really gets called is:

```bash
$GGGPATH/bin/list_spectra /data/tccon/spectra/xx20250408s0e00a.0001 /data/tccon/spectra/xx20250408s0e00a.0002 /data/tccon/spectra/xx20250408s0e00a.0003
```

that is, `list_spectra` actually gets three arguments on the command line.
When you have a large number of spectra, this can translate into tens of thousands of arguments, which can exceed the maximum number
of arguments that can be passed to a program.
To avoid this issue, quote any command line arguments including glob patterns and use the `--expand-globs` flag:

```bash
$GGGPATH/bin/list_spectra --expand-globs '/data/tccon/spectra/xx*'
```

By enclosing the pattern in single (or double) quotes, we prevent the shell from expanding the pattern itself, and
`--expand-globs` tells `list_spectra` it will need to expand them itself.

## Use in TCCON standard processing

`list_spectra` is not part of TCCON standard processing.
Many sites will have their own solutions for listing spectra in the correct order.
GGG itself provides a `list_maker` program which has a similar purpose, but uses a data partition file.

## Use in EM27/SUN standard processing

`list_spectra` itself is not used in EM27/SUN standard processing with EGI-RS.