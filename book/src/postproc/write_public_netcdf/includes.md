# Including other configurations

The public netCDF writer configuration can extend other configuration files.
The intended use is to define a base configuration with a set of variables that should always be included,
and extend that for different specialize use cases.
The TCCON standard configurations use this to reuse the standard configuration (including all of the auxiliary variables,
normal computer variables, and Xgas values from the InGaAs detector) in the extended configuration (which adds the
InSb or Si Xgas values).

To use another configuration file, use the `include` key:

```toml
include = ["base_config.toml"]
```

This is a list, so you can include multiple files:

```toml
include = ["base_config.toml", "extra_aux.toml"]
```

```admonish info
Currently, when giving relative paths as the values for `include` as done here,
they are interpreted as relative to the current working directory.
However, you should not rely on that behavior - the intention is to adjust this
so that relative paths are interpreted as relative to the configuration file
in which they appear.
If you have a global set of configuration files that use `include`, for now,
it is best to use absolute paths in their respective `include` sections.
```

## How configurations are combined

Internally, `write_public_netcdf` uses the `figment` crate to combine the configurations.
Specifically, it uses the ["adjoin" conflict resolution strategy](https://docs.rs/figment/latest/figment/struct.Figment.html#conflict-resolution).
This means that lists (like the explicit Xgas definitions) from each configuration will be concatenated,
and scalar values will be taken from the first configuration that defines them.
(The order in which the configurations are parsed is defined [next](#order-of-inclusion)).

## Order of inclusion

The `include` key is recursive, so if `file1.toml` includes `file2.toml`, and 
`file2.toml` includes `file3.toml`, then `file1.toml` will include the combination
of `file2.toml` and `file3.toml`.
When files include more than one other file, the algorithm does a "depth-first" ordering.
That is, if our top-level configuration has:

```toml
# top.toml
include = ["middle1.toml", "middle2.toml"]
```

`middle1.toml` has:

```toml
# middle1.toml
include = ["bottom1a.toml", "bottom1b.toml"]
```

and `middle2.toml` has:

```toml
# middle2.toml
include = ["bottom2a.toml", "bottom2b.toml"]
```

then the order in which the files are added to the configuration is:

- `top.toml`
- `middle1.toml`
- `bottom1a.toml`
- `bottom1b.toml`
- `middle2.toml`
- `bottom2a.toml`
- `bottom2b.toml`

In other words, coupled with the ["adjoin" behavior used](#how-configurations-are-combined),
this ensures that the inclusion behavior is as you expect.
Settings in `top.toml` take precedence, then _all_ settings in `middle1.toml` (whether they
are defined in `middle1.toml` itself or one of its included files), and then `middle2.toml`
(and its included files) comes last.

```admonish warning
Although you _can_ create complex hierarchies of configurations as shown in this example,
doing so is generally not a good idea.
The more complicated you try to make the set of included files, the more likely you are
to end up with unexpected results - duplicated variables, wrong attributes, etc.
If you find yourself using more than one layer of inclusion, you may be better off
simply creating one large configuration file with the necessary parts of the other
files copied into it.
```