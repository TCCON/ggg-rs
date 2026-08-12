# `create_sunrun_rs` configuration

`create_sunrun_rs` aims to permit as enough flexibility through its configuration file
to avoid the need for anyone to modify the Rust code to account for their site's idiosyncrasies.
The file uses the [TOML format](https://toml.io/en/).
This is a change from the previous `sunrun.dat` format, but TOML is a standard format,
meaning that readers & writers are readily available.
It has also become the de facto standard for programming configuration; Python, Julia,
and Rust all use TOML files to configure projects now, most users should have experience
with it.

The configuration file has two main parts, constant definitions (which apply to all spectra)
and edits (which have options to alter the values in a sunrun row before it is written to the file).

## Constants

Here is an example of a typical constants section:

```toml
[constants]
# 1 = MkIV, 2 = TCCON, 3 = Other
instrument = 2
# 1 = Moon, 2 = Sun
object = 2

[constants.defaults]
oblat = 45.9448
oblon = -90.2732
obalt = 0.442
fsf = 0.99999999
wavtkr = 9900.0
aipl = 0.002
tm = 1.00

[[constants.detectors]]
label = "a"
label_index_start = 16
label_index_stop = 16
start_freq = 4000
end_freq = 11000

[[constants.detectors]]
label = "b"
label_index_start = 16
label_index_stop = 16
start_freq = 11000
end_freq = 15000

[[constants.detectors]]
label = "c"
label_index_start = 16
label_index_stop = 16
start_freq = 1800
end_freq = 6000
```

Let's break this down:

### Pure constants

```toml +constants
[constants]
# 1 = MkIV, 2 = TCCON, 3 = Other
instrument = 2
# 1 = Moon, 2 = Sun
object = 2
```

This first section defines true constants that cannot be inferred from the spectrum.
In particular, it defines which celestial object the instrument was pointing at and
what instrument type this is.
The latter determines how it reads the spectrum header.
Currently, "TCCON" and "Other" are treated the same, but this may change in the future.

```admonish note
For now, we retain the previous convention of defining these as integers. This may
change in the future to be more readable.
```

### Default values

```toml +defaults
[constants.defaults]
oblat = 45.9448
oblon = -90.2732
obalt = 0.442
fsf = 0.99999999
wavtkr = 9900.0
aipl = 0.002
tm = 1.00
```

This section defined defaults for values that may be read from the spectrum header,
but may not be, depending on your instrument configuration.
The values you provide here can either be the correct value to use if absent from the
header, or a null/fill value to use.
The latter is generally more appropriate for quantities that change over time.
The quantities shown in the example are the typical ones a TCCON site will need
to include, but there are more that can be set.
The full list is:

- `tcorr`: Time correction to apply to the runlog. Defaults to 0.0.
- `oblat`: Observation latitude, south being negative. Defaults to -999.0.
- `oblon`: Observation longitude, west being negative. Defaults to -999.0.
- `obalt`: Observation altitude, in kilometers. Defaults to -999.0.
- `tins`: Instrument internal temperature, in degrees C. Defaults to -999.0.
- `pins`: Instrument internal pressure, in hPa. Defaults to -999.0.
- `hins`: Instrument internal humidity, in percent. Defaults to -999.0.
- `tout`: Outside temperature, in degrees C. Defaults to -999.0.
- `pout`: Outside pressure, in hPa. Defaults to -999.0.
- `hout`: Outside humidity, in percent. Defaults to -999.0.
- `sia`: Average solar intensity, in arbitrary units. Defaults to -999.0.
- `fvsi`: Fractional variation in solar intensity, unitless. Defaults to -999.0.
- `wspd`: Wind speed. Defaults to -999.0.
- `wdir`: Wind direction. Defaults to -999.0.
- `nus`: Starting wavenumber of the spectrum. Defaults to -999.0.
- `nue`: Ending wavenumber of the spectrum. Defaults to -999.0.
- `fsf`: Frequency shift factor, unitless. Defaults to -999.0.
- `lasf`: Laser frequency, cm-1. Defaults to -999.0.
- `wavtkr`: Wavetracker number, cm-1. Defaults to -999.0.
- `aipl`: Airmass independent path length. This is the length between the first suntracker mirror and the instrument. Defaults to -999.0.
- `tm`: Telescope magnification factor. Defaults to -999.0.

### Detectors

```toml +detectors
[[constants.detectors]]
label = "a"
label_index_start = 16
label_index_stop = 16
start_freq = 4000
end_freq = 11000

[[constants.detectors]]
label = "b"
label_index_start = 16
label_index_stop = 16
start_freq = 11000
end_freq = 15000

[[constants.detectors]]
label = "c"
label_index_start = 16
label_index_stop = 16
start_freq = 1800
end_freq = 6000
```

This section defines the different detectors your instrument may have, and the mapping from
the part of the spectrum name that indicates which detector it was measured on to the information
here.
The double square brackets mean that this is a list, so you can have as many of these sections
as you need.

Let's take Park Falls InGaAs and Si spectra from the benchmark as an example:

```text
# the numbers count the character index
  1    6   11   16
  |    |    |    |
  pa20040721saaaaa.043
  pa20040721saaaab.043
```

We can see that the 16th character changes from "a" (InGaAs) to "b" (Si).
This lines up with our detector blocks above.
With this, the above configuration tells `create_sunrun_rs` to use the 16th
character to set the frequency ranges; "a" spectra should be used between 4000
and 11000 cm<sup>-1</sup> and "b" spectra between 11000 and 15000 cm<sup>-1</sup>.

```admonish note
How are the wavenumber limits used? 
Rest assured that if you set these outside the
actual frequency limits of your spectra, GGG will automatically limit the range it
uses to the smaller of the two ranges.
However, if you have spectra with overlapping frequencies that both cover a winodw,
this tells GFIT which spectrum to use for a given window.
For instance, if you had a spectrum that actually contained 4,000 to 8,000 cm<sup>-1</sup> and another
that contained 7,000 to 10,000 cm<sup>-1</sup>, a window at 7,500 cm<sup>-1</sup> could be
fit by either spectrum.
Setting the limits here to 4,000 to 8,000 for the first and 8,000 to 10,000 for the second would tell
GFIT to fit that window with the first spectrum.
```

## Edits

Now we get into the reason we updated `create_sunrun` from Fortran to Rust: the need for flexibility in how input corrections to the sunrun parameters.
We often need to correct timing offsets, surface pressure, and the like.
`create_sunrun_rs` handles this by allowing you to define "edits" that modify values for specific sets of spectra.
Each edit definition needs two parts: which spectra to edit and how to edit them.
A single definition begins with `[[edits]]`.
Here's an example that includes different examples of each:

```toml +edits
[[edits]]
spectrum = "pa20040721saaaa?.*"
replace = { tm = 1.01 }

[[edits]]
time_range = ["2020-01-01T00:00:00Z", "2020-03-01T00:00:00Z"]
lua = "r.pout = r.pout + 0.7"
```

This shows that we can specify exact spectra to modify (using glob patterns) or specify time ranges,
and that we can make the edits using a variety of ways.
The following sections will go into detail on each option.

### Selecting rows

Each `[[edit]]` block must use exactly one of the following methods.
If you try to use more than one, only one of the conditions will be
used, and we do not guarantee which one, so do not rely on it!
For example, this block will either use the time range _or_ the spectrum,
but not both:

```toml +edits
[[edits]]
time_range = ["2020-01-01T00:00:00Z", "2020-03-01T00:00:00Z"]
spectrum = "pa20040721saaaa?.*"
replace = { tm = 1.01 }
```

#### By time and lat/lon

Likely the most common selection will be to identify a time range that the correction applies to.
To do so, include the `time_range` key in that `[[edits]]` block:

```toml +edits
[[edits]]
time_range = ["2020-01-01T00:00:00Z", "2020-03-01T00:00:00Z"]
replace = { tm = 1.01 }
```

As in this example, it must be a two-element list with two times.
Any row whose spectrum has a ZPD time between these times (inclusive) will be affected by this block.

```admonish warning
For the moment, the times must be written in the exact format shown in this example,
`%Y-%m-%dT%H:%M:%SZ, including the literal `T` and `Z`.
This is a limitation of the configuration reader that I hope to ease in the future,
but which is trickier than I hoped, so I haven't been able to make it work yet.

Also, if your end time is before your start time, you will get an error when this block runs its check.
```

Optionally, you can include latitude and/or longitude limits like so:


```toml +edits
[[edits]]
time_range = ["2020-01-01T00:00:00Z", "2020-03-01T00:00:00Z"]
lat_range = [45.0, 46.0]
lon_range = [-91, -90]
replace = { tm = 1.01 }
```

This may be more niche, but could be helpful if you have a sunrun with an instrument
in multiple locations, and only one of those locations had a problem.

#### By spectrum

You can tell `create_sunrun_rs` that an edit section applies to rows whose spectrum matches a glob pattern.
To do so, include the `spectrum` key in that `[[edits]]` block:

```toml +edits
[[edits]]
spectrum = "pa20040721saaaa?.*"
replace = { tm = 1.01 }
```

This will only apply to rows whose spectrum name starts with "pa20040721saaaa", have any single character,
a period, and then 0 or more characters.
Given the typical [TCCON naming convention](https://tccon-wiki.caltech.edu/Main/TCCONSpectralNamingConvention),
this would match all spectra from Park Falls on 2024-07-21.
Internally, this uses the [`glob` crate](https://docs.rs/glob/latest/glob/index.html) to match the spectrum names,
see their [`Pattern` docs](https://docs.rs/glob/0.3.4/glob/struct.Pattern.html) for the list of special characters.
If you are familiar with shell glob patterns, these will be similar.

We can also use this to match a single spectrum, just by not using any wildcards, e.g.:

```toml +edits
[[edits]]
spectrum = "pa20040721saaaaa.043"
replace = { tm = 1.01 }
```

This will only affect row for a spectrum with this exact name.

#### By custom condition

Finally, if neither of the above methods suit your needs, you can also define a custom [Lua](https://www.lua.org/pil/)
expression that returns true or false to indicate if that row should be modified.
To do so, include the `condition` key in that `[[edits]]` block:

```toml +edits
[[edits]]
condition = "r.year == 2024"
replace = { tm = 1.01 }
```

The `condition` string can be a single line of Lua or multiple lines, as long as it returns a boolean.
In this example, because it is a single line of Lua, we do not need to use the `return` keyword, the value
is implicitly returned.
However, if you did a multiline Lua block, then you must use `return` to indicate which value to return.
This allows even early returns, so if you had a bug that affected the day after a leap day for instance,
you might do:

```toml +edits
[[edits]]
condition = """
if (r.year % 4) ~= 0 then
    return false
elseif r.month == 3 and r.day == 1 then
    return true
else
    return false
end
"""
replace = { tm = 1.01 }
```

See [the section below](#using-lua-in-the-sunrun-configuration) for more details on interacting with the row via Lua.

### Editing sunrun rows

As with selecting which rows an `[[edit]]` block applies to, there are multiple ways to edit a row.
Unlike the selection, you can use 

#### Simple replacement

The easiest way to modify a sunrun row is to use the `replace` keyword to provide static values to insert.
An example:

```toml +edits
[[edits]]
spectrum = "pa20040721saaaaa.043"
replace = { tm = 1.01 }
```

This will set the "tm" value for the row affected by this block to `1.01`.
The value of `replace` is a map of sunrun column names to the values to insert.
The keys are the **lower case** column names; here, even though the column in the
sunrun file is "TM", we use "tm" as the key.

You can include multiple columns to replace, as in:


```toml +edits
[[edits]]
spectrum = "pa20040721saaaaa.043"
replace = { tm = 1.01, aipl = 0.005 }
```

#### Replacement with Lua

Just as for the conditions, we can use Lua to modify the sunrun rows.
To do so, write a block of Lua code as the `lua` key under the `[[edits]]` block:

```toml +edits
[[edits]]
time_range = ["2020-01-01T00:00:00Z", "2020-03-01T00:00:00Z"]
lua = "r.pout = r.pout + 0.7"
```

This example shows how to add a constant pressure offset.
You can modify multiple values with a multiline expression:

```toml +edits
[[edits]]
time_range = ["2020-01-01T00:00:00Z", "2020-03-01T00:00:00Z"]
lua = """
r.pout = r.pout + 0.7
r.tout = r.tout * 1.01
"""
```

The full Lua interpreter is available, so you can give even more complicated code,
including setting local variables.
If you need to initialize variables for this block, use the `init_lua` key.
This will be a Lua block that only runs once (the first time this block applies to
a sunrun row).
If, for example, you had an issue where a timing offset incremented for each spectrum
recorded, you could do:

```toml +edits
[[edits]]
time_range = ["2020-01-01T00:00:00Z", "2020-03-01T00:00:00Z"]
init_lua = "delta_t = 0.0"
lua = """
delta_t = delta_t + 0.1
r.tcorr = delta_t
"""
```

The `init_lua` block will be run the first time `create_sunrun_rs` generates a row with a spectrum
ZPD time between 1 Jan 2020 and 1 Mar 2020.


Note that, unlike the conditions, you assign values
See [the section below](#using-lua-in-the-sunrun-configuration) for more details on interacting with the row via Lua.


### Using Lua in the sunrun configuration

Lua is a pretty simple language.
All mathematical operations work like you expect: `+`, `-`, `*`, `/`, `^` (exponentiation), and `%` (modulus).
It does _not_ have the assignment versions of these operators, so no `+=` and the like.
There is a [`math` library](https://www.lua.org/manual/5.3/manual.html#6.7) preloaded with additional functions,
e.g. you can do `x = math.exp(-1)`.

The current sunrun row will be available as the variable `r`.
You can access the various fields on it with dot notation, e.g. `r.pout`, and this works for reading or assigning values.
As with [simple replacement](#simple-replacement), the fields of `r` are the lower cased column names from the sunrun file.
There are some additional fields that do not show up in the sunrun file, but which you may find useful for calculations:

- `zpd_time`: ZPD time of the spectrum as a string
- `year`, `month`, `day`, `hour`, `minute`, `second`: ZPD time components as integers.

```admonish note
**Why Lua?** You might be wondering why this uses Lua instead of something like Python, which many people
already use in their work.
Basically, Lua embeds extremely easily into other languages.
Setting up Python to be callable from Rust requires that you have Python set up on your computer separately
and configured so that Rust knows where to find it.
Lua can be embedded directly into a Rust program without needing any separate installation.

```
