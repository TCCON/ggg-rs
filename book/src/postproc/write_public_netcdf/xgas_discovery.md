# Xgas discovery

Usually we do not want to specify every single Xgas to copy; instead, we want the writer to scan the private file to identify Xgases and copy everything that matches.
This both saves a lot of tedious typing in the configuration and minimizes the possibilty of copy-paste errors.

## Rules

The first part of the discovery section is a list of rules for how to find Xgas variables.
These come in two variants:

1. Suffix rules: these look for variables that start with something starting with an Xgas-like pattern
   and ending in the given suffix. The full regex is `^x([a-z][a-z0-9]*)_{suffix}$`, where `{suffix}` is the provided suffix.
   Note that the suffix is passed through [`regex::escape`] to ensure that any special characters are escaped; it will only
   be treated as a literal.
2. Regex rules: these allow you to specify a regular expression to match variables names. The regex _must_ include a named
   capture group with the name "gas" that extracts the physical gas abbreviation (i.e., the `gas` value in an `Xgas` entry).
   This looks like `(?<gas>...)` where the `...` is the regular subexpression that matches that part of the string.

By default, the configuration will add a single regex rule that matches the pattern `^x(?<gas>[a-z][a-z0-9]*)$`.
You can disable this by setting `xgas_rules = false` in the [`[defaults]`](/write_public_netcdf/defaults.html) section of the config.
This rule is designed to match basic Xgas variables, e.g., "xch4", "xn2o", etc.

An example of a regular expression rule that uses the default ways to infer its ancillary variables is:

```toml
[[discovery.rule]]
regex = '^column_average_(?<gas>\w+)$'
```

Two things to note are:

1. The regular expression is inside single quotes; this is how TOML specifies literal strings and it the
   most convenient way to write regexes that include backslashes. (Otherwise TOML itself will intepret them
   as escape characters.)
2. The regex includes `^` and `$` to anchor the start and end of the pattern. In most cases, you will probably
   want to do so as well to avoid matching arbitrary parts of variable names.

An example of a suffix rule that also indicates that variables matching this rule should not include averaging kernels or the traceability scale is:

```toml
[[discovery.rule]]
suffix = "mir"
ak = { type = "omit" }
traceability_scale = { type = "omit" }
```

Note that the suffix rule contains a "suffix" key, while the regular expression rule has a "regex" key - this is how they are distinguished.
Also note that rules are checked in order, and a variable is added following the first rule that matches.
This means that if a variable matches multiple rules, then its ancillary variables will be set up following the first rule that matched.

## Attributes

Discovery rules can specify the fields `xgas_attr_overrides`, `xgas_error_attr_overrides`, `prior_profile_attr_overrides`,
`prior_xgas_attr_overrides`, and `ak_attr_overrides` to set attributes on their respective variables.
These should be used for attributes that will be the same for _all_ the variables of that type created by this rule.
For example, to add a cautionary note about experimental data to our previous mid-IR discovery rule:

```toml
[[discovery.rule]]
suffix = "mir"
xgas_attr_overrides = { note = "Experimental data, use with caution!" }
ak = { type = "omit" }
traceability_scale = { type = "omit" }
```

## Ancillary variables

The rules also include default settings for the prior profile, prior column average, averaging kernel (and its slant Xgas bins), and the traceability scale.
These can be specified the same way as described [in the Xgases ancillary subsection](/write_public_netcdf/explicit_xgases.html#ancillary-variable-specifications),
and the defaults are the same as well.
However only the `inferred` and `omit` types may be used, as `specified` does not make sense when a rule may apply to more than one Xgas.

## Exclusions

The second part of the discovery section are lists of gases or variables to exclude.
The first option is ``excluded_xgas_variables``.
If a variable's private file name matches one of the names in that list, it will not be copied even if it matches one of the rules.
The other option is `excluded_gases`, which matches not the variable name, but the physical gas.
The easiest way to explain this is to consider the standard TCCON configuration:

```toml
[discovery]
excluded_xgas_variables = ["xo2"]
excluded_gases = ["th2o", "fco2", "zco2"]
```

`excluded_xgas_variables` specifically excludes the "xo2" variable; this would match the default regex rule meant to capture Xgases measured on the primary detector, but we don't want to include it because it is not useful for data users.
However, O2 measured on a silicon detector may be useful, so we do not want to exclude all O2 variables.
`excluded_gases` lists three gases that we want to exclude no matter what detector they are retrieved from.
"fco2" and "zco2" are diagnostic windows (for channelling and zero-level offset, respectively) and so will be included once for each detector.
"th2o" is temperature sensitive water, which is generally confusing for the average user, so we want to ensure that it is also excluded from every detector.
