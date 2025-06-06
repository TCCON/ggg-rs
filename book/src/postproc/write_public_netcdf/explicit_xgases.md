# Explicitly specified Xgases

This section allows you to list specific Xgas variables to copy, along with some or all of the ancillary variables needed to properly interpret them. 
Usually, you will not specify each Xgas by hand, but instead will use the [discovery](/write_public_netcdf/xgas_discovery.html) capability of the writer to automatically find each Xgas to copy.
However, variables explicitly listed in this section take precedence over those found by the discovery rules.
This leads to two cases where you might specify an Xgas in this section:

1. The Xgas does not follow the common naming convention, thus making it difficult to discover with simple rules, or difficult
   to map from the Xgas variable to the ancillary variable.
2. The Xgas needs a different way to handle one of its ancillary variables that the default discovery does.

Each Xgas has the following options:

- `xgas` (required): the variable name
- `gas` (required): the physical gas name, e.g., "co2" for all the various CO2 variables (regular, `wco2`, and `lco2`).
  This is used to match up to, e.g., the priors which do not distinguish between the different spectra windows.
- `gas_long` (optional): the full name of the gas instead of its abbreviation, e.g. "carbon dioxide" for CO2. If not given,
  then the configuration will try to find the `gas` value in its [`[gas_long_names]`](/write_public_netcdf/gas_proper_names.html) section and use that,
  falling back on the `gas` value if the gas is not defined in the gas long names section.
- `xgas_attr_overrides` (optional): a table of attribute values that can override existing attribute values on the private Xgas variable.
- `xgas_error_attr_overrides` (optional): a table of attribute values that can override existing attribute values on the private Xgas error variable.
- `prior_profile` (optional): how to copy the a priori profile.
- `prior_profile_attr_overrides` (optional): a table of attribute values that can override existing attribute values on the private prior profile variable.
- `prior_xgas` (optional): how to copy the a priori column average.
- `prior_xgas_attr_overrides` (optional): a table of attribute values that can override existing attribute values on the private prior Xgas variable.
- `ak` (optional): how to copy the averaging kernels.
- `ak_attr_overrides` (optional): a table of attribute values that can override existing attribute values on the private AK variable.
- `slant_bin` (optional): how to find the slant Xgas bin variable needed to expand the AKs.
- `traceability_scale` (optional): how to find the variable containing the WMO or analogous scale for this data.
- `required` (optional): this is `true` by default; set it to `false` if you want to copy this Xgas if present but it is not an error if it is missing.

`prior_profile`, `prior_xgas`, `ak`, `slant_bin`, and `traceability_scale` can all be defined following the syntax in the [ancillary variable specifications](#ancillary-variable-specifications).
Note that `slant_bin` is a special case in that it will only be used if the AKs are to be copied, but cannot be omitted in that case.

To illustrate the two main use cases for this section, here is an excerpt from the standard TCCON configuration:

```toml
[[xgas]]
xgas = "xluft"
gas = "luft"
gas_long = "dry air"
prior_profile = { type = "omit" }
prior_xgas = { type = "omit" }
ak = { type = "omit" }

[[xgas]]
xgas = "xco2_x2019"
gas = "co2"
prior_profile = { type = "specified", only_if_first = true, private_name = "prior_1co2", public_name = "prior_co2" }
prior_xgas = { type = "specified", only_if_first = true, private_name = "prior_xco2_x2019", public_name = "prior_xco2" }
ak = { type = "specified", only_if_first = true, private_name = "ak_xco2" }
slant_bin = { type = "specified", private_name = "ak_slant_xco2_bin" }

[[xgas]]
xgas = "xwco2_x2019"
gas = "co2"
prior_profile = { type = "specified", only_if_first = true, private_name = "prior_1co2", public_name = "prior_co2" }
prior_xgas = { type = "specified", only_if_first = true, private_name = "prior_xwco2_x2019", public_name = "prior_xco2" }
ak = { type = "specified", only_if_first = true, private_name = "ak_xwco2" }
slant_bin = { type = "specified", private_name = "ak_slant_xwco2_bin" }
```

First we have `xluft`.
This variable would be discovered by the [default rule](/write_public_netcdf/xgas_discovery.html#rules); however, that rule will require prior information and AKs.
The prior information is not useful for Xluft, so we want to avoid copying that to reduce the number of extraneous variables, and there are no AKs for Xluft.
Thus we specify "omit" for each of these to tell the writer not to look for them.
We do not have to tell it to omit `slant_bin`, because omitting the AKs implicitly skips that, and `traceability_scale` can be left as normal because there is an "aicf_xluft_scale" variable in the private netCDF files.

Second we have `xco2_x2019` and `xwco2_x2019`.
(We have omitted the x2007 variables and `lco2_x2019` from the above example for brevity).
These would not be discovered by the [default rule](/write_public_netcdf/xgas_discovery.html#rules).
Further, the mapping to their prior and AK variables is unique: all the CO2 Xgas variables can share the prior profiles and column averages, and each "flavor" of CO2 (regular, wCO2, or lCO2) can use the same AKs whether it is on the X2007 or X2019 scale.
Thus, we not only define that these variables need copied, but that we want to rename the prior variables to just "prior_co2" and "prior_xco2" and only copy these the first time we find them.
We also ensure that the AKs and slant bins point to the correct variables.

If we wanted to set the `note` attribute of `xluft`, we could do that like so:

```toml
[[xgas]]
xgas = "xluft"
gas = "luft"
gas_long = "dry air"
prior_profile = { type = "omit" }
prior_xgas = { type = "omit" }
ak = { type = "omit" }
xgas_attr_overrides = { note = "This is a diagnostic quantity" }
```

```admonish note
Not all attributes can be overridden, some are set internally by the netCDF writer to ensure consistency.
If an attribute is not getting the value you expect, first check the logging output from the netCDF writer
for a warning that a particular attribute cannot be set.
```


## Ancillary variable specifications

The ancillary variables (prior profile, prior Xgas, AK, slant bin, and traceability scale) can be defined as one of the following three types:

- `inferred`: indicates that this ancillary variable must be included and should not conflict with any other
  variable. The private and public variable names will be inferred from the Xgas and gas names. This type has
  two options:
    - `only_if_first`: a boolean (`false` by default) that when set to `true` will skip copying the relevant
      variable if a variable with the same public name is already in the public file.
      **Note that the writer does not check that the existing variable's data are equal to what would be written for the new variable!**
    - `required`: a boolean (`true` by default) that when set to `false` allows this variable to be missing
      from the private file. This is intended for [Xgas discovery rules](/write_public_netcdf/xgas_discovery.html#rules)
      more than explicit Xgas definitions.
- `specified`: allows you to specify exactly which variable to copy with the `private_name` field. You can also
  give the `public_name` field to indicate what the variable name in the output file should be; if that is omitted,
  then the public variable will have the same name as the private variable. It is an error if the public variable
  already exists. The also allows the `only_if_first` field, which behaves how it does for the `inferred` type.
- `omit`: indicates that this variable should not be copied.

In the following example, the prior Xgas field shows the use of the `inferred` options, the prior profile field
shows the use of the `specified` options, and the AK field the use of `omit`.

```toml
[[xgas]]
xgas = "xhcl"
gas = "hcl"
prior_xgas = { type = "inferred", only_if_first = true, required = false }
prior_profile = { type = "specified", only_if_first = true, private_name = "prior_1hcl", public_name = "prior_hcl" }
ak = { type = "omit" }
```
  
## Ancillary variable name inference

The writer uses the following rules when asked to infer ancillary variable names.
In these, `{xgas_var}` indicates the Xgas variable name and `{gas}` the physical gas name.

- `prior_profile`: looks for a private variable named `prior_1{gas}` and writes to a variable named `prior_{gas}`.
- `prior_xgas`: looks for a private variable named `prior_{xgas_var}` and writes to the same variable.
- `ak`: looks for a private variable named `ak_{xgas_var}` and writes to the same variable.
- `slant_bin`: looks for a private variable named `ak_slant_{xgas_var}_bin`. This is not written, it is only used to expand
  the AKs to one-per-spectrum.
- `traceability_scale`: looks for a private variable named `aicf_{xgas_var}_scale`. The result is always written to the
  `wmo_or_analogous_scale` attribute of the Xgas variable; that cannot be altered by this configuration.