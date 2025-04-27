# Gas proper names

Ideally, all Xgases should include their proper name in the `long_name` attribute, rather than just its abbreviation.
This section allows you to map the formula (e.g., "co2") to the proper name (e.g., "carbon dioxide"), e.g.:

```toml
[gas_long_names]
co2 = "carbon dioxide"
ch4 = "methane"
co = "carbon monoxide"
```

Note that the keys are the gases, not Xgases.
A default list is included if not turned off in the [`[Defaults]`](/write_public_netcdf/defaults.html) section.
See the source code for [`DEFAULT_GAS_LONG_NAMES`](https://github.com/TCCON/ggg-rs/blob/main/src/bin/write_public_netcdf/constants.rs) for the current list.
You can override any of those without turning off the defaults; e.g., setting `h2o = "dihydrogen monoxide"` in this section will replace the default of "water".

Of course, when [explicitly defining an Xgas to copy](/write_public_netcdf/explicit_xgases.html), you can write in the proper name as the `gas_long` value.
The `[gas_long_names]` section is most useful for automatically discovered Xgases, but it can also be useful when defining multiple Xgas variables that refer to the same physical gas, as the [standard TCCON configuration](https://github.com/TCCON/ggg-rs/blob/main/src/bin/write_public_netcdf/tccon_configs/standard.toml) does with CO2.