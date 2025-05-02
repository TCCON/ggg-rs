# Defaults

Unlike other sections, the `[defaults]` section does not define variables to copy; instead, it modifies how the other sections are filled in.
If this section is omitted, then each of the other sections will add reasonable default values if omitted.
The following boolean options are available to change that behavior:

- `disable_all`: setting this to `true` will ensure that no defaults are added in any section.
- `aux_vars`: setting this to `false` will prevent TCCON standard auxiliary variables from being added in the `aux` section.
- `gas_long_names`: setting this to `false` will prevent the standard mapping of chemical formulae to proper gas names being added to `[gas_long_names]`.
- `xgas_rules`: setting this to `false` will prevent the standard list of patterns to match when looking for Xgas variables from being added to `[discovery.rules]`.