# NetCDF writer checklist

## Variables

- [ ] AKs
    - Will need to implement interpolation in the initial file
- [ ] .mav prior and cell values
    - Implement expanding the priors right away (no `prior_index`)
- Extra values from the .mod/.vmr files
    - [ ] Equivalent latitude profiles
    - [ ] Tropopause altitude
    - [ ] Mod file name
    - [ ] VMR file name
    - [ ] Effective latitude profile
    - [ ] Mid-tropospheric potential temperature
- [ ] Checksums (need to figure out where these come from)
- [ ] Gfit and gsetup versions (need to figure out where these come from)
- [ ] Flag and flagged var name (calculated, or should this come from the oof?)
- [ ] Spectrum name (should come from runlog now)
- [ ] Aux variables (`year` through `h2o_dmf_mod`, also need to determine where these come from)
- [ ] `.vsw` column and column errors
- [ ] `vsw_sf` values - from the `.tsw` file? Or the window-to-window values in the `.vsw` header?
- [ ] `.vsw.ada` xgas and xgas errors
- [ ] `.vav.ada` xgas and xgas errors
- [ ] `.vav.ada.aia` xgas and xgas errors
    - NB: I think this have the `qc.dat` precision/scaling applied and possibly error scaling?
    - For the new version, we should not apply the precision - that isn't useful for a netCDF file and, as seen with `zmin`, can cause issues with lost precision.
- [ ] `.tav` mean scale factors and errors
- [ ] `.vav` mean column densities and errors
- [ ] Laser sampling variables (`lst`, `lse`, `lsu`, `lsf`, `mvd`, `dip`, `dip_si`/`dip_insb`) - presume these are read from the `.lse` file
- [ ] Airmass correction values (per window/gas `_adcf`, `_adcf_error`, `_p`, and `_g` - NB last two might not be present)
- [ ] In situ correction values (per gas for now, `_aicf`, `_aicf_error`, and `_aicf_scale`)
- [ ] `.col` file variables (`*_nit`, `*_cl`, `*_ct`, `*_cc`, `*_fs`, `*_sg`, `*_zo`, `*_rmsocl`, `*_zpres`, then `*_am_*`, `*_ovc_*`, `*_vsf_*`, and `*_vsf_error_*` per gas in the window)
- [ ] `.cbf` file variables (`*_ncbf`, `*_cfampocl`, `*_cfphase`, `*_cfperiod`, `_cbf_##`)

## To add
- For each variable, an attribute that lists the source file name or full path?

## Notes
- Definitely faster to use `write_values` ones than `write_value` many times in a loop -
  for a 5M element f32 variable, the former took 0.02 s, the latter 16.29 s (almost 200x
  slower).