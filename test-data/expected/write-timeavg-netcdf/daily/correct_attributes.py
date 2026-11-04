# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "netCDF4>=1.7,<2",
# ]
# ///
#
# This updates values from Python-created daily files to match what Rust outputs.
# This is only for deliberate changes between the Python and Rust output code.
from datetime import datetime
from netCDF4 import Dataset
from pathlib import Path

FIXED_UNITS = {
    'degrees north': 'degrees_north',
    'degrees east': 'degrees_east',
    'Pascals': 'Pa',
    'Wet air mole fraction in mole/mole': '',
    'Dry air mole fraction in parts per million': 'ppm',
    'Wet air mole fraction in parts per million': 'ppm',
}

NO_LONG_UNITS = {'latitude', 'longitude'}

DESCRIPTIONS = {
    'solar_time_bin': 'The midpoint of each solar time bin.',
    'cdate': 'The mean UTC time of the bin, weighted by the uncertainty in the column averages.',
    'utc_time': 'The mean UTC time of the bin, weighted by the uncertainty in the column averages, in CF-compliant format.',
    'obs_id': 'A unique identifier for each bin in the format YYYYmmddHHMMSSII, where II is the station ID and the rest is the time string of the solar mean time.',
    'station_id': 'The numeric ID of the TCCON station from which each bin was derived.',
    'public': 'A flag indicating if this bin contains only data that would be public at the time of creation (1) or contained at least one private spectrum (0).',
    'bin_flag': 'A flag indicating if the bin is safe to use/include in the daily files.',
    'num_soundings': 'How many TCCON observations are included in the bin',
    'latitude': 'The TCCON latitudes',
    'longitude': 'The TCCON longitudes',
    'sza': 'The mean solar zenith angle of the bin, weighted by the uncertainty in the column averages.',
    'p_surf': 'The TCCON surface pressures, weighted by the uncertainty in the column averages.',
    'p_levels_prior': 'The pressure levels of the priors, with one row per bin.',
    'p_levels_ak': 'The pressure levels of the AKs. In the future, this will be removed once the private files unify the AK and prior pressure levels. Since these do not change, there is a single vector for all bins.',
    'prior_h2o': 'The TCCON H2O priors in wet mole fraction, weighted by the uncertainty in the column averages.',
    'prior_mixing': 'The a priori profile for the retrieved gas, dried with the TCCON H2O profile, and weighted by the column average uncertainties.',
    'prior_mixing_tccon': 'The original TCCON a priori profile for the retrieved gas (in wet mole fraction), weighted by the column average uncertainties.',
    'avg_kernel': 'The averaging kernels, weighted by the column average uncertainties.',
    'column_mixing': 'The bin averaged TCCON column average values, weighted by the uncertainty in the individual observations.',
    'sigma_column_mixing': 'The propagated uncertainty in the bin averaged TCCON column average values.',
}

STD_NAMES = {
    'utc_time': 'time',
    'bin_flag': 'quality_flag',
    'num_soundings': 'number_of_observations',
    'latitude': 'latitude',
    'longitude': 'longitude',
    'sza': 'solar_zenith_angle',
    'p_surf': 'surface_air_pressure',

}

LONG_NAMES = {
    'solar_time_bin': 'midpoint_of_solar_time_bin',
    'cdate': 'date_and_time',
    'obs_id': 'observation_identifier',
    'station_id': 'tccon_station_identifier',
    'public': 'is_data_public',
    'p_levels_prior': 'prior_profile_pressure_levels',
    'p_levels_ak': 'averaging_kernel_pressure_levels',
    'prior_h2o': 'a_priori_water_profile',
    'prior_mixing': 'a_priori_profile',
    'prior_mixing_tccon': 'a_priori_profile',
    'avg_kernel': 'averaging_kernel',
    'column_mixing': 'column_average_mole_fraction',
    'sigma_column_mixing': 'column_average_mole_fraction_uncertainty'
}

now = datetime.now().strftime('%Y%m%d_%H%M')
for f in Path('.').glob('*.nc4'):
    f.copy(f'{f}.bak{now}')
    print(f'Baked up {f}')
    with Dataset(f, 'a') as ds:
        ds.Conventions = 'CF-1.13'
        # The references string is correct, but Python writes as a Strs() type (in Rust lingo)
        # because of non-ASCII characters. Encoding as bytes makes it write the Str() type.
        ds.references = ds.references.encode('utf-8')

        for grp in ds.groups.values():
            # Create new variables (as empty) so that checking for existence passes
            grp.createVariable('bin_flag', 'u8', ('n_obs_instr',))
            grp.createVariable('utc_time', 'i8', ('n_obs_instr',))
            # Fixes for all variables
            for varname, var in grp.variables.items():
                if hasattr(var, 'unit'):
                    long_units = var.unit
                    units = FIXED_UNITS.get(long_units, long_units)
                    del var.unit
                    var.units = units
                    print(f'Set units on {varname} to "{units}"')
                    if varname not in NO_LONG_UNITS and units != long_units:
                        var.long_units = long_units
                        print(f'Set long_units on {varname} to "{long_units}"')
                if varname in DESCRIPTIONS:
                    var.description = DESCRIPTIONS[varname]
                    print(f'Updated description on {varname}"')
                if varname in STD_NAMES:
                    var.standard_name = STD_NAMES[varname]
                    print(f'Updated standard_name on {varname}')
                if varname in LONG_NAMES:
                    var.long_name = LONG_NAMES[varname]
                    print(f'Updated long_name on {varname}')

            # More specific changes
            grp['utc_time'].comment = 'This contains the same times as cdate, but in CF-compliant format.'
            grp['utc_time'].units = 'seconds since 1970-01-01 00:00:00Z'
            grp['p_levels_ak'].comment = 'First index is the surface layer, last index is TOA'
            grp['prior_h2o'].comment = 'First index is the surface layer, last index is TOA'
            grp['prior_mixing_tccon'].comment = 'First index is the surface layer, last index is TOA'
            grp['bin_flag'].units = '1'
            grp['public'].flag_meanings = 'bin_contained_private_data bin_contained_only_public_data'
            grp['avg_kernel'].units = '1'
            grp['num_soundings'].units = '1'
            grp['station_id'].station_006_all_public_after = '2025-10-08'
            grp['station_id'].station_006_tccon_long_name = 'pasadena01'
            grp['station_id'].station_029_all_public_after = '2025-10-07'
            grp['station_id'].station_029_tccon_long_name = 'parkfalls01'
            del grp['obs_id'].comment # now in the description
