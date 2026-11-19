use std::fmt::{Debug, Display};
use std::path::Path;

use chrono::{DateTime, Datelike, NaiveDateTime, Timelike, Utc};
use error_stack::ResultExt;
use ggg_rs::nc_utils::{get_string_attr, get_var_data, map_chrono_i64, parse_release_lag};
use ggg_rs::units::map_values_to_quantity;
use ggg_rs::utils::site_id_from_filename;
use ggg_rs_derive::{Concat, ReadNetcdf, Subset, WriteNetcdf};
use indexmap::IndexMap;
use itertools::Itertools;
use ndarray::{Array1, Array2, Axis};
use uom::si::angle::degree;
use uom::si::f32::{Angle, Pressure, Ratio};
use uom::si::pressure::pascal;
use uom::si::ratio::{part_per_million, ratio};

use crate::CliError;

///
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, clap::ValueEnum, strum::Display)]
pub(crate) enum Gas {
    CO2,
    CH4,
}

impl Gas {
    pub(crate) fn output_group(&self) -> &'static str {
        match self {
            Gas::CO2 => "CO2",
            Gas::CH4 => "CH4",
        }
    }
}

pub(crate) type AllGasAvgData = IndexMap<Gas, TimeAvgData>;

/// A structure representing time-averaged data from one or more TCCON sites.
#[derive(ReadNetcdf, WriteNetcdf, Subset, Concat)]
#[cfg_attr(test, derive(Debug, Default, PartialEq))]
#[netcdf(describe, context(write = AllSitesMetaAttrs))]
pub(crate) struct TimeAvgData {
    /// The midpoint of each solar time bin.
    #[netcdf(
        dimensions("n_obs_instr", "idate"),
        attr(name = "long_name", value = "midpoint_of_solar_time_bin"),
        map(write = map_datetimes_to_cdate, read = map_cdate_to_naive_datetimes), kind = "array"
    )]
    pub(crate) solar_time_bin: Array1<NaiveDateTime>,

    /// The mean UTC time of the bin, weighted by the uncertainty in the column
    /// averages.
    #[netcdf(
        dimensions("n_obs_instr", "idate"),
        attr(name = "long_name", value = "date_and_time"),
        map(write = map_datetimes_to_cdate, read = map_cdate_to_datetimes), kind = "array"
    )]
    // Note: this maps to the 2D array with the datetime parts as the second dimension.
    pub(crate) cdate: Array1<DateTime<Utc>>,

    /// The mean UTC time of the bin, weighted by the uncertainty in the column
    /// averages, in CF-compliant format.
    #[netcdf(
        dimensions("n_obs_instr"),
        rename = "utc_time",
        kind = "array",
        attr(name = "standard_name", value = "time"),
        attr(name = "comment", value = "This contains the same times as cdate, but in CF-compliant format."),
        map_units(write = Self::map_utc_cf_time, read = Self::noop::<Array1<i64>>)
    )]
    #[subset(skip)]
    #[concat(skip)]
    // Note: this is written by converting cdate; no code should reference this (obviously).
    pub(crate) _utc_time: (),

    /// A unique identifier for each bin in the format YYYYmmddHHMMSSII, where
    /// II is the station ID and the rest is the time string of the solar mean time.
    #[netcdf(
        dimensions("n_obs_instr"),
        attr(name = "long_name", value = "observation_identifier")
    )]
    pub(crate) obs_id: Array1<i64>,

    /// The numeric ID of the TCCON station from which each bin was derived.
    #[netcdf(
        dimensions("n_obs_instr"),
        attr(name = "long_name", value = "tccon_station_identifier"),
        attrs(func = Self::station_id_attrs)
    )]
    pub(crate) station_id: Array1<i8>,

    /// A flag indicating if this bin contains only data that would be public
    /// at the time of creation (1) or contained at least one private spectrum (0).
    #[netcdf(
        dimensions("n_obs_instr"),
        attr(name = "long_name", value = "is_data_public"),
        attr( name = "flag_values", value = vec![0_i8, 1_i8]),
        attr( name = "flag_meanings", value = "bin_contained_private_data bin_contained_only_public_data")
    )]
    pub(crate) public: Array1<i8>,

    /// A flag indicating if the bin is safe to use/include in the daily files.
    #[netcdf(
        dimensions("n_obs_instr"),
        attr(name = "standard_name", value = "quality_flag"),
        attr(name = "units", value = "1")
    )]
    // bin flags are to be added separately by the flagging object
    pub(crate) bin_flag: Array1<u64>,

    /// How many TCCON observations are included in the bin
    #[netcdf(
        dimensions("n_obs_instr"),
        attr(name = "standard_name", value = "number_of_observations"),
        attr(name = "units", value = "1")
    )]
    pub(crate) num_soundings: Array1<u64>,

    /// The TCCON latitudes
    #[netcdf(
        dimensions("n_obs_instr"),
        map(write = ggg_rs::units::angle::map::<degree, _, _, _, _>, read = map_deg_to_q),
        attr(name = "standard_name", value = "latitude"),
        attr(name = "units", value = "degrees_north"),
        kind = "array",
    )]
    pub(crate) latitude: Array1<Angle>,

    /// The TCCON longitudes
    #[netcdf(
        dimensions("n_obs_instr"),
        map(write = ggg_rs::units::angle::map::<degree, _, _, _, _>, read = map_deg_to_q),
        attr(name = "standard_name", value = "longitude"),
        attr(name = "units", value = "degrees_east"),
        kind = "array",
    )]
    pub(crate) longitude: Array1<Angle>,

    /// The mean solar zenith angle of the bin, weighted by the uncertainty in
    /// the column averages.
    #[netcdf(
        dimensions("n_obs_instr"),
        map(write = ggg_rs::units::angle::map::<degree, _, _, _, _>, read = map_deg_to_q),
        attr(name = "standard_name", value = "solar_zenith_angle"),
        attr(name = "units", value = "degrees"),
        attr(name = "comment", value = "Calculated as the weighted mean of SZAs in the bin, not the SZA at the bin center time."),
        kind = "array",
    )]
    pub(crate) sza: Array1<Angle>,

    /// The TCCON surface pressures, weighted by the uncertainty in the column
    /// averages.
    #[netcdf(
        dimensions("n_obs_instr"),
        attr(name = "standard_name", value = "surface_air_pressure"),
        attr(name = "long_units", value = "Pascals"),
        map_units(write = ggg_rs::units::pressure::map_units::<pascal, _, _, _, _>, read = map_values_to_quantity::<Pressure, f32, _>),
        kind = "array",
    )]
    pub(crate) p_surf: Array1<Pressure>,

    /// The pressure levels of the priors, with one row per bin.
    #[netcdf(
        dimensions("n_obs_instr", "n_lev"),
        attr(name = "long_name", value = "prior_profile_pressure_levels"),
        attr(name = "long_units", value = "Pascals"),
        map_units(write = ggg_rs::units::pressure::map_units::<pascal, _, _, _, _>, read = map_values_to_quantity::<Pressure, f32, _>),
        kind = "array",
    )]
    pub(crate) p_levels_prior: Array2<Pressure>,

    /// The pressure levels of the AKs. In the future, this will
    /// be removed once the private files unify the AK and prior
    /// pressure levels. Since these do not change, there is a
    /// single vector for all bins.
    #[netcdf(
        dimensions("n_lev"),
        attr(name = "long_name", value = "averaging_kernel_pressure_levels"),
        attr(name = "long_units", value = "Pascals"),
        attr(name = "comment", value = "First index is the surface layer, last index is TOA"),
        map_units(write = ggg_rs::units::pressure::map_units::<pascal, _, _, _, _>, read = map_values_to_quantity::<Pressure, f32, _>),
        kind = "array",
    )]
    #[subset(skip)]
    #[concat(skip)]
    pub(crate) p_levels_ak: Array1<Pressure>,

    /// The TCCON H2O priors in wet mole fraction, weighted by the uncertainty
    /// in the column averages.
    #[netcdf(
        dimensions("n_obs_instr", "n_lev"),
        attr(name = "long_name", value = "a_priori_water_profile"),
        attr(name = "long_units", value = "Wet air mole fraction in mole/mole"),
        attr(name = "comment", value = "First index is the surface layer, last index is TOA"),
        map_units(write = ggg_rs::units::ratio::map_units::<ratio, _, _, _, _>, read = map_values_to_quantity::<Ratio, f32, _>),
        kind = "array",
    )]
    pub(crate) prior_h2o: Array2<Ratio>,

    /// The a priori profile for the retrieved gas, dried with the TCCON H2O profile,
    /// and weighted by the column average uncertainties.
    #[netcdf(
        dimensions("n_obs_instr", "n_lev"),
        attr(name = "comment", value = "First index is the surface layer, last index is TOA"),
        attr(name = "long_name", value = "a_priori_profile"),
        attr(name = "long_units", value = "Dry air mole fraction in parts per million"),
        attr(name = "adjustment", value = "This has been dried with the TCCON prior water profile"),
        map_units(write = ggg_rs::units::ratio::map_units::<part_per_million, _, _, _, _>, read = map_values_to_quantity::<Ratio, f32, _>),
        kind = "array",
    )]
    pub(crate) prior_mixing: Array2<Ratio>,

    /// The original TCCON a priori profile for the retrieved gas (in wet mole fraction),
    /// weighted by the column average uncertainties.
    #[netcdf(
        dimensions("n_obs_instr", "n_lev"),
        attr(name = "long_name", value = "a_priori_profile"),
        attr(name = "comment", value = "First index is the surface layer, last index is TOA"),
        attr(name = "long_units", value = "Wet air mole fraction in parts per million"),
        map_units(write = ggg_rs::units::ratio::map_units::<part_per_million, _, _, _, _>, read = map_values_to_quantity::<Ratio, f32, _>),
        kind = "array",
    )]
    pub(crate) prior_mixing_tccon: Array2<Ratio>,

    /// The averaging kernels, weighted by the column average uncertainties.
    #[netcdf(
        dimensions("n_obs_instr", "n_lev"),
        attr(name = "long_name", value = "averaging_kernel"),
        attr(name = "units", value = "1"),
        attr(
            name = "comment",
            value = "First index is the surface layer, last index is TOA"
        )
    )]
    pub(crate) avg_kernel: Array2<f32>,

    /// The bin averaged TCCON column average values, weighted by the uncertainty
    /// in the individual observations.
    #[netcdf(
        dimensions("n_obs_instr"),
        attr(name = "long_name", value = "column_average_mole_fraction"),
        attr(name = "long_units", value = "Dry air mole fraction in parts per million"),
        map_units(write = ggg_rs::units::ratio::map_units::<part_per_million, _, _, _, _>, read = map_values_to_quantity::<Ratio, f32, _>),
        kind = "array",
    )] // NB: the WMO scale is written through its field on this struct.
    pub(crate) column_mixing: Array1<Ratio>,

    /// The propagated uncertainty in the bin averaged TCCON column average values.
    #[netcdf(
        dimensions("n_obs_instr"),
        attr(name = "long_name", value = "column_average_mole_fraction_uncertainty"),
        attr(name = "long_units", value = "Dry air mole fraction in parts per million"),
        map_units(write = ggg_rs::units::ratio::map_units::<part_per_million, _, _, _, _>, read = map_values_to_quantity::<Ratio, f32, _>),
        kind = "array",
    )]
    pub(crate) sigma_column_mixing: Array1<Ratio>,

    /// The WMO or analagous calibration scale to which the column mixing values are tied.
    #[netcdf(custom(write = Self::add_wmo_scale_attr, read = Self::get_wmo_scale_attr))]
    #[subset(skip)]
    #[concat(skip = "equal")]
    pub(crate) wmo_or_analagous_scale: String,
}

impl TimeAvgData {
    pub(crate) fn new_with_bins(
        solar_time_bin: Array1<NaiveDateTime>,
        nlev: usize,
        wmo_scale: String,
    ) -> Self {
        let nbin = solar_time_bin.len();
        Self {
            solar_time_bin,
            cdate: Array1::from_elem(nbin, chrono::DateTime::from_timestamp_nanos(0)),
            _utc_time: (),
            obs_id: Array1::from_elem(nbin, i64::MIN),
            station_id: Array1::from_elem(nbin, i8::MIN),
            public: Array1::from_elem(nbin, i8::MIN),
            bin_flag: Array1::from_elem(nbin, 0),
            num_soundings: Array1::from_elem(nbin, 0),
            latitude: Array1::from_elem(nbin, Angle::new::<uom::si::angle::degree>(f32::MIN)),
            longitude: Array1::from_elem(nbin, Angle::new::<uom::si::angle::degree>(f32::MIN)),
            sza: Array1::from_elem(nbin, Angle::new::<uom::si::angle::degree>(f32::MIN)),
            p_surf: Array1::from_elem(nbin, Pressure::new::<uom::si::pressure::pascal>(f32::MIN)),
            p_levels_prior: Array2::from_elem(
                [nbin, nlev],
                Pressure::new::<uom::si::pressure::pascal>(f32::MIN),
            ),
            p_levels_ak: Array1::from_elem(
                nlev,
                Pressure::new::<uom::si::pressure::pascal>(f32::MIN),
            ),
            prior_h2o: Array2::from_elem(
                [nbin, nlev],
                Ratio::new::<uom::si::ratio::ratio>(f32::MIN),
            ),
            prior_mixing: Array2::from_elem(
                [nbin, nlev],
                Ratio::new::<uom::si::ratio::ratio>(f32::MIN),
            ),
            prior_mixing_tccon: Array2::from_elem(
                [nbin, nlev],
                Ratio::new::<uom::si::ratio::ratio>(f32::MIN),
            ),
            avg_kernel: Array2::from_elem([nbin, nlev], f32::MIN),
            column_mixing: Array1::from_elem(nbin, Ratio::new::<uom::si::ratio::ratio>(f32::MIN)),
            sigma_column_mixing: Array1::from_elem(
                nbin,
                Ratio::new::<uom::si::ratio::ratio>(f32::MIN),
            ),
            wmo_or_analagous_scale: wmo_scale,
        }
    }

    fn add_wmo_scale_attr<'a>(
        &'a self,
        scale: &str,
        _rust_name: &str,
        _nc_name: &str,
        grp: &mut netcdf::GroupMut<'_>,
        _ctx: &AllSitesMetaAttrs,
    ) -> netcdf::Result<()> {
        let mut var = grp
            .variable_mut("column_mixing")
            .expect("the column_mixing variable must be written before the WMO scale value");
        var.put_attribute("wmo_or_analogous_scale", scale)
            .map(|_| ())
    }

    fn get_wmo_scale_attr(
        _rust_name: &str,
        _nc_name: &str,
        grp: &netcdf::Group<'_>,
    ) -> netcdf::Result<String> {
        let var = grp.variable("column_mixing").ok_or_else(|| {
            netcdf::Error::Str("the column_mixing variable could not be found".to_string())
        })?;
        let scale = get_string_attr(&var, "wmo_or_analogous_scale").map_err(|e| {
            netcdf::Error::Str(format!("could not retrieve the WMO scale value: {e}"))
        })?;
        Ok(scale)
    }

    fn map_utc_cf_time(&self, _: &(), name: &str) -> (Array1<i64>, &'static str) {
        map_chrono_i64(self, &self.cdate, name)
    }

    fn noop<A>(
        _data: A,
        _field: &str,
        _var: &netcdf::Variable,
        _units: Option<&str>,
    ) -> netcdf::Result<()> {
        Ok(())
    }

    fn station_id_attrs<'a>(
        &'a self,
        station_ids: &Array1<i8>,
        _rs_name: &str,
        _nc_name: &str,
        var: &mut netcdf::VariableMut<'_>,
        ctx: &AllSitesMetaAttrs,
    ) -> netcdf::Result<()> {
        let mut unique_stations = station_ids.iter().copied().unique().collect_vec();
        unique_stations.sort();
        var.put_attribute("num_stations", unique_stations.len() as i32)?;

        for id in unique_stations {
            let meta = ctx.meta_for_station(id).ok_or_else(|| {
                netcdf::Error::Str(format!("No metadata available for station ID = {id}"))
            })?;
            meta.write_station_var_attrs(id, var)?;
        }

        Ok(())
    }

    pub(crate) fn read_gas_from_file(
        nc_file: &Path,
        gas: &Gas,
    ) -> error_stack::Result<Self, CliError> {
        let nc = netcdf::open(&nc_file).change_context_lazy(|| {
            CliError::Context(format!("error opening netCDF file {}", nc_file.display()))
        })?;
        let nc = nc
            .group(gas.output_group())
            .change_context_lazy(|| {
                CliError::Context(format!(
                    "error getting group '{}' from {}",
                    gas.output_group(),
                    nc_file.display()
                ))
            })?
            .ok_or_else(|| {
                CliError::Context(format!(
                    "group '{}' not present in {}",
                    gas.output_group(),
                    nc_file.display()
                ))
            })?;
        let data = TimeAvgData::read_netcdf(&nc).change_context_lazy(|| {
            CliError::Context(format!(
                "error reading from group '{}' of {}",
                gas.output_group(),
                nc_file.display()
            ))
        })?;
        Ok(data)
    }
}

/// For the degree quantities, we use map for writing to control the exact unit.
/// This means we need to use a function on read that extracts the units from the
/// variable directly.
fn map_deg_to_q(
    arr: Array1<f32>,
    name: &str,
    var: &netcdf::Variable<'_>,
) -> netcdf::Result<Array1<Angle>> {
    let units: String = var
        .attribute_value("units")
        .ok_or_else(|| netcdf::Error::NotFound("attribute: 'units'".to_string()))??
        .try_into()
        .map_err(|e| netcdf::Error::Str(format!("cannot convert units to string: {e}")))?;
    map_values_to_quantity(arr, name, var, Some(units.as_str()))
}

/// A structure representing level 2 data from a single TCCON site.
/// If read from a pre-GGG2020.2 private file, this must handle converting
/// the priors and AKs to one-per-spectrum and putting them on the same
/// pressure levels.
pub(crate) struct Level2Data {
    /// The UTC time of each observation
    pub(crate) utc_time: Array1<DateTime<Utc>>,

    /// The solar time of each observation
    pub(crate) solar_time: Array1<NaiveDateTime>,

    /// The numeric ID of the source station
    pub(crate) station_id: i8,

    /// Whether this data would be public (1) or not (0) at the
    /// current time
    pub(crate) is_public: Array1<i8>,

    /// L2 quality flag for each spectrum
    pub(crate) flag: Array1<i32>,

    /// The latitude of each observation
    pub(crate) latitude: Array1<Angle>,

    /// The longitude of each observation
    pub(crate) longitude: Array1<Angle>,

    /// The solar zenith angle of each observation
    pub(crate) sza: Array1<Angle>,

    /// The surface pressure of each observation.
    pub(crate) p_surf: Array1<Pressure>,

    /// The pressure levels of the priors. These must
    /// be expanded to one row per observation (not on prior times).
    pub(crate) p_levels_prior: Array2<Pressure>,

    /// The pressure levels of the AKs. In the future, this will
    /// be removed once the private files unified the AK and prior
    /// pressure levels.
    pub(crate) p_levels_ak: Array1<Pressure>,

    /// The water profile in wet mole fraction. These must be
    /// expanded to one row per observation (not on prior time).
    pub(crate) prior_h2o_wet: Array2<Ratio>,

    /// The target gas profile in dry mole fraction, dried using the
    /// H2O profile given in the `prior_h2o_wet` field. These must be
    /// expanded to one row per observation (not on prior time).
    pub(crate) prior_dry: Array2<Ratio>,

    /// The target gas profile in wet mole fraction. These must be
    /// expanded to one row per observation (not on prior time).
    pub(crate) prior_wet: Array2<Ratio>,

    /// The target gases AKs, expanded to one row per observation
    /// (not as lookup tables).
    pub(crate) avg_kernel: Array2<f32>,

    /// The column average Xgas values.
    pub(crate) xgas: Array1<Ratio>,

    /// The column averaged Xgas L2 uncertainties.
    pub(crate) xgas_error: Array1<Ratio>,

    /// The WMO or equivalent scale to which the `Xgas` variables are tied.
    pub(crate) xgas_wmo_scale: String,
}

impl Debug for Level2Data {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ii = ndarray::s![0..5];
        let ii2 = ndarray::s![0..5, 0..2];
        let jj = ndarray::s![0..2];
        f.debug_struct("Level2Data")
            .field("utc_time", &self.utc_time.slice(ii))
            .field("solar_time", &self.solar_time.slice(ii))
            .field("station_id", &self.station_id)
            .field("is_public", &self.is_public.slice(ii))
            .field("latitude", &self.latitude.slice(ii))
            .field("longitude", &self.longitude.slice(ii))
            .field("sza", &self.sza.slice(ii))
            .field("p_surf", &self.p_surf.slice(ii))
            .field("p_levels_prior", &self.p_levels_prior.slice(ii2))
            .field("p_levels_ak", &self.p_levels_ak.slice(jj))
            .field("prior_h2o_wet", &self.prior_h2o_wet.slice(ii2))
            .field("prior_dry", &self.prior_dry.slice(ii2))
            .field("prior_wet", &self.prior_wet.slice(ii2))
            .field("avg_kernel", &self.avg_kernel.slice(ii2))
            .field("xgas", &self.xgas.slice(ii))
            .field("xgas_error", &self.xgas_error.slice(ii))
            .field("xgas_wmo_scale", &self.xgas_wmo_scale)
            .field(
                "-- note --",
                &"fields truncated to at most 5 times, 2 levels",
            )
            .finish()
    }
}

impl Level2Data {
    pub(crate) fn subset(self, indices: &[usize]) -> Self {
        // TODO: maybe this can be a macro that catches any new fields?
        Self {
            utc_time: self.utc_time.select(Axis(0), indices),
            solar_time: self.solar_time.select(Axis(0), indices),
            station_id: self.station_id,
            is_public: self.is_public.select(Axis(0), indices),
            flag: self.flag.select(Axis(0), indices),
            latitude: self.latitude.select(Axis(0), indices),
            longitude: self.longitude.select(Axis(0), indices),
            sza: self.sza.select(Axis(0), indices),
            p_surf: self.p_surf.select(Axis(0), indices),
            p_levels_prior: self.p_levels_prior.select(Axis(0), indices),
            // p_levels_ak are a 1D vector of consistent pressures, so they don't
            // get subset.
            p_levels_ak: self.p_levels_ak,
            prior_h2o_wet: self.prior_h2o_wet.select(Axis(0), indices),
            prior_dry: self.prior_dry.select(Axis(0), indices),
            prior_wet: self.prior_wet.select(Axis(0), indices),
            avg_kernel: self.avg_kernel.select(Axis(0), indices),
            xgas: self.xgas.select(Axis(0), indices),
            xgas_error: self.xgas_error.select(Axis(0), indices),
            xgas_wmo_scale: self.xgas_wmo_scale,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, ReadNetcdf)]
#[netcdf(context(ty = i8))]
pub(crate) struct StationMetaAttrs {
    /// The long name of the TCCON station defined in the MIP mapping.
    /// Usually, this is the TCCON name without trailing numbers,
    /// e.g., "parkfalls".
    #[netcdf(custom(read = Self::get_from_var))]
    pub(crate) station_name: String,

    /// The long name of the TCCON station as used by the network.
    /// This is the version with the tailing numbers, e.g. "parkfalls01".
    #[netcdf(custom(read = Self::get_from_var))]
    pub(crate) tccon_long_name: String,

    /// The number of days from acquisition for which data from this station
    /// is withheld from the public archive. `None` if L2 data came from a public
    /// file.
    #[netcdf(custom(read = Self::get_opt_from_var))]
    pub(crate) source_release_lag: Option<String>,

    /// The DOI for the level 2 data from this site, starting with "10."
    #[netcdf(custom(read = Self::get_from_var))]
    pub(crate) source_data_doi: String,

    /// The revision identifier for the level 2 data from this site, usually
    /// "R0", "R1", etc.
    #[netcdf(custom(read = Self::get_from_var))]
    pub(crate) source_data_revision: String,

    /// The citation for the level 2 data from this site.
    #[netcdf(custom(read = Self::get_from_var))]
    pub(crate) source_data_reference: String,

    /// The date/time after which all data in this file will be public.
    /// `None` if the L2 data came from a public file.
    #[netcdf(custom(read = Self::get_opt_date_from_var))]
    pub(crate) all_public_after: Option<chrono::NaiveDate>,
}

impl StationMetaAttrs {
    /// Write the attributes for the station_id variable for this station.
    /// Note that the numeric station ID must be passed, as it is not included
    /// in the metadata.
    pub(crate) fn write_station_var_attrs(
        &self,
        station_id: i8,
        var: &mut netcdf::VariableMut<'_>,
    ) -> netcdf::Result<()> {
        // Must use this to write the attributes, rather than an implementation
        // of WriteNetcdf with custom overrides; that needs a group passed in to
        // the custom functions, but to call this from the station_id attrs() annotation,
        // it must take a variable.
        var.put_attribute(
            &Self::attr_name(station_id, None),
            self.station_name.as_str(),
        )?;
        var.put_attribute(
            &Self::attr_name(station_id, Some("tccon_long_name")),
            self.tccon_long_name.as_str(),
        )?;
        var.put_attribute(
            &Self::attr_name(station_id, Some("source_release_lag")),
            self.source_release_lag.as_deref().unwrap_or("N/A"),
        )?;
        var.put_attribute(
            &Self::attr_name(station_id, Some("source_data_doi")),
            self.source_data_doi.as_str(),
        )?;
        var.put_attribute(
            &Self::attr_name(station_id, Some("source_data_revision")),
            self.source_data_revision.as_str(),
        )?;
        var.put_attribute(
            &Self::attr_name(station_id, Some("source_data_reference")),
            self.source_data_reference.as_str(),
        )?;

        let public_after = self
            .all_public_after
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "N/A".to_string());
        var.put_attribute(
            &Self::attr_name(station_id, Some("all_public_after")),
            public_after,
        )?;

        Ok(())
    }

    fn get_from_var(
        _rust_name: &str,
        nc_name: &str,
        grp: &netcdf::Group<'_>,
        station_id: &i8,
    ) -> netcdf::Result<String> {
        let var = grp.variable("station_id").ok_or_else(|| {
            netcdf::Error::Str(
                "expected a variable named 'station_id' on the input group".to_string(),
            )
        })?;

        let opt_nc_name = if nc_name == "station_name" {
            None
        } else {
            Some(nc_name)
        };

        get_string_attr(&var, &Self::attr_name(*station_id, opt_nc_name))
            .map_err(|e| netcdf::Error::Str(e.to_string()))
    }

    fn get_opt_from_var(
        _rust_name: &str,
        nc_name: &str,
        grp: &netcdf::Group<'_>,
        station_id: &i8,
    ) -> netcdf::Result<Option<String>> {
        let val = Self::get_from_var(_rust_name, nc_name, grp, station_id)?;
        if val == "N/A" {
            Ok(None)
        } else {
            Ok(Some(val))
        }
    }

    fn get_opt_date_from_var(
        _rust_name: &str,
        nc_name: &str,
        grp: &netcdf::Group<'_>,
        station_id: &i8,
    ) -> netcdf::Result<Option<chrono::NaiveDate>> {
        let s = Self::get_opt_from_var(_rust_name, nc_name, grp, station_id)?;
        if let Some(s) = s {
            let date = chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d").map_err(|e| {
                netcdf::Error::Str(format!(
                    "could not parse '{s}' as a date in YYYY-MM-DD format: {e}"
                ))
            })?;
            Ok(Some(date))
        } else {
            Ok(None)
        }
    }

    fn attr_name(station_id: i8, name: Option<&str>) -> String {
        if let Some(name) = name {
            format!("station_{station_id:03}_{name}")
        } else {
            format!("station_{station_id:03}")
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct StationJsonCitation {
    pub(crate) tccon_site_id: String,
    pub(crate) tccon_name: String,
    pub(crate) mip_id: i8,
    pub(crate) mip_name: String,
    pub(crate) doi: String,
    pub(crate) text_citation: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct AllStationJsonCitation {
    #[serde(rename = "__comment__")]
    pub(crate) comment: String,
    pub(crate) citations: Vec<StationJsonCitation>,
}

impl AllStationJsonCitation {
    pub(crate) fn new(
        meta: &AllSitesMetaAttrs,
        comment: String,
    ) -> error_stack::Result<Self, CliError> {
        let mut citations = vec![];

        for (tccon_site_id, mip_id, attrs) in meta.iter_site_attrs() {
            citations.push(StationJsonCitation {
                tccon_site_id: tccon_site_id.to_string(),
                tccon_name: attrs.tccon_long_name.clone(),
                mip_id,
                mip_name: attrs.station_name.clone(),
                doi: attrs.source_data_doi.clone(),
                text_citation: attrs.source_data_reference.clone(),
            });
        }

        Ok(Self { comment, citations })
    }
}

#[derive(Debug, Default)]
pub(crate) struct AllSitesMetaAttrs {
    meta: Vec<StationMetaAttrs>,
    site_ids: IndexMap<String, usize>,
    station_ids: IndexMap<i8, usize>,
}

impl AllSitesMetaAttrs {
    pub(crate) fn from_one(meta: StationMetaAttrs, site_id: String, station_id: i8) -> Self {
        let mut out = Self::default();
        out.add_meta(meta, site_id, station_id)
            .expect("adding the first metadata should never error");
        out
    }

    fn add_meta<S: ToString + Display>(
        &mut self,
        meta: StationMetaAttrs,
        site_id: S,
        station_id: i8,
    ) -> error_stack::Result<(), CliError> {
        let idx = self.meta.len();
        self.meta.push(meta);
        if let Some(old_idx) = self.site_ids.insert(site_id.to_string(), idx) {
            return Err(CliError::Custom(format!(
                "site ID {site_id} inferred from multiple input files (#{} and {}), not supported",
                old_idx + 1,
                idx + 1
            ))
            .into());
        }
        if let Some(old_idx) = self.station_ids.insert(station_id, idx) {
            return Err(CliError::Custom(format!("station ID {station_id} inferred from multiple input files (#{} and {}), not supported", old_idx+1, idx+1)).into());
        }
        Ok(())
    }

    pub(crate) fn from_site_files<P: AsRef<Path>>(
        files: &[P],
        gases: &[Gas],
    ) -> error_stack::Result<Self, CliError> {
        let mut out = Self::default();

        let mut meta = vec![];
        for file in files {
            meta.push(Self::load_site_file(file.as_ref(), gases)?);
        }

        // Sorting isn't strictly necessary, but it's nice to have the
        // sites in order in the citation files.
        meta.sort_by_key(|m| m.2);
        for (this_meta, this_site_id, this_station_id) in meta {
            out.add_meta(this_meta, this_site_id, this_station_id)?;
        }

        Ok(out)
    }

    pub(crate) fn meta_for_station(&self, station_id: i8) -> Option<&StationMetaAttrs> {
        self.station_ids
            .get(&station_id)
            // Will only panic if our map holds an incorrect index, which it
            // should not if this was constructed via `from_site_files`
            .map(|idx| &self.meta[*idx])
    }

    pub(crate) fn release_lag_for_station(
        &self,
        station_id: i8,
    ) -> error_stack::Result<Option<chrono::TimeDelta>, CliError> {
        let idx = self
            .station_ids
            .get(&station_id)
            .ok_or_else(|| CliError::Custom(format!("No metadata for station #{station_id}")))?;
        if let Some(lag) = &self.meta[*idx].source_release_lag {
            parse_release_lag(lag)
                .change_context_lazy(|| {
                    CliError::Custom(format!(
                        "error parsing release lag for station #{station_id}"
                    ))
                })
                .map(Some)
        } else {
            Ok(None)
        }
    }
    pub(crate) fn iter_site_attrs(&self) -> impl Iterator<Item = (&str, i8, &StationMetaAttrs)> {
        self.site_ids.iter().map(|(sid, idx)| {
            let attrs = &self.meta[*idx];
            let mip_id = self
                .station_ids
                .iter()
                .fold(
                    None,
                    |acc, (id, this_idx)| {
                        if this_idx == idx {
                            Some(*id)
                        } else {
                            acc
                        }
                    },
                )
                .expect("All indices should be in both the station and site ID maps");
            (sid.as_str(), mip_id, attrs)
        })
    }

    fn load_site_file(
        file: &Path,
        gases: &[Gas],
    ) -> error_stack::Result<(StationMetaAttrs, String, i8), CliError> {
        let ds = netcdf::open(file).change_context_lazy(|| {
            CliError::Context(format!("error opening file {}", file.display()))
        })?;
        let root = ds.root().ok_or_else(|| {
            CliError::Custom(format!(
                "could not get root group while reading station metadata from file {}",
                file.display()
            ))
        })?;
        let site_id = site_id_from_filename(file).change_context_lazy(|| {
            CliError::Custom(format!(
                "error inferring site ID from file path {}",
                file.display()
            ))
        })?;

        // Need to verify that there is only a single station ID across all groups
        let mut station_id: Option<i8> = None;
        let mut meta: Option<StationMetaAttrs> = None;
        for gas in gases {
            let grp_name = gas.output_group();
            let grp = root.group(grp_name).ok_or_else(|| {
                CliError::Custom(format!(
                    "group '{grp_name}' not found in {}",
                    file.display()
                ))
            })?;
            let station_ids = get_var_data::<i8, ndarray::Ix1, _>(&grp, "station_id")
                .change_context_lazy(|| {
                    CliError::Context(format!(
                        "error getting station_id values from group {grp_name} of {}",
                        file.display()
                    ))
                })?;
            if let Some((this_station_id,)) = station_ids.iter().copied().unique().collect_tuple() {
                if station_id.is_some_and(|s| s != this_station_id) {
                    return Err(CliError::Custom(format!(
                        "file {} contains different stations IDs across gas groups",
                        file.display()
                    ))
                    .into());
                } else {
                    station_id = Some(this_station_id);
                }
            } else {
                return Err(CliError::Custom(format!(
                    "file {} contains multiple station IDs in group {grp_name}",
                    file.display()
                ))
                .into());
            };

            let this_meta = StationMetaAttrs::read_netcdf_context(
                &grp,
                &station_id.expect("station_id should be set here"),
            )
            .change_context_lazy(|| {
                CliError::Context(format!(
                    "error occurred while reading station metadata from file {}",
                    file.display()
                ))
            })?;

            if meta.is_some_and(|m| m != this_meta) {
                return Err(CliError::Custom(format!(
                    "metadata for station {} differs across gases in file {}",
                    station_id.unwrap(),
                    file.display()
                ))
                .into());
            } else {
                meta = Some(this_meta);
            }
        }

        if let (Some(sid), Some(meta)) = (station_id, meta) {
            Ok((meta, site_id, sid))
        } else {
            Err(CliError::Custom(format!(
                "no station IDs and/or metadata found in {} (usually means there were no groups",
                file.display()
            ))
            .into())
        }
    }
}

fn map_datetimes_to_cdate<A, D: Datelike + Timelike>(_: A, dt: &Array1<D>, _: &str) -> Array2<i16> {
    let mut cdate = Array2::from_elem([dt.len(), 6], i16::MIN);
    ndarray::Zip::from(cdate.rows_mut())
        .and(dt)
        .for_each(|mut row, d| {
            row[0] = d.year() as i16;
            row[1] = d.month() as i16;
            row[2] = d.day() as i16;
            row[3] = d.hour() as i16;
            row[4] = d.minute() as i16;
            row[5] = d.second() as i16;
        });
    cdate
}

fn map_cdate_to_datetimes(
    cdates: Array2<i16>,
    _: &str,
    _: &netcdf::Variable<'_>,
) -> netcdf::Result<Array1<DateTime<Utc>>> {
    cdate_to_datetimes(cdates).map(|arr| arr.mapv(|d| d.and_utc()))
}

fn map_cdate_to_naive_datetimes(
    cdates: Array2<i16>,
    _: &str,
    _: &netcdf::Variable<'_>,
) -> netcdf::Result<Array1<NaiveDateTime>> {
    cdate_to_datetimes(cdates)
}

pub(crate) fn cdate_to_datetimes(cdates: Array2<i16>) -> netcdf::Result<Array1<NaiveDateTime>> {
    let datetimes: Vec<_> = cdates.rows().into_iter().map(|r| {
        chrono::NaiveDate::from_ymd_opt(r[0] as i32, r[1] as u32, r[2] as u32)
            .ok_or_else(|| netcdf::Error::Str(format!("Invalid year, month, or day in cdate array: year = {}, month = {}, day = {}", r[0], r[1], r[2])))
            .and_then(|date| {
                date.and_hms_opt(r[3] as u32, r[4] as u32, r[5] as u32)
                    .ok_or_else(|| netcdf::Error::Str(format!("Invalid hour, minute, or second in cdate array: hour = {}, minute = {}, second = {}", r[3], r[4], r[5])))
            })
    }).try_collect()?;
    Ok(Array1::from_vec(datetimes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ggg_rs::test_utils::{init_test_logging, utc_dt_ymdhms};
    use ndarray::array;
    use uom::si::pressure::hectopascal;

    #[test]
    fn test_time_avg_data_round_trip() {
        init_test_logging(Some(log::LevelFilter::Debug));

        let orig = TimeAvgData {
            solar_time_bin: array![
                utc_dt_ymdhms(2026, 1, 2, 3, 4, 5).naive_utc(),
                utc_dt_ymdhms(2026, 2, 3, 4, 5, 6).naive_utc()
            ],
            cdate: array![
                utc_dt_ymdhms(2026, 1, 2, 3, 4, 0),
                utc_dt_ymdhms(2026, 2, 3, 4, 5, 0),
            ],
            _utc_time: (),
            obs_id: array![2026010203040501, 2026020304050611],
            station_id: array![1, 11],
            public: array![1, 0],
            bin_flag: array![0, 1],
            num_soundings: array![2, 20],
            latitude: array![Angle::new::<degree>(45.5), Angle::new::<degree>(-45.5)],
            longitude: array![Angle::new::<degree>(90.0), Angle::new::<degree>(-90.0)],
            sza: array![Angle::new::<degree>(30.0), Angle::new::<degree>(35.0)],
            p_surf: array![
                Pressure::new::<hectopascal>(1010.0),
                Pressure::new::<hectopascal>(1013.25)
            ],
            p_levels_prior: array![
                [
                    Pressure::new::<hectopascal>(1000.0),
                    Pressure::new::<hectopascal>(900.0),
                    Pressure::new::<hectopascal>(800.0)
                ],
                [
                    Pressure::new::<hectopascal>(1001.0),
                    Pressure::new::<hectopascal>(901.0),
                    Pressure::new::<hectopascal>(801.0)
                ]
            ],
            p_levels_ak: array![
                Pressure::new::<hectopascal>(1013.0),
                Pressure::new::<hectopascal>(950.0),
                Pressure::new::<hectopascal>(900.0)
            ],
            prior_h2o: array![
                [
                    Ratio::new::<part_per_million>(1000.0),
                    Ratio::new::<part_per_million>(500.0),
                    Ratio::new::<part_per_million>(5.0),
                ],
                [
                    Ratio::new::<part_per_million>(10000.0),
                    Ratio::new::<part_per_million>(5000.0),
                    Ratio::new::<part_per_million>(50.0),
                ]
            ],
            prior_mixing: array![
                [
                    Ratio::new::<part_per_million>(400.0),
                    Ratio::new::<part_per_million>(375.0),
                    Ratio::new::<part_per_million>(355.0),
                ],
                [
                    Ratio::new::<part_per_million>(410.0),
                    Ratio::new::<part_per_million>(380.0),
                    Ratio::new::<part_per_million>(350.0),
                ]
            ],
            prior_mixing_tccon: array![
                [
                    Ratio::new::<part_per_million>(399.0),
                    Ratio::new::<part_per_million>(374.0),
                    Ratio::new::<part_per_million>(354.0),
                ],
                [
                    Ratio::new::<part_per_million>(409.0),
                    Ratio::new::<part_per_million>(379.0),
                    Ratio::new::<part_per_million>(349.0),
                ]
            ],
            avg_kernel: array![[1.5, 1.0, 0.5], [2.0, 1.5, 1.0]],
            column_mixing: array![
                Ratio::new::<part_per_million>(380.0),
                Ratio::new::<part_per_million>(379.0)
            ],
            sigma_column_mixing: array![
                Ratio::new::<part_per_million>(10.0),
                Ratio::new::<part_per_million>(8.0)
            ],
            wmo_or_analagous_scale: "XTEST".to_string(),
        };

        let mut meta = AllSitesMetaAttrs::default();
        meta.add_meta(
            StationMetaAttrs {
                station_name: "first".to_string(),
                tccon_long_name: "first01".to_string(),
                source_release_lag: Some("0 days".to_string()),
                source_data_doi: "10.xxxx".to_string(),
                source_data_revision: "R0".to_string(),
                source_data_reference: "Doe, John. A paper!".to_string(),
                all_public_after: None,
            },
            "fi",
            1,
        )
        .unwrap();
        meta.add_meta(
            StationMetaAttrs {
                station_name: "second".to_string(),
                tccon_long_name: "second02".to_string(),
                source_release_lag: Some("90 days".to_string()),
                source_data_doi: "10.yyyy".to_string(),
                source_data_revision: "R1".to_string(),
                source_data_reference: "Doe, Jane. Another paper!".to_string(),
                all_public_after: None,
            },
            "se",
            11,
        )
        .unwrap();

        let test_dir = ggg_rs::test_utils::test_data_dir();
        let file = test_dir.join("outputs/write-timeavg-netcdf/other/time_avg_round_trip.nc");
        {
            let mut ds = netcdf::create(&file).unwrap();
            let mut root = ds.root_mut().unwrap();
            orig.write_netcdf_context(&mut root, &meta).unwrap();
        }

        let ds = netcdf::open(&file).unwrap();
        let root = ds.root().unwrap();
        let read = TimeAvgData::read_netcdf(&root).unwrap();
        assert_eq!(orig, read);
    }

    #[test]
    fn test_datetime_to_cdate_round_trip() {
        let dates = array![
            utc_dt_ymdhms(2026, 1, 2, 10, 20, 30),
            utc_dt_ymdhms(2026, 2, 3, 20, 30, 40)
        ];
        let expected_cdates = array![[2026, 1, 2, 10, 20, 30], [2026, 2, 3, 20, 30, 40]];
        let test_cdates = map_datetimes_to_cdate(0, &dates, "");
        assert_eq!(expected_cdates, test_cdates, "conversion to cdate failed");
        let test_dates = cdate_to_datetimes(test_cdates)
            .unwrap()
            .mapv(|d| d.and_utc());
        assert_eq!(dates, test_dates, "conversion back to datetimes failed");
    }
}
