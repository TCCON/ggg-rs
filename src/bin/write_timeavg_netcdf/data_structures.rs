use std::fmt::Debug;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use ndarray::{Array1, Array2, Axis};
use uom::si::f32::{Angle, Pressure, Ratio};

/// A structure representing time-averaged data from one or more TCCON sites.
pub(crate) struct TimeAvgData {
    /// The midpoint of each solar time bin bin.
    pub(crate) solar_mid_time: Array1<NaiveDateTime>,

    /// The mean UTC time of the bin, weighted by the uncertainty in the column
    /// averages.
    pub(crate) utc_mean_time: Array1<DateTime<Utc>>,

    /// A unique identifier for each bin in the format YYYYmmddHHMMSSII, where
    /// II is the station ID and the rest is the time string of the solar mean time.
    pub(crate) obs_id: Array1<i64>,

    /// The numeric ID of the TCCON station from which each bin was derived.
    pub(crate) station_id: Array1<i8>,

    /// A flag indicating if this bin contains only data that would be public
    /// at the time of creation (1) or contained at least one private spectrum (0).
    pub(crate) public: Array1<i8>,

    /// The TCCON latitudes
    pub(crate) latitude: Array1<Angle>,

    /// The TCCON longitudes
    pub(crate) longitude: Array1<Angle>,

    /// The mean solar zenith angle of the bin, weighted by the uncertainty in
    /// the column averages.
    pub(crate) sza: Array1<Angle>,

    /// The TCCON surface pressures, weighted by the uncertainty in the column
    /// averages.
    pub(crate) p_surf: Array1<Pressure>,

    /// The pressure levels of the priors, with one row per bin.
    pub(crate) p_levels_prior: Array2<Pressure>,

    /// The pressure levels of the AKs. In the future, this will
    /// be removed once the private files unified the AK and prior
    /// pressure levels. Since these do not change, there is a
    /// single vector for all bins.
    pub(crate) p_levels_ak: Array1<Pressure>,

    /// The TCCON H2O priors in wet mole fraction, weighted by the uncertainty
    /// in the column averages.
    pub(crate) prior_h2o: Array2<Ratio>,

    /// The a priori profile for the retrieved gas, dried with the TCCON H2O profile,
    /// and weighted by the column average uncertainties.
    pub(crate) prior_mixing: Array2<Ratio>,

    /// The original TCCON a priori profile for the retrieved gas (in wet mole fraction),
    /// weighted by the column average uncertainties.
    pub(crate) prior_mixing_tccon: Array2<Ratio>,

    /// The averaging kernels, weighted by the column average uncertainties.
    pub(crate) avg_kernel: Array2<f32>,

    /// The bin averaged TCCON column average values, weighted by the uncertainty
    /// in the individual observations.
    pub(crate) column_mixing: Array1<Ratio>,

    /// The propagated uncertainty in the bin averaged TCCON column average values.
    pub(crate) sigma_column_mixing: Array1<Ratio>,

    /// The WMO or analagous calibration scale to which the column mixing values are tied.
    pub(crate) wmo_or_analagous_scale: String,
}

impl TimeAvgData {
    pub(crate) fn new_with_bins(
        solar_mid_time: Array1<NaiveDateTime>,
        nlev: usize,
        wmo_scale: String,
    ) -> Self {
        let nbin = solar_mid_time.len();
        Self {
            solar_mid_time,
            utc_mean_time: Array1::from_elem(nbin, chrono::DateTime::from_timestamp_nanos(0)),
            obs_id: Array1::from_elem(nbin, i64::MIN),
            station_id: Array1::from_elem(nbin, i8::MIN),
            public: Array1::from_elem(nbin, i8::MIN),
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
            p_levels_ak: self.p_levels_ak.select(Axis(0), indices),
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

pub(crate) struct StationMetaAttrs {
    /// The long name of the station, e.g. "caltech" or "nicosia"
    pub(crate) name: String,

    /// The number of days from acquisition for which data from this station
    /// is withheld from the public archive.
    pub(crate) release_lag_days: Option<u32>,

    /// The DOI for the level 2 data from this site, starting with "10."
    pub(crate) data_doi: String,

    /// The revision identifier for the level 2 data from this site, usually
    /// "R0", "R1", etc.
    pub(crate) data_revision: String,

    /// The citation for the level 2 data from this site.
    pub(crate) data_reference: String,
}
