use chrono::{DateTime, NaiveDateTime, Utc};
use ndarray::{Array1, Array2};
use uom::si::f32::{Angle, Pressure, Ratio};

/// A structure representing time-averaged data from one or more TCCON sites.
pub(crate) struct TimeAvgData {
    /// The mean UTC time of the bin, weighted by the uncertainty in the column
    /// averages.
    pub(crate) utc_mean_time: Array1<DateTime<Utc>>,

    /// The mean solar time of each bin. TODO: confirm that this should be
    /// a naive date time, i.e., it is relative to solar noon for each site
    pub(crate) solar_mean_time: Array1<NaiveDateTime>,

    /// A unique identifier for each bin in the format YYYYmmddHHMMSSII, where
    /// II is the station ID and the rest is the time string of the
    pub(crate) obs_id: Array1<String>,

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

    /// The TCCON pressure levels for the AKs and priors, weighted by the
    /// uncertainty in the column averages.
    pub(crate) p_levels: Array2<Pressure>,

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

    /// The latitude of each observation
    pub(crate) latitude: Array1<Angle>,

    /// The longitude of each observation
    pub(crate) longitude: Array1<Angle>,

    /// The solar zenith angle of each observation
    pub(crate) sza: Array1<Angle>,

    /// The surface pressure of each observation.
    pub(crate) p_surf: Array1<Pressure>,

    /// The pressure levels of the priors and AK. These must
    /// be expanded to one row per observation (not on prior times),
    /// and must be unified between the priors and AKs.
    pub(crate) p_levels: Array2<Pressure>,

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
