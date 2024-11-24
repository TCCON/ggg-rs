use std::rc::Rc;

pub(crate) struct AttributeSet {
    description: Rc<str>,
    units: Rc<str>,
    standard_name: Rc<str>,
    long_name: Rc<str>,
}

impl AttributeSet {
    pub(crate) fn new(description: &str, units: &str, std_name: &str) -> Self {
        let long_name = std_name.replace("_", " ");
        Self { description: Rc::from(description), units: Rc::from(units), standard_name: Rc::from(std_name), long_name: Rc::from(long_name.as_str()) }
    }

    pub(crate) fn new_long_name(description: &str, units: &str, std_name: &str, long_name: &str) -> Self {
        Self { description: Rc::from(description), units: Rc::from(units), standard_name: Rc::from(std_name), long_name: Rc::from(long_name) }
    }

    pub(crate) fn write_attrs(&self, var: &mut netcdf::VariableMut) -> netcdf::Result<()> {
        var.put_attribute("description", self.description.as_ref())?;
        var.put_attribute("units", self.units.as_ref())?;
        var.put_attribute("standard_name", self.standard_name.as_ref())?;
        var.put_attribute("long_name", self.long_name.as_ref())?;
        Ok(())
    }

    pub(crate) fn write_attrs_with_minmax<T: Into<netcdf::AttributeValue>>(&self, var: &mut netcdf::VariableMut, vmin: T, vmax: T) -> netcdf::Result<()> {
        self.write_attrs(var)?;
        var.put_attribute("vmin", vmin)?;
        var.put_attribute("vmax", vmax)?;
        Ok(())
    }
}

// ------------------------------------- //
// Define specific variables' attributes //
// ------------------------------------- //

#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, strum::Display)]
#[strum(serialize_all = "lowercase")]
pub(crate) enum FixedVar {
    Year,
    Day,
    Hour,
    Run,
    Lat,
    Lon,
    Zobs,
    Zmin,
    Solzen,
    Azim,
    Osds,
    Opd,
    Fovi,
    Amal,
    Graw,
    Tins,
    Pins,
    Tout,
    Pout,
    Hout,
    Sia,
    Fvsi,
    Wspd,
    Wdir,
}

impl FixedVar {
    pub(crate) fn get_attributes(&self) -> AttributeSet {
        match self {
            FixedVar::Year => year_attrs(),
            FixedVar::Day => day_attrs(),
            FixedVar::Hour => hour_attrs(),
            FixedVar::Run => run_attrs(),
            FixedVar::Lat => lat_attrs(),
            FixedVar::Lon => lon_attrs(),
            FixedVar::Zobs => zobs_attrs(),
            FixedVar::Zmin => zmin_attrs(),
            FixedVar::Solzen => solzen_attrs(),
            FixedVar::Azim => azim_attrs(),
            FixedVar::Osds => osds_attrs(),
            FixedVar::Opd => opd_attrs(),
            FixedVar::Fovi => fovi_attrs(),
            FixedVar::Amal => amal_attrs(),
            FixedVar::Graw => graw_attrs(),
            FixedVar::Tins => tins_attrs(),
            FixedVar::Pins => pins_attrs(),
            FixedVar::Tout => tout_attrs(),
            FixedVar::Pout => pout_attrs(),
            FixedVar::Hout => hout_attrs(),
            FixedVar::Sia => sia_attrs(),
            FixedVar::Fvsi => fvsi_attrs(),
            FixedVar::Wspd => wspd_attrs(),
            FixedVar::Wdir => wdir_attrs(),
        }
    }

    pub(crate) fn write_attrs(&self, var: &mut netcdf::VariableMut) -> netcdf::Result<()> {
        let attrs = self.get_attributes();
        attrs.write_attrs(var)
    }
}

// Runlog/aux variables //

pub(crate) fn year_attrs() -> AttributeSet {
    AttributeSet::new("year, e.g. 2009", "years", "year")
}

pub(crate) fn day_attrs() -> AttributeSet {
    AttributeSet::new("day of the year, e.g. 1 to 366", "days", "day_of_year")
}

pub(crate) fn hour_attrs() -> AttributeSet {
    AttributeSet::new("fractional UT hours (zero path difference crossing time", "hours", "decimal_hour")
}

pub(crate) fn run_attrs() -> AttributeSet {
    AttributeSet::new("run index", "", "run_number")
}

pub(crate) fn lat_attrs() -> AttributeSet {
    AttributeSet::new("latitude of the observation in degrees, south is negative", "degrees_north", "latitude")
}

pub(crate) fn lon_attrs() -> AttributeSet {
    AttributeSet::new("longitude of the observation in degrees, west is negative", "degrees_east", "longitude")
}

pub(crate) fn zobs_attrs() -> AttributeSet {
    AttributeSet::new("geometric altitude above sea level", "km", "observation_altitude")
}

pub(crate) fn zmin_attrs() -> AttributeSet {
    AttributeSet::new("altitude derived from surface pressure measurements", "km", "pressure_altitude")
}

pub(crate) fn solzen_attrs() -> AttributeSet {
    AttributeSet::new("solar zenith angle of the observation", "degrees", "solar_zenith_angle")
}

pub(crate) fn azim_attrs() -> AttributeSet {
    AttributeSet::new("solar azimuth angle of the observation", "degrees", "solar_azimuth_angle")
}

pub(crate) fn osds_attrs() -> AttributeSet {
    AttributeSet::new("observer-sun Doppler stretch of the observation", "ppm", "observer_sun_dopple_stretch")
}

pub(crate) fn opd_attrs() -> AttributeSet {
    AttributeSet::new("maximum optical path difference for the interferometer during the observation", "cm", "maximum_optical_path_difference")
}

pub(crate) fn fovi_attrs() -> AttributeSet {
    AttributeSet::new("internal field of view", "radians", "internal_field_of_view")
}

pub(crate) fn amal_attrs() -> AttributeSet {
    AttributeSet::new("angular mis-alignment", "radians", "angular_misalignment")
}

pub(crate) fn graw_attrs() -> AttributeSet {
    AttributeSet::new("spectral point spacing", "cm-1", "spectral_point_spacing")
}

pub(crate) fn tins_attrs() -> AttributeSet {
    AttributeSet::new("internal temperature of the interferometer", "degrees_Celsius", "instrument_internal_temperature")
}

pub(crate) fn pins_attrs() -> AttributeSet {
    AttributeSet::new("internal pressure of the interferometer", "hPa", "instrument_internal_pressure")
}

pub(crate) fn tout_attrs() -> AttributeSet {
    AttributeSet::new("external temperature at the measurement site", "degrees_Celsius", "atmospheric_temperature")
}

pub(crate) fn pout_attrs() -> AttributeSet {
    AttributeSet::new("external pressure at the measurement site", "hPa", "atmospheric_pressure")
}

pub(crate) fn hout_attrs() -> AttributeSet {
    AttributeSet::new("external relative humidity at the measurement site", "%", "atmospheric_humidity")
}

pub(crate) fn sia_attrs() -> AttributeSet {
    AttributeSet::new("average solar intensity during the measurement", "AU", "solar_intensity_average")
}

pub(crate) fn fvsi_attrs() -> AttributeSet {
    AttributeSet::new("variation in solar intensity relative to the average during the measurement", "%", "fractional_variation_in_solar_intensity")
}

pub(crate) fn wspd_attrs() -> AttributeSet {
    AttributeSet::new("wind speed during the measurement", "m.s-1", "wind_speed")
}

pub(crate) fn wdir_attrs() -> AttributeSet {
    AttributeSet::new("wind direction during the measurement", "degrees", "wind_direction")
}