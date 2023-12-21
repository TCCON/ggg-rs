use chrono::NaiveDate;
use tabled::Tabled;

pub type CatalogueEntryResult<T> = Result<T, CatalogueEntryError>;


#[derive(Debug, thiserror::Error)]
pub enum CatalogueEntryError {
    #[error("Invalid value for field {field}: {reason}")]
    InvalidValue{field: &'static str, reason: String},
    #[error("Field {0} is required for a catalogue entry")]
    MissingRequiredField(&'static str)
}


/// Write out a list of catalogue entries plus the header to a stream
/// 
/// # Inputs
/// - `writer`: any object that implements the [`std::io::Write`] trait, such as [`std::io::Stdout`] or [`std::fs::File`].
///   The catalogue will be written to that stream.
/// - `entries`: the list of catalogue entries to write out
/// - `no_colon:` if `true`, then the header line will be printed without a leading colon. This should be `false` in
///   most cases where writing out a catalogue for use by I2S, as the header must be commented out for I2S.
pub fn write_opus_catalogue_table<W: std::io::Write>(writer: &mut W, entries: &[OpusCatalogueEntry], no_colon: bool) -> std::io::Result<()> {
    // While we can use the Tabled trait to make the table directly, we use a builder to control whether there is a colon
    // at the start of the header line
    let mut builder = tabled::builder::Builder::new();
    if no_colon {
        builder.set_header(OpusCatalogueEntry::headers());
    } else {
        let mut headers = OpusCatalogueEntry::headers();
        headers[0] = format!(": {}", headers[0]).into();
        builder.set_header(headers);
    }

    for entry in entries {
        builder.push_record(entry.fields());
    }

    let mut catalogue = builder.build();
    catalogue.with(tabled::settings::style::Style::blank())
        .with(tabled::settings::Alignment::left());
    
    write!(writer, "{catalogue}")
}


pub struct OpusCatalogueEntry {
    spectrum_name: String,
    year: i32,
    month: u32,
    day: u32,
    run: u32,
    latitude: f32,
    longitude: f32,
    altitude: f32,
    instrument_temperature: f32,
    instrument_pressure: f32,
    instrument_humidity: f32,
    outside_temperature: f32,
    outside_pressure: f32,
    outside_humidity: f32,
    solar_intensity_average: f32,
    fractional_variation_solar_intensity: f32,
    wind_speed: f32,
    wind_direction: f32,
}

impl OpusCatalogueEntry {
    pub fn build(spectrum_name: String) -> OpusCatalogueEntryBuilder {
        let mut builder = OpusCatalogueEntryBuilder::default();
        builder.spectrum_name = spectrum_name;
        builder
    }

    pub fn write_headers<W: std::io::Write>(writer: &mut W, no_colon: bool) -> std::io::Result<()> {
        if !no_colon {
            write!(writer, ":")?
        }

        for header in Self::headers() {
            write!(writer, "  {header}")?;
        }
        writeln!(writer, "")?;

        Ok(())
    }

    pub fn write<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let name = self.quote_spec_name();

        // Make variables for all of these so we can use them directly in the formatting string
        let y = self.year;
        let m = self.month;
        let d = self.day;
        let r = self.run;
        let lat = self.latitude;
        let lon = self.longitude;
        let alt = self.altitude;
        let tins = self.instrument_temperature;
        let pins = self.instrument_pressure;
        let hins = self.instrument_humidity;
        let tout = self.outside_temperature;
        let pout = self.outside_pressure;
        let hout = self.outside_humidity;
        let sia = self.solar_intensity_average;
        let fvsi = self.fractional_variation_solar_intensity;
        let wspd = self.wind_speed;
        let wdir = self.wind_direction;

        writeln!(writer, "{name}  {y}  {m}  {d}  {r}  {lat:.4}  {lon:.4}  {alt:.4}  {tins:.1}  {pins:.1}  {hins:.1}  {tout:.1}  {pout:.1}  {hout:.1}  {sia:.1}  {fvsi:.4}  {wspd:.1}  {wdir:.1}")
    }

    fn quote_spec_name(&self) -> String {
        // If we are by chance given a spectrum that has spaces in it, quote the name to ensure I2S doesn't split on them
        // (note, have not tested this works for I2S as of 2023-09-26)
        if self.spectrum_name.contains(' ') {
            format!("\"{}\"", self.spectrum_name)
        } else {
            self.spectrum_name.clone()
        }
    }
}

impl tabled::Tabled for OpusCatalogueEntry {
    const LENGTH: usize = 18;

    fn fields(&self) -> Vec<std::borrow::Cow<'_, str>> {
        vec![
            self.quote_spec_name().into(),
            format!("{}", self.year).into(),
            format!("{}", self.month).into(),
            format!("{}", self.day).into(),
            format!("{}", self.run).into(),
            format!("{:.4}", self.latitude).into(),
            format!("{:.4}", self.longitude).into(),
            format!("{:.4}", self.altitude).into(),
            format!("{:.1}", self.instrument_temperature).into(),
            format!("{:.1}", self.instrument_pressure).into(),
            format!("{:.1}", self.instrument_humidity).into(),
            format!("{:.1}", self.outside_temperature).into(),
            format!("{:.1}", self.outside_pressure).into(),
            format!("{:.1}", self.outside_humidity).into(),
            format!("{:.1}", self.solar_intensity_average).into(),
            format!("{:.4}", self.fractional_variation_solar_intensity).into(),
            format!("{:.1}", self.wind_speed).into(),
            format!("{:.1}", self.wind_direction).into(),
        ]
    }

    fn headers() -> Vec<std::borrow::Cow<'static, str>> {
        vec![
            "Spectrum_Name".into(),
            "year".into(),
            "mon".into(),
            "day".into(),
            "run".into(),
            "lat".into(),
            "lon".into(),
            "alt".into(),
            "Tins".into(),
            "Pins".into(),
            "Hins".into(),
            "Tout".into(),
            "Pout".into(),
            "Hout".into(),
            "SIA".into(),
            "FVSI".into(),
            "WSPD".into(),
            "WDIR".into(),
        ]
    }
}

#[derive(Debug, Default)]
pub struct OpusCatalogueEntryBuilder {
    spectrum_name: String,
    year: Option<i32>,
    month: Option<u32>,
    day: Option<u32>,
    run: Option<u32>,
    latitude: Option<f32>,
    longitude: Option<f32>,
    altitude: Option<f32>,
    instrument_temperature: Option<f32>,
    instrument_pressure: Option<f32>,
    instrument_humidity: Option<f32>,
    outside_temperature: Option<f32>,
    outside_pressure: Option<f32>,
    outside_humidity: Option<f32>,
    solar_intensity_average: Option<f32>,
    fractional_variation_solar_intensity: Option<f32>,
    wind_speed: Option<f32>,
    wind_direction: Option<f32>,
}

impl OpusCatalogueEntryBuilder {
    pub fn finalize(self, fill_value: f32) -> CatalogueEntryResult<OpusCatalogueEntry> {
        Ok(OpusCatalogueEntry { 
            spectrum_name: self.spectrum_name, 
            year: self.year.ok_or_else(|| CatalogueEntryError::MissingRequiredField("year"))?, 
            month: self.month.ok_or_else(|| CatalogueEntryError::MissingRequiredField("month"))?, 
            day: self.day.ok_or_else(|| CatalogueEntryError::MissingRequiredField("day"))?, 
            run: self.run.ok_or_else(|| CatalogueEntryError::MissingRequiredField("run"))?, 
            latitude: self.latitude.unwrap_or(fill_value), 
            longitude: self.longitude.unwrap_or(fill_value), 
            altitude: self.altitude.unwrap_or(fill_value), 
            instrument_temperature: self.instrument_temperature.unwrap_or(fill_value), 
            instrument_pressure: self.instrument_pressure.unwrap_or(fill_value), 
            instrument_humidity: self.instrument_humidity.unwrap_or(fill_value), 
            outside_temperature: self.outside_temperature.unwrap_or(fill_value), 
            outside_pressure: self.outside_pressure.unwrap_or(fill_value), 
            outside_humidity: self.outside_humidity.unwrap_or(fill_value), 
            solar_intensity_average: self.solar_intensity_average.unwrap_or(fill_value), 
            fractional_variation_solar_intensity: self.fractional_variation_solar_intensity.unwrap_or(fill_value), 
            wind_speed: self.wind_speed.unwrap_or(fill_value), 
            wind_direction: self.wind_direction.unwrap_or(fill_value) 
        })
    }

    pub fn with_time(mut self, year: i32, month: u32, day: u32, run: u32) -> CatalogueEntryResult<Self> {
        if NaiveDate::from_ymd_opt(year, month, day).is_none() {
            return Err(CatalogueEntryError::InvalidValue { field: "year/month/day", reason: format!("{year:04}-{month:02}-{day:02} is not a valid date") })
        }

        self.year = Some(year);
        self.month = Some(month);
        self.day = Some(day);
        self.run = Some(run);

        Ok(self)
    }

    pub fn with_coordinates(mut self, latitude: f32, longitude: f32, altitude_meters: f32) -> CatalogueEntryResult<Self> {
        if latitude < -90.0 || latitude > 90.0 {
            return Err(CatalogueEntryError::InvalidValue { field: "latitude", reason: format!("Latitude must be between -90 and +90, {latitude} is invalid") })
        } else {
            self.latitude = Some(latitude);
        }

        if longitude < -180.0 || longitude > 360.0 {
            return Err(CatalogueEntryError::InvalidValue { field: "longitude", reason: format!("Longitude must be between -180 and +360, {longitude} is invalid") })
        } else if longitude > 180.0 {
            self.longitude = Some(longitude - 360.0);
        } else {
            self.longitude = Some(longitude);
        }

        self.altitude = Some(altitude_meters);

        Ok(self)
    }

    #[allow(non_snake_case)]
    pub fn with_instrument(mut self, instr_temperature_degC: f32, instr_pressure_hPa: f32, instr_humidity_percent: f32) -> Self {
        self.instrument_temperature = Some(instr_temperature_degC);
        self.instrument_pressure = Some(instr_pressure_hPa);
        self.instrument_humidity = Some(instr_humidity_percent);
        self
    }


    #[allow(non_snake_case)]
    pub fn with_outside_met(mut self, temperature_degC: f32, pressure_hPa: f32, humidity_percent: f32) -> Self {
        self.outside_temperature = Some(temperature_degC);
        self.outside_pressure = Some(pressure_hPa);
        self.outside_humidity = Some(humidity_percent);
        self
    }

    pub fn with_solar(mut self, sia: f32, fvsi: f32) -> Self {
        self.solar_intensity_average = Some(sia);
        self.fractional_variation_solar_intensity = Some(fvsi);
        self
    }

    pub fn with_wind(mut self, wind_speed: f32, wind_direction: f32) -> Self {
        self.wind_speed = Some(wind_speed);
        self.wind_direction = Some(wind_direction);
        self
    }
}
