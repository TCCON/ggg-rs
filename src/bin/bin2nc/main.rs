use std::path::{PathBuf, Path};

use clap::Parser;
use ggg_rs::{self, utils::{GggError, self}, runlogs::RunlogDataRec, opus::Spectrum};

#[derive(Debug, Parser)]
struct Cli {
    runlog: PathBuf,
    output: PathBuf
}

fn main() {
    let clargs = Cli::parse();
    let mut writer = IndividualNcWriter::new(&clargs.runlog, clargs.output).unwrap();

    let runlog = ggg_rs::runlogs::Runlog::open(&clargs.runlog).unwrap();
    for data_rec in runlog.into_iter() {
        let spec = ggg_rs::opus::read_spectrum_from_runlog_rec(&data_rec).unwrap();
        writer.add_spectrum(&data_rec, &spec).unwrap();
        println!("Wrote spectrum {} as netCDF", data_rec.spectrum_name);
    }
}

trait NcWriter {
    fn new(runlog: &Path, out_path: PathBuf) -> Result<Box<Self>, GggError>;
    fn add_spectrum(&mut self, data_rec: &RunlogDataRec, spectrum: &Spectrum) -> Result<(), GggError>;
    fn write_0d_var<'f, T: netcdf::Numeric>(nc: &'f mut netcdf::GroupMut, varname: &str, value: T, units: &str, description: &str) 
    -> Result<netcdf::VariableMut<'f>, GggError>;
    fn write_1d_var<'f>(nc: &'f mut netcdf::GroupMut, varname: &str, dims: &[&str], data: &ndarray::Array1<f32>, units: &str, description: &str) 
    -> Result<netcdf::VariableMut<'f>, GggError>;

    fn freq_dim() -> &'static str {
        "frequency"
    }

    fn write_spectrum_values(&mut self, nc: &mut netcdf::GroupMut, data_rec: &RunlogDataRec, spectrum: &Spectrum, out_file: &Path) -> Result<(), GggError> {
        // Create the main variables (frequency and intensity)
        let dimname = Self::freq_dim();

        Self::write_1d_var(
            nc,
            dimname,
            &[dimname],
            &spectrum.freq,
            "cm-1",
            "Frequency in wavenumbers of the measured intensity"
        ).or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_1d_var(
            nc, 
            "intensity",
            &[dimname],
            &spectrum.spec,
            "AU",
            "Measured radiance intensiy in arbitrary units"
        ).or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        // Create the ancillary variables from the runlog that we actually care about
        Self::write_0d_var(nc, "year", data_rec.year, "year", "Year the spectrum was observed")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "day", data_rec.day, "day", "Day-of-year the spectrum was observed")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "hour", data_rec.hour, "utc_hour", "Fractional UT hour when zero path difference occurred")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "latitude", data_rec.obs_lat, "degrees_north", "Latitude where the spectrum was observed")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "longitude", data_rec.obs_lon, "degrees_east", "Longitude where the spectrum was observed")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "altitude", data_rec.obs_alt, "km", "Altitude where the spectrum was observed")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "sza", data_rec.asza, "deg", "Astronomical solar zenith angle during the observation")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "azi", data_rec.azim, "deg", "Azimuth angle of the observation")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "pointing_offset", data_rec.poff, "deg", "The pointing offset in degrees")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "doppler", data_rec.osds, "ppm", "Observer-sun doppler stretch")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "fov_internal", data_rec.fovi, "radians", "Internal field of view")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "fov_external", data_rec.fovo, "radians", "External field of view")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        // TODO: units for amal
        Self::write_0d_var(nc, "angular_misalignment", data_rec.amal, "", "Angular misalignment")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        // TODO: get what the ZLO is a fraction of, just 1?
        Self::write_0d_var(nc, "zlo", data_rec.zoff, "", "Zero level offset as a fraction")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "snr", data_rec.snr, "", "Signal to noise ratio")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "apodization", data_rec.apf.as_int(), "flag", 
            &format!("An integer describing what kind of apodization was applied to the spectrum: {}", utils::ApodizationFxn::int_map_string()))
            .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "instrument_temperature", data_rec.tins, "deg_C", "Temperature inside the instrument")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "instrumnent_pressure", data_rec.pins, "mbar", "Pressure inside the instrument")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "instrument_humidity", data_rec.hins, "%", "Relative humidity inside the instrument")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "outside_temperature", data_rec.tout, "deg_C", "Temperature measured at or near the observation site")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "outside_pressure", data_rec.pout, "mbar", "Pressure measured at or near the observation")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "outside_humidity", data_rec.hout, "%", "Relative humidity measured at or near the observation")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "solar_intensity_average", data_rec.sia, "AU", "Average solar intensity during the observation")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "solar_intensity_frac_var", data_rec.fvsi, "", "Fractional variation in solar intensity during the observation")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "wind_speed", data_rec.wspd, "m s-1", "Wind speed measured at or near the observation")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        // TODO: confirm wind direction convention
        Self::write_0d_var(nc, "wind_dir", data_rec.wdir, "deg", "Wind direction measured at or near the observation")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "airmass_independent_path", data_rec.aipl, "km", "Path length independent of sun position, often the distance between the sun tracker mirror and FTS")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Ok(())
    }
}

struct IndividualNcWriter {
    save_dir: PathBuf
}

impl NcWriter for IndividualNcWriter {
    fn new(_: &Path, out_path: PathBuf) -> Result<Box<Self>, GggError> {
        if !out_path.is_dir() {
            return Err(GggError::CouldNotWrite { path: out_path, reason: "is not a directory".to_owned() });
        }

        Ok(Box::new(Self{ save_dir: out_path }))
    }

    fn add_spectrum(&mut self, data_rec: &RunlogDataRec, spectrum: &Spectrum) -> Result<(), GggError> {
        let out_file = self.save_dir.join(format!("{}.nc", data_rec.spectrum_name));

        let mut nc = netcdf::create(&out_file)
            .or_else(|e| Err(GggError::CouldNotWrite { path: out_file.clone(), reason: format!("{} (while creating netcdf file)", e) }))?;

        let npts = spectrum.freq.len();
        let dimname = "frequency";

        // Create the only needed dimension
        nc.add_dimension(dimname, npts)
            .or_else(|e| Err(GggError::CouldNotWrite { path: out_file.clone(), reason: format!("{} (while creating '{}' dimension)", e, dimname) }))?;

        let mut root = nc.root_mut()
            .ok_or_else(|| GggError::CouldNotWrite { path: out_file.clone(), reason: "Could not get root group as mutable".to_owned()})?;
        self.write_spectrum_values(&mut root, data_rec, spectrum, &out_file)
    }

    fn write_0d_var<'f, T: netcdf::Numeric>(nc: &'f mut netcdf::GroupMut, varname: &str, value: T, units: &str, description: &str) 
    -> Result<netcdf::VariableMut<'f>, GggError> {
        let mut var = nc.add_variable::<T>(varname, &[])
            .or_else(|e| Err(GggError::CouldNotWrite { path: PathBuf::new(), reason: format!("{e} (while creating the {varname} variable)") }))?;

        var.put_value(value, None)
            .or_else(|e| Err(GggError::CouldNotWrite { path: PathBuf::new(), reason: format!("{e} (while writing value to {varname})") }))?;

        var.add_attribute("units", units)
            .or_else(|e| Err(GggError::CouldNotWrite { path: PathBuf::new(), reason: format!("{} (while adding 'units' attribute to {}", e, varname) }))?;

        var.add_attribute("description", description)
            .or_else(|e| Err(GggError::CouldNotWrite { path: PathBuf::new(), reason: format!("{} (while adding 'description' attribute to {}", e, varname) }))?;
        Ok(var)
    }

    fn write_1d_var<'f>(nc: &'f mut netcdf::GroupMut, varname: &str, dims: &[&str], data: &ndarray::Array1<f32>, units: &str, description: &str) 
    -> Result<netcdf::VariableMut<'f>, GggError> {
        let mut var = nc.add_variable::<f32>(varname, dims)
            .or_else(|e| Err(GggError::CouldNotWrite { path: PathBuf::new(), reason: format!("{} (while creating the {} variable)", e, varname) }))?;

        let data_slice = data.as_slice()
            .ok_or_else(|| GggError::CouldNotWrite{path: PathBuf::new(), reason: "Could not convert frequency to a slice".to_owned()})?;

        var.put_values(data_slice, None, None)
            .or_else(|e| Err(GggError::CouldNotWrite { path: PathBuf::new(), reason: format!("{} (while writing values to {})", e, varname) }))?;

        var.add_attribute("units", units)
            .or_else(|e| Err(GggError::CouldNotWrite { path: PathBuf::new(), reason: format!("{} (while adding 'units' attribute to {}", e, varname) }))?;

        var.add_attribute("description", description)
            .or_else(|e| Err(GggError::CouldNotWrite { path: PathBuf::new(), reason: format!("{} (while adding 'description' attribute to {}", e, varname) }))?;

        Ok(var)
    }
}


struct MultipleNcWriter {
    save_dile: PathBuf
}