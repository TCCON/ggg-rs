use std::{path::{PathBuf, Path}, collections::HashMap, cell::Cell};

use clap::Parser;
use ggg_rs::{self, utils::{GggError, self}, runlogs::{RunlogDataRec, Runlog}, opus::Spectrum};

/// Generate netCDF versions of binary TCCON spectra listed in a given runlog
///
/// This follows the existing GGG convention in that it will search for the spectra
/// named in the runlog under directories listed in `$GGGPATH/config/data_part.lst`.
/// If a spectrum cannot be found, the program will crash with an error.
#[derive(Debug, Parser)]
struct Cli {
    /// Path to the runlog. This must be a full relative or absolute path, that is,
    /// if you are running this program from `$GGGPATH`, then 
    /// `runlogs/gnd/pa_ggg_benchmark.grl` would work, but not just `pa_ggg_benchmark.grl`.
    runlog: PathBuf,

    /// Directory to which to output the spectra. The spectra will be named automatically.
    /// Existing spectra will be overwritten.
    output: PathBuf,

    /// Set this flag to output a single file containing all spectra, rather than separate files
    /// for each spectrum. Note that this requires all spectra to have the same frequency grid.
    #[clap(short = 's', long = "single-file")]
    single_file: bool,
}

fn main() {
    let clargs = Cli::parse();
    let runlog = ggg_rs::runlogs::Runlog::open(&clargs.runlog).unwrap();
    if clargs.single_file {
        let writer = MultipleNcWriter::new_with_default_map(clargs.output, true).unwrap();
        writer_loop(writer, runlog);
    } else {
        let writer = IndividualNcWriter::new( clargs.output).unwrap();
        writer_loop(writer, runlog);
    }
}

fn writer_loop<W: NcWriter>(mut writer: W, runlog: Runlog) {
    for data_rec in runlog.into_iter() {
        let spec = ggg_rs::opus::read_spectrum_from_runlog_rec(&data_rec).unwrap();
        writer.add_spectrum(&data_rec, &spec).unwrap();
        println!("Wrote spectrum {} as netCDF", data_rec.spectrum_name);
    }
}

trait NcWriter {
    fn add_spectrum(&mut self, data_rec: &RunlogDataRec, spectrum: &Spectrum) -> Result<(), GggError>;
    fn write_0d_var<'f, T: netcdf::Numeric>(nc: &'f mut netcdf::GroupMut, varname: &str, spec_idx: usize, value: T, units: &str, description: &str) 
    -> Result<netcdf::VariableMut<'f>, GggError>;
    fn write_1d_var<'f>(nc: &'f mut netcdf::GroupMut, varname: &str, spec_idx: usize, data: &ndarray::Array1<f32>, units: &str, description: &str) 
    -> Result<netcdf::VariableMut<'f>, GggError>;

    fn freq_dim() -> &'static str {
        "frequency"
    }

    fn write_spectrum_values(nc: &mut netcdf::GroupMut, data_rec: &RunlogDataRec, spectrum: &Spectrum, out_file: &Path, spec_idx: usize, write_freq: bool) -> Result<(), GggError> {
        // Create the main variables (frequency and intensity)
        let dimname = Self::freq_dim();

        if write_freq {
            Self::write_1d_var(
                nc,
                dimname,
                spec_idx,
                &spectrum.freq,
                "cm-1",
                "Frequency in wavenumbers of the measured intensity"
            ).or_else(|e| Err(e.with_path(out_file.to_owned())))?;
        }

        Self::write_1d_var(
            nc, 
            "intensity",
            spec_idx,
            &spectrum.spec,
            "AU",
            "Measured radiance intensiy in arbitrary units"
        ).or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        // Create the ancillary variables from the runlog that we actually care about
        let timestamp = utils::runlog_ydh_to_datetime(data_rec.year, data_rec.day, data_rec.hour).timestamp();
        Self::write_0d_var(nc, "time", spec_idx, timestamp, "seconds since 1970-01-01", "Zero path difference time for this spectrum")
        .map_err(|e| e.with_path(out_file.to_owned()))?;

        Self::write_0d_var(nc, "year", spec_idx, data_rec.year, "year", "Year the spectrum was observed")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "day", spec_idx, data_rec.day, "day", "Day-of-year the spectrum was observed")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "hour", spec_idx, data_rec.hour, "utc_hour", "Fractional UT hour when zero path difference occurred")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "latitude", spec_idx, data_rec.obs_lat, "degrees_north", "Latitude where the spectrum was observed")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "longitude", spec_idx, data_rec.obs_lon, "degrees_east", "Longitude where the spectrum was observed")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "altitude", spec_idx, data_rec.obs_alt, "km", "Altitude where the spectrum was observed")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "sza", spec_idx, data_rec.asza, "deg", "Astronomical solar zenith angle during the observation")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "azi", spec_idx, data_rec.azim, "deg", "Azimuth angle of the observation")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "pointing_offset", spec_idx, data_rec.poff, "deg", "The pointing offset in degrees")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "doppler", spec_idx, data_rec.osds, "ppm", "Observer-sun doppler stretch")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "fov_internal", spec_idx, data_rec.fovi, "radians", "Internal field of view")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "fov_external", spec_idx, data_rec.fovo, "radians", "External field of view")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        // TODO: units for amal
        Self::write_0d_var(nc, "angular_misalignment", spec_idx, data_rec.amal, "", "Angular misalignment")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        // TODO: get what the ZLO is a fraction of, just 1?
        Self::write_0d_var(nc, "zlo", spec_idx, data_rec.zoff, "", "Zero level offset as a fraction")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "snr", spec_idx, data_rec.snr, "", "Signal to noise ratio")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "apodization", spec_idx, data_rec.apf.as_int(), "flag", 
            &format!("An integer describing what kind of apodization was applied to the spectrum: {}", utils::ApodizationFxn::int_map_string()))
            .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "instrument_temperature", spec_idx, data_rec.tins, "deg_C", "Temperature inside the instrument")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "instrumnent_pressure", spec_idx, data_rec.pins, "mbar", "Pressure inside the instrument")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "instrument_humidity", spec_idx, data_rec.hins, "%", "Relative humidity inside the instrument")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "outside_temperature", spec_idx, data_rec.tout, "deg_C", "Temperature measured at or near the observation site")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "outside_pressure", spec_idx, data_rec.pout, "mbar", "Pressure measured at or near the observation")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "outside_humidity", spec_idx, data_rec.hout, "%", "Relative humidity measured at or near the observation")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "solar_intensity_average", spec_idx, data_rec.sia, "AU", "Average solar intensity during the observation")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "solar_intensity_frac_var", spec_idx, data_rec.fvsi, "", "Fractional variation in solar intensity during the observation")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "wind_speed", spec_idx, data_rec.wspd, "m s-1", "Wind speed measured at or near the observation")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        // TODO: confirm wind direction convention
        Self::write_0d_var(nc, "wind_dir", spec_idx, data_rec.wdir, "deg", "Wind direction measured at or near the observation")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Self::write_0d_var(nc, "airmass_independent_path", spec_idx, data_rec.aipl, "km", "Path length independent of sun position, often the distance between the sun tracker mirror and FTS")
        .or_else(|e| Err(e.with_path(out_file.to_owned())))?;

        Ok(())
    }
}

struct IndividualNcWriter {
    save_dir: PathBuf
}

impl IndividualNcWriter {
    fn new(out_path: PathBuf) -> Result<Self, GggError> {
        if !out_path.is_dir() {
            return Err(GggError::CouldNotWrite { path: out_path, reason: "is not a directory".to_owned() });
        }

        Ok(Self{ save_dir: out_path })
    }
}

impl NcWriter for IndividualNcWriter {
    fn add_spectrum(&mut self, data_rec: &RunlogDataRec, spectrum: &Spectrum) -> Result<(), GggError> {
        let out_file = self.save_dir.join(format!("{}.nc", data_rec.spectrum_name));

        let mut nc = netcdf::create(&out_file)
            .or_else(|e| Err(GggError::CouldNotWrite { path: out_file.clone(), reason: format!("{} (while creating netcdf file)", e) }))?;

        let npts = spectrum.freq.len();
        let dimname = Self::freq_dim();

        // Create the only needed dimension
        nc.add_dimension(dimname, npts)
            .or_else(|e| Err(GggError::CouldNotWrite { path: out_file.clone(), reason: format!("{} (while creating '{}' dimension)", e, dimname) }))?;

        let mut root = nc.root_mut()
            .ok_or_else(|| GggError::CouldNotWrite { path: out_file.clone(), reason: "Could not get root group as mutable".to_owned()})?;
        Self::write_spectrum_values(&mut root, data_rec, spectrum, &out_file, 0, true)
    }

    fn write_0d_var<'f, T: netcdf::Numeric>(nc: &'f mut netcdf::GroupMut, varname: &str, _spec_idx: usize, value: T, units: &str, description: &str) 
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

    fn write_1d_var<'f>(nc: &'f mut netcdf::GroupMut, varname: &str, _spec_idx: usize, data: &ndarray::Array1<f32>, units: &str, description: &str) 
    -> Result<netcdf::VariableMut<'f>, GggError> {
        let mut var = nc.add_variable::<f32>(varname, &[Self::freq_dim()])
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


struct SpecGroupDef {
    detector_code: char,
    ifirst: usize,
    ilast: usize,
    delta_nu: f64,
    group_name: String,
    curr_idx: Cell<usize>
}

impl SpecGroupDef {
    fn new(runlog_entry: &RunlogDataRec, detector_mapping: &HashMap<char, String>) -> Result<Self, GggError> {
        let rl_det_code = Self::get_spectrum_det_code(&runlog_entry.spectrum_name)?;
        let group_name = detector_mapping
            .get(&rl_det_code)
            .and_then(|s| Some(s.to_owned()))
            .unwrap_or_else(|| rl_det_code.to_string());

        Ok(Self { detector_code: rl_det_code, ifirst: runlog_entry.ifirst, ilast: runlog_entry.ilast, delta_nu: runlog_entry.delta_nu, group_name, curr_idx: Cell::new(0) })
    }

    fn get_spectrum_det_code(spectrum_name: &str) -> Result<char, GggError> {
        let rl_det_code = if let Some((stem, _)) = spectrum_name.split_once('.') {
            stem.chars().last()
                .ok_or_else(|| GggError::DataError { path: PathBuf::new(), cause: format!("Could not get last character of spectrum stem in {}", spectrum_name) })?
        }else{
            return Err(GggError::DataError { path: PathBuf::new(), cause: format!("Could not find '.' to split on in spectrum name: {}", spectrum_name )});
        };

        Ok(rl_det_code)
    }

    fn entry_matches_group(&self, runlog_entry: &RunlogDataRec) -> Result<bool, GggError> {
        let rl_det_code = Self::get_spectrum_det_code(&runlog_entry.spectrum_name)?;
        
        if rl_det_code == self.detector_code && runlog_entry.ifirst == self.ifirst && runlog_entry.ilast == self.ilast && runlog_entry.delta_nu == self.delta_nu {
            Ok(true)
        }else if rl_det_code == self.detector_code {
            // Inconsistent detector code and frequency window
            Err(GggError::DataError { path: PathBuf::new(), cause: format!("Spectrum {} has a different frequency window than other spectra with the same detector code", runlog_entry.spectrum_name) })
        }else{
            Ok(false)
        }
    }

    fn get_next_index(&self) -> usize {
        let next_idx = self.curr_idx.get();
        self.curr_idx.set(next_idx + 1);
        next_idx
    }
}

struct MultipleNcWriter {
    save_file: PathBuf,
    detector_mapping: HashMap<char, String>,
    group_defs: Vec<SpecGroupDef>,
    nc_file: netcdf::MutableFile
}

impl MultipleNcWriter {
    fn new(detector_mapping: HashMap<char, String>, output_file: PathBuf, clobber: bool) -> Result<Self, GggError> {
        if output_file.is_dir() {
            return Err(GggError::CouldNotWrite { path: output_file, reason: "Expected a file, got a path to a directory".to_owned() });
        }

        if output_file.exists() && !clobber {
            return Err(GggError::CouldNotWrite { path: output_file, reason: "File already exists".to_owned() });
        }

        let nc_file = netcdf::create(&output_file)
            .map_err(|e| GggError::CouldNotWrite { 
                path: output_file.clone(), 
                reason: format!("Could not create netCDF file: {e}")
            })?;

        Ok(Self { save_file: output_file, detector_mapping, group_defs: Vec::new(), nc_file })
    }

    fn new_with_default_map(output_file: PathBuf, clobber: bool) -> Result<Self, GggError> {
        let mapping = Self::default_mapping();
        Self::new(mapping, output_file, clobber)
    }

    fn new_with_map_overrides(map_overrides: HashMap<char, String>, output_file: PathBuf, clobber: bool) -> Result<Self, GggError> {
        let mut mapping = Self::default_mapping();
        for (k, v) in map_overrides.into_iter() {
            mapping.insert(k, v);
        }
        Self::new(mapping, output_file, clobber)
    }

    fn default_mapping() -> HashMap<char, String> {
        HashMap::from_iter([
            ('a', "InGaAs".to_owned()),
            ('b', "Si".to_owned()),
            ('c', "InSb".to_owned()),
        ])
    }

    fn spec_dim() -> &'static str {
        "spectrum"
    }

    fn find_spectrum_group(&mut self, runlog_entry: &RunlogDataRec) -> Result<&SpecGroupDef, GggError> {
        let mut idx = None;
        for (i, grp_def) in self.group_defs.iter().enumerate() {
            if grp_def.entry_matches_group(runlog_entry)? {
                idx = Some(i);
                break;
            }
        }

        // Have to find the index first and return this way because otherwise Rust thinks we have a mutable and
        // immutable borrow happening simultaneously
        if let Some(i) = idx {
            Ok(&self.group_defs[i])
        }else{
            let new_group = SpecGroupDef::new(runlog_entry, &self.detector_mapping)?;
            self.create_group(&new_group)?;
            self.group_defs.push(new_group);
            Ok(self.group_defs.last().unwrap())
        }
    }

    fn create_group(&mut self, group_def: &SpecGroupDef) -> Result<(), GggError> {
        let nc_path = self.nc_file.path().unwrap_or_else(|_| PathBuf::from("?"));
        // This creates the new spectrum group, with an unlimited dimension for time so that we can append new spectra.
        self.nc_file.add_group(&group_def.group_name)
            .map_err(|e| GggError::CouldNotWrite { 
                path: nc_path.clone(), 
                reason: format!("Could not create netCDF group {}: {}", group_def.group_name, e) 
            })?;

        Ok(())
    }

    fn init_group(nc_path: &Path, grp: &mut netcdf::GroupMut, group_name: &str, data_rec: &RunlogDataRec, spectrum: &Spectrum) -> Result<(), GggError> {
        grp.add_dimension(Self::spec_dim(), 0)
        .map_err(|e| GggError::CouldNotWrite { 
            path: nc_path.to_owned(), 
            reason: format!("Could not create dimension 'spectrum' (unlimited): {e}")
        })?;

        grp.add_dimension(Self::freq_dim(), spectrum.freq.len())
        .map_err(|e| GggError::CouldNotWrite {
            path: nc_path.to_owned(),
            reason: format!("Could not add frequency dimension to '{group_name}' group: {e}") 
        })?;

        let mut freq_var = grp.add_variable::<f32>(Self::freq_dim(), &[Self::freq_dim()])
        .map_err(|e| GggError::CouldNotWrite { 
            path: nc_path.to_owned(), 
            reason: format!("Could not create frequency variable in group '{group_name}': {e}") 
        })?;

        let freq_values = spectrum.freq.as_slice()
        .ok_or_else(|| GggError::CouldNotWrite { 
            path: nc_path.to_owned(),
            reason: format!("Could not convert frequency values from spectrum '{}' to a slice", data_rec.spectrum_name)
        })?;

        freq_var.put_values(freq_values, None, None)
        .map_err(|e| GggError::CouldNotWrite { 
            path: nc_path.to_owned(), 
            reason: format!("Could not write frequency values to group '{group_name}': {e}") 
        })?;

        freq_var.add_attribute("units", "cm-1")
        .map_err(|e| GggError::CouldNotWrite { 
            path: nc_path.to_owned(), 
            reason: format!("Could not add 'units' attribute to 'frequency' variable in group '{group_name}': {e}") 
        })?;

        freq_var.add_attribute("description", "Frequency in wavenumbers of the measured intensity")
        .map_err(|e| GggError::CouldNotWrite { 
            path: nc_path.to_owned(), 
            reason: format!("Could not add 'description' attribute to 'frequency' variable in group '{group_name}': {e}") 
        })?;

        Ok(())
    }

    fn write_str_var<'f>(nc: &'f mut netcdf::GroupMut, varname: &str, spec_idx: usize, value: &str, description: &str) -> Result<netcdf::VariableMut<'f>, GggError> {
        let group_name = nc.name();

        let mut var = if nc.variable(varname).is_some() {
            nc.variable_mut(varname).unwrap()
        } else {
            let mut v = nc.add_string_variable(varname, &[Self::spec_dim()])
            .map_err(|e| GggError::CouldNotWrite { 
                path: PathBuf::from("?"), 
                reason: format!("Could not create string variable '{varname}' in group '{group_name}': {e}")
            })?;

            v.add_attribute("description", description)
            .map_err(|e| GggError::CouldNotWrite { 
                path: PathBuf::from("?"), 
                reason: format!("Could not add 'units' attribute to string variable '{varname}' in group '{group_name}': {e}")
            })?;

            v
        };

        var.put_string(value, Some(&[spec_idx]))
        .map_err(|e| GggError::CouldNotWrite { 
            path: PathBuf::from("?"), 
            reason: format!("Could not write string value to variable '{varname}' in group '{group_name}' at index {spec_idx}: {e}")
        })?;

        Ok(var)
    }
}

impl NcWriter for MultipleNcWriter {
    fn add_spectrum(&mut self, data_rec: &RunlogDataRec, spectrum: &Spectrum) -> Result<(), GggError> {
        // For each entry, check if the spectrum can go in one of the existing groups. If we need to create a group, do so.
        // If there's an issue (i.e. the spectrum should go in a certain group based on its detector code but has a different
        // frequency grid) either crash or skip that spectrum.
        let nc_path = self.nc_file.path().unwrap_or_else(|_| PathBuf::from("?"));

        let (group_name, next_idx) = {
            let grp_def = self.find_spectrum_group(data_rec)?;
            let spec_idx = grp_def.get_next_index();
            (grp_def.group_name.to_owned(), spec_idx)
        };

        let mut grp = self.nc_file.group_mut(&group_name)
            .map_err(|e| GggError::NotImplemented(
                format!("Could not get netCDF group '{}' (this should not happen), error was: {e}", &group_name)
            ))?
            .ok_or_else(|| GggError::NotImplemented(
                format!("Could not get netCDF group '{}' (this should not happen)", &group_name)
            ))?;

        // Initialize group dimensions here because we need the spectrum for the frequency
        if grp.dimension(Self::freq_dim()).is_none() {
            Self::init_group(&nc_path, &mut grp, &group_name, data_rec, spectrum)?;
        }

        Self::write_str_var(&mut grp, "spectrum", next_idx, &data_rec.spectrum_name, "Spectrum name")?;
        Self::write_spectrum_values(&mut grp, data_rec, spectrum, &self.save_file, next_idx, false)
    }

    fn write_0d_var<'f, T: netcdf::Numeric>(nc: &'f mut netcdf::GroupMut, varname: &str, spec_idx: usize, value: T, units: &str, description: &str) 
    -> Result<netcdf::VariableMut<'f>, GggError> {
        let group_name = nc.name();

        let mut var = if nc.variable(varname).is_some() {
            // Couldn't do an if let Some(v) = nc.variable_mut(varname) because that made the
            // nc mutable borrow in the if let clause conflict with the mutable borrow in the
            // else block
            nc.variable_mut(varname).unwrap()
        }else{
            let mut v = nc.add_variable::<T>(varname, &[Self::spec_dim()])
            .map_err(|e| GggError::CouldNotWrite { 
                path: PathBuf::from("?"), 
                reason: format!("Could not create variable '{varname}' in group '{group_name}': {e}") 
            })?;

            v.add_attribute("units", units)
            .map_err(|e| GggError::CouldNotWrite { 
                path: PathBuf::from("?"), 
                reason: format!("Could not add 'units' attribute to variable '{varname}' in group '{group_name}': {e}")
            })?;

            v.add_attribute("description", description)
            .map_err(|e| GggError::CouldNotWrite { 
                path: PathBuf::from("?"), 
                reason: format!("Could not add 'units' attribute to variable '{varname}' in group '{group_name}': {e}")
            })?;

            v
        };

        var.put_value(value, Some(&[spec_idx]))
        .map_err(|e| GggError::CouldNotWrite { 
            path: PathBuf::from("?"), 
            reason: format!("Could not write scalar value to variable '{varname}' in group '{group_name}' at index {spec_idx}: {e}")
        })?;

        Ok(var)
    }

    fn write_1d_var<'f>(nc: &'f mut netcdf::GroupMut, varname: &str, spec_idx: usize, data: &ndarray::Array1<f32>, units: &str, description: &str) 
    -> Result<netcdf::VariableMut<'f>, GggError> {
        let group_name = nc.name();

        let mut var = if nc.variable(varname).is_some() {
            nc.variable_mut(varname).unwrap()
        }else{
            let mut v = nc.add_variable::<f32>(varname, &[Self::spec_dim(), Self::freq_dim()])
            .map_err(|e| GggError::CouldNotWrite { 
                path: PathBuf::from("?"), 
                reason: format!("Could not create variable '{varname}' in group '{group_name}': {e}")
            })?;

            v.add_attribute("units", units)
            .map_err(|e| GggError::CouldNotWrite { 
                path: PathBuf::from("?"), 
                reason: format!("Could not add 'units' attribute to variable '{varname}' in group '{group_name}': {e}")
            })?;

            v.add_attribute("description", description)
            .map_err(|e| GggError::CouldNotWrite { 
                path: PathBuf::from("?"), 
                reason: format!("Could not add 'units' attribute to variable '{varname}' in group '{group_name}': {e}")
            })?;

            v
        };

        let values = data.as_slice()
        .ok_or_else(|| GggError::CouldNotWrite { 
            path: PathBuf::from("?"), 
            reason: format!("Could not convert data for variable '{varname}' at spectrum index {spec_idx} in group '{group_name}' to a slice")
        })?;

        var.put_values(values, Some(&[spec_idx, 0]), None)
        .map_err(|e| GggError::CouldNotWrite { 
            path: PathBuf::from("?"),
            reason: format!("Could not write values for variable '{varname}' at spectrum index {spec_idx} in group '{group_name}': {e}")
        })?;

        Ok(var)
    }
}