use std::{path::{PathBuf, Path}, collections::HashMap, cell::Cell};

use clap::Parser;
use ggg_rs::{self, utils::{GggError, self}, runlogs::{RunlogDataRec, Runlog}, opus::Spectrum};
use netcdf::extent::Extents;

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

    /// Set this flag to output the full path to each spectrum, rather than its name, as the
    /// "spectrum" variable in a multiple-spectrum file. In a single-spectrum file, this will
    /// be added as a root-level attribute. 
    #[clap(short = 'f', long)]
    full_spec_paths: bool,

    #[clap(flatten)]
    data_part_args: utils::DataPartArgs,
}



fn main() {
    let clargs = Cli::parse();
    let data_part = clargs.data_part_args.get_data_partition()
        .expect("Unable to set up data partition for spectrum paths");
    let runlog = ggg_rs::runlogs::Runlog::open(&clargs.runlog).unwrap();
    if clargs.single_file {
        let runlog_clone = ggg_rs::runlogs::Runlog::open(&clargs.runlog).unwrap();
        let writer = MultipleNcWriter::new_with_default_map(&data_part, clargs.output, runlog_clone, true).unwrap();
        writer_loop(writer, runlog, &data_part, clargs.full_spec_paths);
    } else {
        let writer = IndividualNcWriter::new( clargs.output).unwrap();
        writer_loop(writer, runlog, &data_part, clargs.full_spec_paths);
    }
}

fn writer_loop<W: NcWriter>(mut writer: W, runlog: Runlog, data_part: &utils::DataPartition, full_spec_paths: bool) {
    for data_rec in runlog.into_iter() {
        let spec = ggg_rs::opus::read_spectrum_from_runlog_rec(&data_rec, data_part).unwrap();
        writer.add_spectrum(&data_rec, &spec, full_spec_paths).unwrap();
        println!("Wrote spectrum {} as netCDF", data_rec.spectrum_name);
    }
}

trait NcWriter {
    fn add_spectrum(&mut self, data_rec: &RunlogDataRec, spectrum: &Spectrum, full_spec_paths: bool) -> Result<(), GggError>;
    fn write_0d_var<'f, T: netcdf::NcPutGet>(nc: &'f mut netcdf::GroupMut, varname: &str, spec_idx: usize, value: T, units: &str, description: &str) 
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
            "Measured radiance intensity in arbitrary units"
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
    fn add_spectrum(&mut self, data_rec: &RunlogDataRec, spectrum: &Spectrum, full_spec_paths: bool) -> Result<(), GggError> {
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
        if full_spec_paths {
            let spec_path = format!("{}", spectrum.path.display());
            root.add_attribute("full_spectrum_path", spec_path.as_str())
                .map_err(|_| GggError::CouldNotWrite { path: out_file.clone(), reason: "Could not add 'full_spectrum_path' attribute to root group".to_string() })?;
        }
        Self::write_spectrum_values(&mut root, data_rec, spectrum, &out_file, 0, true)
    }

    fn write_0d_var<'f, T: netcdf::NcPutGet>(nc: &'f mut netcdf::GroupMut, varname: &str, _spec_idx: usize, value: T, units: &str, description: &str) 
    -> Result<netcdf::VariableMut<'f>, GggError> {
        let mut var = nc.add_variable::<T>(varname, &[])
            .or_else(|e| Err(GggError::CouldNotWrite { path: PathBuf::new(), reason: format!("{e} (while creating the {varname} variable)") }))?;

        var.put_value(value, Extents::All)
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

        var.put_values(data_slice, Extents::All)
            .or_else(|e| Err(GggError::CouldNotWrite { path: PathBuf::new(), reason: format!("{} (while writing values to {})", e, varname) }))?;

        var.add_attribute("units", units)
            .or_else(|e| Err(GggError::CouldNotWrite { path: PathBuf::new(), reason: format!("{} (while adding 'units' attribute to {}", e, varname) }))?;

        var.add_attribute("description", description)
            .or_else(|e| Err(GggError::CouldNotWrite { path: PathBuf::new(), reason: format!("{} (while adding 'description' attribute to {}", e, varname) }))?;

        Ok(var)
    }
}


#[derive(Debug)]
struct SpecGroupDef {
    detector_code: char,
    max_spec_length: usize,
    group_name: String,
    curr_idx: Cell<usize>
}

impl SpecGroupDef {
    fn new(runlog_entry: &RunlogDataRec, data_part: &utils::DataPartition, detector_mapping: &HashMap<char, String>) -> Result<Self, GggError> {
        let rl_det_code = Self::get_spectrum_det_code(&runlog_entry.spectrum_name)?;
        let group_name = detector_mapping
            .get(&rl_det_code)
            .and_then(|s| Some(s.to_owned()))
            .unwrap_or_else(|| rl_det_code.to_string());
        let spec_length: usize = ggg_rs::opus::get_spectrum_num_points(&runlog_entry.spectrum_name, data_part, runlog_entry.pointer, runlog_entry.bpw)
            .map_err(|e| GggError::CouldNotOpen { 
                descr: "binary spectrum".to_owned(), 
                path: PathBuf::from(&runlog_entry.spectrum_name), 
                reason: e.to_string()
            })?.try_into().expect("Cannot fit spectrum length into system usize");

        Ok(Self { detector_code: rl_det_code, group_name, max_spec_length: spec_length, curr_idx: Cell::new(0) })
    }

    fn get_spectrum_det_code(spectrum_name: &str) -> Result<char, GggError> {
        // Must use the character position rather than splitting on the period - some Karlrsuhe spectra have an extra
        // character before the detector for example.
        if let Some(rl_det_code) = spectrum_name.chars().nth(15) {
            Ok(rl_det_code)
        }else{
            Err(GggError::DataError { path: PathBuf::new(), cause: format!("Could not get 16th character in spectrum name: {}", spectrum_name )})
        }
    }

    fn entry_matches_group(&self, runlog_entry: &RunlogDataRec) -> Result<bool, GggError> {
        let rl_det_code = Self::get_spectrum_det_code(&runlog_entry.spectrum_name)?;
        Ok(rl_det_code == self.detector_code)
    }

    fn get_next_index(&self) -> usize {
        let next_idx = self.curr_idx.get();
        self.curr_idx.set(next_idx + 1);
        next_idx
    }
}

struct MultipleNcWriter {
    save_file: PathBuf,
    group_defs: Vec<SpecGroupDef>,
    nc_file: netcdf::MutableFile
}

impl MultipleNcWriter {
    fn new(data_part: &utils::DataPartition, detector_mapping: HashMap<char, String>, output_file: PathBuf, runlog: Runlog, clobber: bool) -> Result<Self, GggError> {
        if output_file.is_dir() {
            return Err(GggError::CouldNotWrite { path: output_file, reason: "Expected a file, got a path to a directory".to_owned() });
        }

        if output_file.exists() && !clobber {
            return Err(GggError::CouldNotWrite { path: output_file, reason: "File already exists".to_owned() });
        }

        let mut nc_file = netcdf::create(&output_file)
            .map_err(|e| GggError::CouldNotWrite { 
                path: output_file.clone(), 
                reason: format!("Could not create netCDF file: {e}")
            })?;

        let group_defs = Self::make_group_defs(runlog, data_part, &detector_mapping, &mut nc_file)?;

        Ok(Self { save_file: output_file, group_defs, nc_file })
    }

    fn new_with_default_map(data_part: &utils::DataPartition, output_file: PathBuf, runlog: Runlog, clobber: bool) -> Result<Self, GggError> {
        let mapping = Self::default_mapping();
        Self::new(data_part, mapping, output_file, runlog, clobber)
    }

    // Don't need this right now, but may in the future.
    #[allow(dead_code)]
    fn new_with_map_overrides(data_part: &utils::DataPartition, map_overrides: HashMap<char, String>, output_file: PathBuf, runlog: Runlog, clobber: bool) -> Result<Self, GggError> {
        let mut mapping = Self::default_mapping();
        for (k, v) in map_overrides.into_iter() {
            mapping.insert(k, v);
        }
        Self::new(data_part, mapping, output_file, runlog, clobber)
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

    fn make_group_defs(runlog: Runlog, data_part: &utils::DataPartition, detector_mapping: &HashMap<char, String>, nc_file: &mut netcdf::MutableFile) -> Result<Vec<SpecGroupDef>, GggError> {
        let mut groups: Vec<SpecGroupDef> = Vec::new();

        for data_rec in runlog {
            let spec_grp = groups.iter_mut().find(|g| g.entry_matches_group(&data_rec).unwrap_or(false));
            if let Some(spec_grp) = spec_grp {
                if let Ok(size) = ggg_rs::opus::get_spectrum_num_points(&data_rec.spectrum_name, data_part, data_rec.pointer, data_rec.bpw) {
                    let size: usize = size.try_into().expect("Could not fit number of spectrum points into system usize");
                    if spec_grp.max_spec_length < size {
                        spec_grp.max_spec_length = size;
                    }
                }
            }else{
                let new_group = SpecGroupDef::new(&data_rec, data_part, detector_mapping)?;
                groups.push(new_group);
            }
        }
        
        for group in groups.iter() {
            Self::create_group(nc_file, group)?;
        }
        Ok(groups)
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
            Err(GggError::NotImplemented(format!("Group for spectrum {} was not created during initialization", runlog_entry.spectrum_name)))
        }
    }

    fn create_group(nc_file: &mut netcdf::MutableFile, group_def: &SpecGroupDef) -> Result<(), GggError> {
        let nc_path = nc_file.path().unwrap_or_else(|_| PathBuf::from("?"));
        // This creates the new spectrum group, with an unlimited dimension for time so that we can append new spectra.
        let mut grp = nc_file.add_group(&group_def.group_name)
            .map_err(|e| GggError::CouldNotWrite { 
                path: nc_path.clone(), 
                reason: format!("Could not create netCDF group {}: {}", group_def.group_name, e) 
            })?;

        Self::init_group(&nc_path, &mut grp, &group_def.group_name, group_def.max_spec_length)?;

        Ok(())
    }

    fn init_group(nc_path: &Path, grp: &mut netcdf::GroupMut, group_name: &str, max_spec_length: usize) -> Result<(), GggError> {
        grp.add_dimension(Self::spec_dim(), 0)
        .map_err(|e| GggError::CouldNotWrite { 
            path: nc_path.to_owned(), 
            reason: format!("Could not create dimension 'spectrum' (unlimited) in '{group_name}': {e}")
        })?;

        grp.add_dimension(Self::freq_dim(), max_spec_length)
        .map_err(|e| GggError::CouldNotWrite {
            path: nc_path.to_owned(),
            reason: format!("Could not add frequency dimension (unlimited) to '{group_name}' group: {e}") 
        })?;

        let mut freq_var = grp.add_variable::<f32>(Self::freq_dim(), &[Self::spec_dim(), Self::freq_dim()])
        .map_err(|e| GggError::CouldNotWrite { 
            path: nc_path.to_owned(), 
            reason: format!("Could not create frequency variable in group '{group_name}': {e}") 
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

        let ext: Extents = spec_idx.into();
        var.put_string(value, ext)
        .map_err(|e| GggError::CouldNotWrite { 
            path: PathBuf::from("?"), 
            reason: format!("Could not write string value to variable '{varname}' in group '{group_name}' at index {spec_idx}: {e}")
        })?;

        Ok(var)
    }
}

impl NcWriter for MultipleNcWriter {
    fn add_spectrum(&mut self, data_rec: &RunlogDataRec, spectrum: &Spectrum, full_spec_paths: bool) -> Result<(), GggError> {
        // For each entry, check if the spectrum can go in one of the existing groups. If we need to create a group, do so.
        // If there's an issue (i.e. the spectrum should go in a certain group based on its detector code but has a different
        // frequency grid) either crash or skip that spectrum.

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

        if full_spec_paths {
            let spec_path = format!("{}", spectrum.path.display());
            Self::write_str_var(&mut grp, "spectrum", next_idx, &spec_path, "Spectrum name")?;
        } else {
            Self::write_str_var(&mut grp, "spectrum", next_idx, &data_rec.spectrum_name, "Spectrum name")?;
        }
        Self::write_spectrum_values(&mut grp, data_rec, spectrum, &self.save_file, next_idx, true)
    }

    fn write_0d_var<'f, T: netcdf::NcPutGet>(nc: &'f mut netcdf::GroupMut, varname: &str, spec_idx: usize, value: T, units: &str, description: &str) 
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

        let ext: Extents = spec_idx.into();
        var.put_value(value, ext)
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

        let ext: Extents = [spec_idx..spec_idx+1, 0..values.len()].into();
        var.put_values(values, ext)
        .map_err(|e| GggError::CouldNotWrite { 
            path: PathBuf::from("?"),
            reason: format!("Could not write values for variable '{varname}' at spectrum index {spec_idx} in group '{group_name}': {e}")
        })?;

        Ok(var)
    }
}