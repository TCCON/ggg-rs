use std::{collections::{HashSet, HashMap}, fmt::Display};

use chrono::{DateTime, Utc};
use error_stack::ResultExt;
use itertools::Itertools;
use log::{debug, info};
use ndarray::{Array1, ArrayView1, Array2};

use crate::sources::DataSourceList;


#[derive(Debug, thiserror::Error)]
pub enum DimensionError {
    #[error("Dimension {dim} is provided by two separate files: {first_src} and {second_src}")]
    MultiplyDefinedDimension{dim: Dimension, first_src: String, second_src: String},
    #[error("Required dimension '{0}' not provided by any source file")]
    MissingDimension(Dimension),
    #[error("Error writing dimension '{0}' information")]
    WriteError(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Dimension {
    Time,
    PriorTime,
    PriorAltitude,
    CellIndex,
    SpectrumNameLength,
    AkAltitude,
    AkSlantXgasBin,
    StringLength(usize)
}

impl Display for Dimension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Dimension::Time => write!(f, "time"),
            Dimension::PriorTime => write!(f, "prior_time"),
            Dimension::PriorAltitude => write!(f, "prior_altitude"),
            Dimension::CellIndex => write!(f, "cell_index"),
            Dimension::SpectrumNameLength => write!(f, "specname"),
            Dimension::AkAltitude => write!(f, "ak_altitude"),
            Dimension::AkSlantXgasBin => write!(f, "ak_slant_xgas_bin"),
            Dimension::StringLength(n) => write!(f, "a{n}"),
        }
    }
}

impl Dimension {
    fn standard_name(&self) -> String {
        match self {
            Dimension::Time => "time".to_string(),
            Dimension::PriorTime => "prior_time".to_string(),
            Dimension::PriorAltitude => "prior_altitude_profile".to_string(),
            Dimension::CellIndex => "cell_index".to_string(),
            Dimension::SpectrumNameLength => "".to_string(),
            Dimension::AkAltitude => "averaging_kernel_altitude_levels".to_string(),
            Dimension::AkSlantXgasBin => "averaging_kernel_slant_xgas_bin_index".to_string(),
            Dimension::StringLength(_) => "".to_string(),
        }
    }

    fn long_name(&self) -> String {
        self.standard_name().replace("_", " ")
    }

    fn description(&self) -> String {
        match self {
            Dimension::Time => "UTC time of zero path difference for this spectrum".to_string(),
            Dimension::PriorTime => "UTC time for the prior profiles, corresponds to GEOS5 times every 3 hours from 0 to 21".to_string(),
            Dimension::PriorAltitude => "altitude levels for the prior profiles, these are the same for all the priors".to_string(),
            Dimension::CellIndex => "variables with names including 'cell_' will be along dimensions (prior_time,cell_index)".to_string(),
            Dimension::SpectrumNameLength => "".to_string(),
            Dimension::AkAltitude => "altitude levels for column averaging kernels".to_string(),
            Dimension::AkSlantXgasBin => "index of the slant xgas bins for the column averaging kernels".to_string(),
            Dimension::StringLength(_) => "".to_string(),
        }
    }
}

pub enum DimensionWithValues {
    Time(Array1<DateTime<Utc>>, Array1<String>), 
    PriorTime(Array1<DateTime<Utc>>),
    PriorAltitude(Array1<f32>),
    CellIndex,
    SpectrumNameLength(usize),
    AkAltitude(Array1<f32>),
    AkSlantXgasBin,
    StringLength(usize)
}


impl DimensionWithValues {
    pub fn len(&self) -> usize {
        match self {
            DimensionWithValues::Time(t, _) => t.len(),
            DimensionWithValues::PriorTime(t) => t.len(),
            DimensionWithValues::PriorAltitude(alt) => alt.len(),
            DimensionWithValues::CellIndex => 2,
            DimensionWithValues::SpectrumNameLength(n) => *n,
            DimensionWithValues::AkAltitude(alt) => alt.len(),
            DimensionWithValues::AkSlantXgasBin => 15,
            DimensionWithValues::StringLength(n) => *n,
        }
    }

    pub fn dimension(&self) -> Dimension {
        match self {
            DimensionWithValues::Time(_, _) => Dimension::Time,
            DimensionWithValues::PriorTime(_) => Dimension::PriorTime,
            DimensionWithValues::PriorAltitude(_) => Dimension::PriorAltitude,
            DimensionWithValues::CellIndex => Dimension::CellIndex,
            DimensionWithValues::SpectrumNameLength(_) => Dimension::SpectrumNameLength,
            DimensionWithValues::AkAltitude(_) => Dimension::AkAltitude,
            DimensionWithValues::AkSlantXgasBin => Dimension::AkSlantXgasBin,
            DimensionWithValues::StringLength(n) => Dimension::StringLength(*n),
        }
    }

    pub fn name(&self) -> String {
        self.dimension().to_string()
    }

    fn write_variable<'a>(&self, nc: &'a mut netcdf::GroupMut<'a>) -> error_stack::Result<(), netcdf::Error> {
        match self {
            DimensionWithValues::Time(times, specnames) => {
                Self::write_time_var(nc, times.view(), self.dimension())?;
                Self::write_specnames(nc, specnames.view())?;
            },
            DimensionWithValues::PriorTime(times) => {
                Self::write_time_var(nc, times.view(), self.dimension())?;
            },
            DimensionWithValues::PriorAltitude(_) => todo!(),
            DimensionWithValues::CellIndex => todo!(),
            DimensionWithValues::SpectrumNameLength(_) => (), // no actual values to write, just need the dimension defined
            DimensionWithValues::AkAltitude(_) => todo!(),
            DimensionWithValues::AkSlantXgasBin => todo!(),
            DimensionWithValues::StringLength(_) => (), // no actual values to write, just need the dimension defined
        }

        Ok(())
    }

    fn write_time_var(nc: &mut netcdf::GroupMut, times: ArrayView1<DateTime<Utc>>, dim: Dimension) -> error_stack::Result<(), netcdf::Error> {
        let timestamps = times.iter()
            .map(|t| t.timestamp())
            .collect_vec();

        let time_name = dim.to_string();
        let mut var = nc.add_variable::<i64>(&time_name, &[&time_name])?;
        var.put_values(timestamps.as_slice(), netcdf::Extents::All)?;

        var.put_attribute("standard_name", dim.standard_name())?;
        var.put_attribute("long_name", dim.long_name())?;
        var.put_attribute("description", dim.description())?;
        var.put_attribute("units", "seconds since 1970-01-01 00:00:00")?;
        var.put_attribute("calendar", "gregorian")?;
        Ok(())
    }

    fn write_specnames<'a>(nc: &'a mut netcdf::GroupMut<'a>, specnames: ArrayView1<String>) -> error_stack::Result<(), netcdf::Error> {
        let ntimes = nc.dimension(&Dimension::Time.to_string())
            .ok_or_else(|| netcdf::Error::NotFound(Dimension::Time.to_string()))?
            .len();
        let speclength = nc.dimension(&Dimension::SpectrumNameLength.to_string())
            .ok_or_else(|| netcdf::Error::NotFound(Dimension::Time.to_string()))?
            .len();

        if ntimes != specnames.len() {
            return Err(netcdf::Error::DimensionMismatch { wanted: specnames.len(), actual: ntimes })
                .attach_printable("Number of times in netCDF file does not match the number of spectrum names")?;
        }

        let mut specname_chars = Array2::<u8>::zeros((ntimes, speclength));
        for (mut char_row, specname) in specname_chars.rows_mut().into_iter().zip(specnames.iter()) {
            let bytes = Array1::from_iter(specname.as_bytes().iter().copied());
            char_row.assign(&bytes);
        }

        // Should this error, we might be able to use "as_standard_order" to convert it - but we want to be
        // sure that does the right thing, so let's wait for a test case.
        let values_slice = specname_chars.as_slice()
        .ok_or_else(|| netcdf::Error::Str(
            "Could not convert 2D character array to standard order slice".to_string()
        ))?;

        let mut var = nc.add_variable_with_type(
            "spectrum",
            &[&Dimension::Time.to_string(), &Dimension::SpectrumNameLength.to_string()],
            &netcdf::types::VariableType::Basic(netcdf::types::BasicType::Char)
        )?;

        var.put_values(values_slice, netcdf::Extents::All)?;

        // unsafe {
        //     var.put_raw_values(values_slice, netcdf::Extents::All)?;
        // }
        var.put_attribute("_Encoding", "UTF-8")?;
        var.put_attribute("standard_name", "spectrum_file_name")?;
        var.put_attribute("long_name", "spectrum file name")?;
        var.put_attribute("description", "name of the primary spectrum for each observation")?;

        Ok(())
    }
}

pub(crate) fn write_required_dimensions<'a>(nc: &'a mut netcdf::GroupMut<'a>, sources: &DataSourceList) -> error_stack::Result<(), DimensionError> {
    let req_dims = identify_required_dimensions(sources)?;
    create_dims_in_group(nc, &req_dims)?;
    write_dims_in_group(nc, &req_dims)?;
    info!("{} dimensions written", req_dims.len());
    Ok(())
}

fn identify_required_dimensions(sources: &DataSourceList) -> error_stack::Result<Vec<&DimensionWithValues>, DimensionError> {
    let mut required_dims: HashSet<Dimension> = HashSet::new();
    for source in sources.iter() {
        required_dims.extend(source.required_dimensions().iter());
    }

    let mut available_dims: HashMap<Dimension, (&DimensionWithValues, String)> = HashMap::new();
    for source in sources.iter() {
        for dim_vals in source.provided_dimensions() {
            let dim = dim_vals.dimension();
            if let Some((_, first_src)) = available_dims.get(&dim) {
                return Err(DimensionError::MultiplyDefinedDimension { dim, first_src: first_src.to_string(), second_src: source.to_string() }.into());

            } else {
                available_dims.insert(dim, (dim_vals, source.to_string()));
            }
        }
    }

    let dims_to_write: Vec<&DimensionWithValues> = required_dims.iter()
        .map(|d| {
            available_dims.get(d)
                .map(|v| v.0)
                .ok_or_else(|| DimensionError::MissingDimension(*d))
        }).try_collect()?;

    Ok(dims_to_write)
}

fn create_dims_in_group(nc: &mut netcdf::GroupMut, dims: &[&DimensionWithValues]) -> error_stack::Result<(), DimensionError> {
    for &dim in dims {
        nc.add_dimension(&dim.name(), dim.len())
            .change_context(DimensionError::WriteError(dim.name()))?;
        debug!("Dimension {} defined", dim.name());
    }
    Ok(())
}

fn write_dims_in_group(nc: &mut netcdf::GroupMut, dims: &[&DimensionWithValues]) -> error_stack::Result<(), DimensionError> 
{
    for &dim in dims {
        dim.write_variable(nc)
            .change_context(DimensionError::WriteError(dim.name()))?;
        debug!("Dimension {} written", dim.name());
    }

    Ok(())
}