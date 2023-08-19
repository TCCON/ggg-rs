use std::path::{PathBuf, Path};

use ggg_rs::{runlogs::FallibleRunlog};

use crate::interface::{DataSource, Dimension, DataGroup, TranscriptionError, DimensionValues};

pub struct TcconRunlog {
    runlog: PathBuf,
    variables: Vec<String>,
    dimensions: Vec<Dimension>,
}

impl TcconRunlog {
    pub fn new(runlog: PathBuf) -> Result<Self, TranscriptionError> {
        let times = Self::read_times(&runlog)?;
        let time_dim = Dimension::new_time(
            "time".to_string(),
            DimensionValues::DateTime(times),
            true
        );

        let variables = vec![]; // TODO: define the variables we want from the runlog
        Ok(Self { runlog, variables, dimensions: vec![time_dim] })
    }

    fn read_times(runlog: &Path) -> Result<ndarray::Array1<chrono::NaiveDateTime>, TranscriptionError> {
        let runlog_handle = FallibleRunlog::open(runlog)
            .map_err(|e| TranscriptionError::ReadError { file: runlog.to_path_buf(), cause: e.to_string() })?;

        let mut times = vec![];
        for (line, record) in runlog_handle.into_line_iter() {
            let this_time = match record {
                Ok(rl_rec) => rl_rec.zpd_time(),
                Err(e) => return Err(TranscriptionError::ReadErrorAtLine { file: runlog.to_owned(), line, cause: e.to_string() })
            };

            if let Some(this_time) = this_time {
                times.push(this_time);
            } else {
                return Err(TranscriptionError::ReadErrorAtLine { file: runlog.to_owned(), line, cause: "Invalid ZPD time".to_string() });
            }
        }

        Ok(ndarray::Array1::from_vec(times))
    }
}

impl DataSource for TcconRunlog {
    fn provided_dimensions(&self) -> &[Dimension] {
        &self.dimensions
    }

    fn required_dimensions(&self) -> &[&str] {
        &["time"]
    }

    fn required_groups(&self) -> &[DataGroup] {
        &[DataGroup::InGaAs]
    }

    fn variable_names(&self) -> &[String] {
        &self.variables
    }

    fn write_variables(&mut self, nc_grp: &mut netcdf::GroupMut, group: crate::interface::DataGroup) -> Result<(), crate::interface::TranscriptionError> {
        todo!()
    }
}