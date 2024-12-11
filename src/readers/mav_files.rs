use core::f32;
use std::{collections::HashMap, io::{BufRead, BufReader}, path::{Path, PathBuf}, str::FromStr};

use itertools::Itertools;
use ndarray::Array1;

use crate::utils::{get_file_shape_info, FileBuf, GggError};

pub struct MavBlockHeader {
    pub ncol: usize,
    pub nlev: usize,
    pub next_spectrum: String,
    pub tropopause_altitude: f32,
    pub observer_latitude: f32,
    pub vmr_file: PathBuf,
    pub mod_file: PathBuf,
}

impl MavBlockHeader {
    fn parse_from_reader(rdr: &mut FileBuf<BufReader<std::fs::File>>, mav_path: &Path, first_line: &str) -> error_stack::Result<Self, GggError> {
        let next_spectrum = Self::split_value_line(first_line, "first", "Next Spectrum")?;
        
        let shape = get_file_shape_info(rdr, 3).map_err(|e| GggError::custom(e.to_string()))?;
        let nhead = shape[0];
        let ncol = shape[1];
        let nlev = shape[2];

        if nhead < 6 {
            return Err(GggError::not_implemented(".mav block header has less than 6 lines - this is not expected for GGG2020 and later").into());
        }

        let mut buf = String::new();
        buf.clear();
        rdr.read_line(&mut buf).map_err(|e| GggError::could_not_read(mav_path.to_path_buf(), e.to_string()))?;
        let trop_alt = Self::parse_value_line::<f32>(&buf, "third", "Tropopause Altitude")?;

        buf.clear();
        rdr.read_line(&mut buf).map_err(|e| GggError::could_not_read(mav_path.to_path_buf(), e.to_string()))?;
        let obs_lat = Self::parse_value_line::<f32>(&buf, "fourth", "Observer Latitude")?;

        buf.clear();
        rdr.read_line(&mut buf).map_err(|e| GggError::could_not_read(mav_path.to_path_buf(), e.to_string()))?;
        let vmr_file = PathBuf::from(&buf);

        buf.clear();
        rdr.read_line(&mut buf).map_err(|e| GggError::could_not_read(mav_path.to_path_buf(), e.to_string()))?;
        let mod_file = PathBuf::from(&buf);

        let n_extra_lines = nhead.saturating_sub(6);
        for _ in 0..n_extra_lines {
            // move past any extra header lines so that we're positioned to read the column names next.
            rdr.read_line(&mut buf).map_err(|e| GggError::could_not_read(mav_path.to_path_buf(), e.to_string()))?;
        }
        
        Ok(Self { ncol, nlev, next_spectrum, tropopause_altitude: trop_alt, observer_latitude: obs_lat, vmr_file, mod_file })
    }

    fn parse_value_line<T: FromStr>(buf: &str, position: &str, start_with: &str) -> error_stack::Result<T, GggError> {
        let s = Self::split_value_line(buf, position, start_with)?;
        let v: T = s.trim().parse().map_err(|e| GggError::custom(format!(
            "could not parse value in the \"{start_with}\" line ({s})"
        )))?;
        Ok(v)
    }

    fn split_value_line(buf: &str, position: &str, start_with: &str) -> Result<String, GggError> {
        let buf = buf.trim();
        if buf.starts_with(&format!("{start_with}:")) {
            let value = buf.split_once(':').unwrap().1.to_string();
            return Ok(value)
        } else {
            return Err(GggError::custom(format!(
                "Expected {position} line of .mav file block to start with \"{start_with}:\", instead got \"{buf}\""
            )));
        }
    }
}

pub struct MavBlock {
    pub header: MavBlockHeader,
    pub data: HashMap<String, Array1<f32>>,
    pub column_order: Vec<String>,
}

impl MavBlock {
    fn parse_from_reader(rdr: &mut FileBuf<BufReader<std::fs::File>>, mav_path: &Path, first_line: &str) -> error_stack::Result<Self, GggError> {
        let header = MavBlockHeader::parse_from_reader(rdr, mav_path, first_line)?;

        let mut buf = String::new();
        rdr.read_line(&mut buf).map_err(|e| GggError::could_not_read(mav_path.to_path_buf(), e.to_string()))?;
        let colnames = buf.trim().split_ascii_whitespace().map(|s| s.to_string()).collect_vec();

        let mut profiles = HashMap::new();
        for colname in colnames.iter() {
            profiles.insert(colname.to_string(), Array1::from_elem(header.nlev, f32::NAN));
        }

        for i in 0..header.nlev {
            buf.clear();
            rdr.read_line(&mut buf).map_err(|e| GggError::could_not_read(mav_path.to_path_buf(), e.to_string()))?;
            for (s, colname) in buf.trim().split_ascii_whitespace().zip(colnames.iter()) {
                let v: f32 = s.parse().map_err(|e| GggError::custom(format!(
                    "Could not parse {colname} value for level {} of {} block as a float",
                    i+1,
                    header.next_spectrum
                )))?;

                let arr = profiles.get_mut(colname)
                    .expect("profiles hash map should alread have an entry for all columns in the .mav block");
                arr[i] = v;
            }
        }

        Ok(Self { header, data: profiles, column_order: colnames })
    }
}

pub struct MavIterator {
    mav_file_path: PathBuf,
    rdr: FileBuf<BufReader<std::fs::File>>
}

impl MavIterator {
    fn new(mav_file: PathBuf) -> Result<Self, GggError> {
        let mut rdr = FileBuf::open(&mav_file)?;
        let mut buf = String::new();
        rdr.read_line(&mut buf).map_err(|e| GggError::could_not_read(mav_file.clone(), e.to_string()))?; // skip the version line
        Ok(Self { mav_file_path: mav_file, rdr })
    }
}

impl Iterator for MavIterator {
    type Item = error_stack::Result<MavBlock, GggError>;

    fn next(&mut self) -> Option<Self::Item> {
        // We have to read the first line here to check if we're at the end of the file
        let mut buf = String::new();
        // This loop should deal with blank lines at the end of the file - we would keep reading
        // until we hit the end of the file. It will always run once, because buf starts out empty.
        while buf.trim().is_empty() {
            buf.clear();
            match self.rdr.read_line(&mut buf) {
                Ok(0) => return None,
                Ok(_) => (),
                Err(e) => {
                    let err = error_stack::Report::new(GggError::could_not_read(self.mav_file_path.clone(), e.to_string()));
                    return Some(Err(err))
                }
            };
        }
        
        let res = MavBlock::parse_from_reader(&mut self.rdr, &self.mav_file_path, &buf);
        Some(res)
    }
}

pub fn open_and_iter_mav_file(mav_file: PathBuf) -> Result<MavIterator, GggError> {
    MavIterator::new(mav_file)
}
