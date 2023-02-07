use std::{collections::HashMap, path::{Path, PathBuf}, str::FromStr, io::BufRead};
use clap::Parser;
use plotly::{Plot, Scatter, ImageFormat};
use ggg_rs::utils::{self, GggError};


struct SptData {
    header: SptHeader,
    columns: Vec<String>,
    data: HashMap<String, Vec<f32>>
}

struct SptHeader {
    _first_freq: f32,
    _last_freq: f32,
    _num_freq: usize,
    _effective_spec_resolution: f32,
    _sza: f32,
    _obs_alt: f32,
    _zmin: f32,
    _fit_rms: f32,
    _effective_pressure: f32,
    _solar_disk_frac_obs: f32,
    zero_offset: f32,
}

impl SptHeader {
    fn from_header_line(line: &str) -> Result<SptHeader, GggError> {
        fn parse_field<T: FromStr>(s: &str, i: usize) -> Result<T, GggError> {
            s.parse::<T>()
                .map_err(|e| GggError::HeaderError { path: PathBuf::new(), cause: format!("Could not parse {}th element {} as a number", i+1, s) })
        }

        let split: Vec<&str> = line.trim().split_ascii_whitespace().collect();
        if split.len() < 11 {
            return Err(GggError::HeaderError { path: PathBuf::new(), cause: format!("Spectral fit file had too few elements in the second header line (expected {}, found {})", 11, split.len() )});
        }
        
        Ok(Self {
            _first_freq: parse_field(split[0], 0)?,
            _last_freq: parse_field(split[1], 1)?,
            _num_freq: parse_field(split[2], 2)?,
            _effective_spec_resolution: parse_field(split[3], 3)?,
            _sza: parse_field(split[4], 4)?,
            _obs_alt: parse_field(split[5], 5)?,
            _zmin: parse_field(split[6], 6)?,
            _fit_rms: parse_field(split[7], 7)?,
            _effective_pressure: parse_field(split[8], 8)?,
            _solar_disk_frac_obs: parse_field(split[9], 9)?,
            zero_offset: parse_field(split[10], 10)?,
        })
    }
}


fn read_spt_file(spt_file: &Path) -> Result<SptData, GggError> {
    let mut fbuf = utils::FileBuf::open(spt_file)?;
    let (nhead, ncol) = utils::get_nhead_ncol(&mut fbuf)?;

    // Assumume that the second line has the spectrum information (SZA, ZLO, etc)
    let line = fbuf.read_header_line()?;
    let header = SptHeader::from_header_line(&line)?;

    // Assume that the last line of the header has the column names
    let mut col_names = String::new();
    for _ in 2..nhead {
        col_names = fbuf.read_header_line()?;
    }

    let col_names: Vec<&str> = col_names.trim().split_ascii_whitespace().collect();
    if col_names.len() != ncol {
        return Err(GggError::HeaderError { path: spt_file.to_owned(), cause: format!("Number of column names ({}) is not equal to the number of columns specified in the first line ({})", col_names.len(), ncol) });
    }

    let mut data: Vec<Vec<f32>> = Vec::with_capacity(ncol);
    for _ in 0..ncol {
        data.push(Vec::new());
    }

    for (iline, line) in fbuf.into_reader().lines().enumerate() {
        let line = line.map_err(|e| GggError::DataError { 
            path: spt_file.to_owned(), 
            cause: format!("Error reading line {}: {}", iline + nhead, e)
        })?;

        let split: Vec<f32> = line
            .trim()
            .split_ascii_whitespace()
            .map(|s| s.parse::<f32>().unwrap_or(f32::NAN))
            .collect();

        if split.len() < ncol {
            return Err(GggError::DataError { 
                path: spt_file.to_owned(), 
                cause: format!("Line {} does not have the expected number of columns ({})", iline+nhead, ncol)
            });
        }

        for (icol, val) in split.into_iter().enumerate() {
            data[icol].push(val);
        }
    }

    let mut data_map = HashMap::new();
    for (&col, vals) in col_names.iter().zip(data) {
        data_map.insert(col.to_owned(), vals);
    }

    // Finally convert the Tm and Tc to be compatible with the other transmittances (see GGG FAQs on the wiki)
    if let Some(continuum) = data_map.remove("Cont") {
        if let Some(mut tm) = data_map.remove("Tm") {
            for (i, &c) in continuum.iter().enumerate() {
                tm[i] = (tm[i] / c - header.zero_offset) / (1.0 - header.zero_offset);
            }
            data_map.insert("Tm".to_owned(), tm);
        } 

        if let Some(mut tc) = data_map.remove("Tc") {
            for (i, &c) in continuum.iter().enumerate() {
                tc[i] = (tc[i] / c - header.zero_offset) / (1.0 - header.zero_offset);
            }
            data_map.insert("Tc".to_owned(), tc);
        } 

        // We had to remove continuum to avoid holding an immutable ref to the data map, so put it back
        data_map.insert("Cont".to_owned(), continuum);
    } else {
        eprintln!("Warning: cannot convert Tc and Tm to proper transmittances because the Cont (continuum) column could not be found");
    }

    Ok(SptData{
        header,
        columns: col_names.into_iter().map(|el| el.to_owned()).collect(),
        data: data_map,
    })
}

#[derive(Debug, Parser)]
struct Cli {
    spt_file: PathBuf,
    #[clap(short = 'o', long = "output-file")]
    output_file: Option<PathBuf>
}

fn main() -> Result<(), GggError> {
    let clargs = Cli::parse();
    let spt = read_spt_file(&clargs.spt_file)?;

    let output_file = clargs.output_file.unwrap_or_else(|| {
        clargs.spt_file.with_extension("png")
    });

    let freq = spt.data.get("Freq")
        .ok_or_else(|| GggError::DataError { path: clargs.spt_file.clone(), cause: "Could not find the 'Freq' column".to_owned() })?;

    let tm = spt.data.get("Tm").unwrap();

    let mut plot = Plot::new();
    let trace = Scatter::new(freq.clone(), tm.clone());
    let trace = trace.line(plotly::common::Line::new().color("black"));
    plot.add_trace(trace);
    
    plot.write_image(&output_file, ImageFormat::PNG, 2400, 600, 1.0);
    Ok(())
}