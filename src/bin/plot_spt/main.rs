use std::{collections::{HashMap,HashSet}, path::{Path, PathBuf}, str::FromStr, io::BufRead, convert::Infallible, borrow::Borrow, hash::Hash};
use clap::Parser;
use plotly::{Plot, Scatter, ImageFormat, common::{Line, Mode, Title}, Layout, layout::Axis};
use ggg_rs::{utils::{self, GggError}, error::HeaderError};


struct SptData {
    _header: SptHeader,
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
    fn from_header_line(line: &str) -> Result<SptHeader, HeaderError> {
        fn parse_field<T: FromStr>(s: &str, i: usize, full_line: &str) -> Result<T, HeaderError> {
            s.parse::<T>()
                .map_err(|_| HeaderError::ParseError { location: full_line.into(), cause: format!("Could not parse {}th element {} as a number", i+1, s) })
        }

        let split: Vec<&str> = line.trim().split_ascii_whitespace().collect();
        if split.len() < 11 {
            return Err(HeaderError::ParseError { location: line.into(), cause: format!("Spectral fit file had too few elements in the second header line (expected {}, found {})", 11, split.len() )});
        }
        
        Ok(Self {
            _first_freq: parse_field(split[0], 0, line)?,
            _last_freq: parse_field(split[1], 1, line)?,
            _num_freq: parse_field(split[2], 2, line)?,
            _effective_spec_resolution: parse_field(split[3], 3, line)?,
            _sza: parse_field(split[4], 4, line)?,
            _obs_alt: parse_field(split[5], 5, line)?,
            _zmin: parse_field(split[6], 6, line)?,
            _fit_rms: parse_field(split[7], 7, line)?,
            _effective_pressure: parse_field(split[8], 8, line)?,
            _solar_disk_frac_obs: parse_field(split[9], 9, line)?,
            zero_offset: parse_field(split[10], 10, line)?,
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
        return Err(HeaderError::NumColMismatch { location: spt_file.into(), got: col_names.len(), expected: ncol }.into());
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
        _header: header,
        columns: col_names.into_iter().map(|el| el.to_owned()).collect(),
        data: data_map,
    })
}

#[derive(Debug, Parser)]
struct Cli {
    /// Path to the SPT file to plot.
    spt_file: PathBuf,
    
    /// File to save the plot to. If not given, uses the same name as the SPT file plus ".png".
    #[clap(short = 'o', long = "output-file")]
    output_file: Option<PathBuf>,
    
    /// Columns from the SPT file to plot. If omitted, all are plotted.
    #[clap(short = 'c', long = "columns", value_parser = comma_list, default_value = "")]
    columns: OptionalSet<String>, // HashSet was easier to parse to, when this was a Vec, clap expected the parser to return a String, not Vec<String>
}

fn comma_list(arg: &str) -> Result<OptionalSet<String>, Infallible> {
    if arg.len() == 0 {
        Ok(OptionalSet::All)
    }else{
        let set = arg.split(',').map(|el| el.to_owned()).collect();
        Ok(OptionalSet::Some(set))
    }
}

#[derive(Debug, Clone)]
enum OptionalSet<T> 
where T: Eq + Hash
{
    All,
    Some(HashSet<T>)
}

impl<T> OptionalSet<T> 
where T: Eq + std::hash::Hash
{
    fn contains<Q>(&self, el: &Q) -> bool 
    where T: Borrow<Q>,
          Q: Eq + Hash + ?Sized  
    {
        match self {
            OptionalSet::All => true,
            OptionalSet::Some(set) => set.contains(el.borrow()),
        }
    }
}

fn main() -> Result<(), GggError> {
    let clargs = Cli::parse();
    let mut spt = read_spt_file(&clargs.spt_file)?;

    let spt_basename = clargs.spt_file.file_name().expect("Expecting input SPT file to have a path component after the final slash");
    let output_file = clargs.output_file.unwrap_or_else(|| {
        let mut tmp = spt_basename.to_owned();
        tmp.push(".png");
        clargs.spt_file.with_file_name(tmp)
    });

    let freq = spt.data.remove("Freq")
        .ok_or_else(|| GggError::DataError { path: clargs.spt_file.clone(), cause: "Could not find the 'Freq' column".to_owned() })?;

    let mut plot = Plot::new();

    if clargs.columns.contains("Tm") {
        let tm = spt.data.remove("Tm").unwrap();
        let trace = Scatter::new(freq.clone(), tm).name("Measured").mode(Mode::Lines);
        let trace = trace.line(Line::new().color("black"));
        plot.add_trace(trace);
    }

    if clargs.columns.contains("Tc") {
        let tc = spt.data.remove("Tc").expect("Did not find the Tc column in the spt file");
        let trace = Scatter::new(freq.clone(), tc).name("Total calc.").mode(Mode::Lines);
        let trace = trace.line(Line::new().color("gray").dash(plotly::common::DashType::Dash));
        plot.add_trace(trace);
    }

    // Iterate over columns to retain the order. Okay that Tm and Tc are in the columns; they are already removed from the
    // HashMap, so the if let Some(_) check will skip them.
    for key in spt.columns.iter() {
        if !clargs.columns.contains(key) {
            continue;
        }

        if let Some(value) = spt.data.remove(key) {
            let trace = Scatter::new(freq.clone(), value).name(key).mode(Mode::Lines);
            plot.add_trace(trace);
        }
    }

    let layout = Layout::new()
        .title(Title::new(spt_basename.to_string_lossy().as_ref()))
        .x_axis(Axis::new().title(Title::new("Frequency (cm-1)")))
        .y_axis(Axis::new().title(Title::new("Transmittance (AU)")));
    plot.set_layout(layout);
    
    plot.write_image(&output_file, ImageFormat::PNG, 2400, 600, 1.0);
    Ok(())
}