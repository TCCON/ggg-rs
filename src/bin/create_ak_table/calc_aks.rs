use std::{collections::HashMap, f64};

use error_stack::ResultExt;
use ndarray::{s, Array1, Array2, ArrayView1, Axis, Zip};

use crate::read_aks::AkInfo;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CalcError {
    #[error("Received an empty list of AK info structures")]
    EmptyVec,
    #[error("Different spectra have different numbers of levels (ispec = {i1} and {i2})")]
    InconsistentNumLevels { i1: usize, i2: usize },
    #[error("{field} is not the same for all levels of spectrum {ispec}")]
    InconsistentValue { field: &'static str, ispec: usize },
    #[error(
        "{field} levels are not within the expected tolerance between {base_gas} and {other_gas}"
    )]
    InconsistentLevels {
        field: &'static str,
        base_gas: String,
        other_gas: String,
    },
    #[error("Error occurred while creating the table for gas '{0}'")]
    GasContext(String),
}

impl CalcError {
    fn gas_context<S: ToString>(gas: S) -> Self {
        Self::GasContext(gas.to_string())
    }
}

pub(crate) struct AkTableSet {
    pub(crate) tables: HashMap<String, AkTable>,
    pub(crate) sza_bin_centers: Array1<f64>,
    pub(crate) pressure: Array1<f64>,
    pub(crate) altitude: Array1<f64>,
}

pub(crate) struct AkTable {
    pub(crate) bins: AkBinType,
    pub(crate) aks: Array2<f64>,
}

pub(crate) enum AkBinType {
    SZA,
}

struct SpectrumAks {
    zmin: Array1<f64>,
    sza: Array1<f64>,
    airmass: Array1<f64>,
    z: Array1<f64>,
    p: Array1<f64>,
    ak: Array2<f64>,
}

impl SpectrumAks {
    fn new(nspec: usize, nlev: usize) -> SpectrumAks {
        SpectrumAks {
            zmin: Array1::zeros(nspec),
            sza: Array1::zeros(nspec),
            airmass: Array1::zeros(nspec),
            z: Array1::zeros(nlev),
            p: Array1::zeros(nlev),
            ak: Array2::zeros((nspec, nlev)),
        }
    }
}

/// Create the collection of
pub(crate) fn make_ak_tables(
    ak_infos: HashMap<String, Vec<AkInfo>>,
) -> error_stack::Result<AkTableSet, CalcError> {
    // First we handle reshaping the list of AKs from the .all files into proper 2D arrays,
    // with spectrum and level as dimensions.
    let mut spectrum_aks = HashMap::new();
    for (gas, ak_infos) in ak_infos {
        log::info!("Tabulating {gas} AKs");
        let this_gas_aks = make_one_gas_ak_table(&ak_infos)
            .change_context_lazy(|| CalcError::gas_context(gas.clone()))?;
        spectrum_aks.insert(gas, this_gas_aks);
    }

    // We check that all of the gases are on a reasonably consistent veritcal grid
    // so that we don't have to write different vertical grids for different gases
    // in the table file.
    log::debug!("Checking for consistency of AK vertical grids");
    let (pressure, altitude) = check_level_coords(&spectrum_aks)?;

    // Last we handle the binning - for now, just by SZA. Slant Xgas options can come later.
    let mut tables = HashMap::new();
    let bin_edges = ndarray::Array1::range(0.0, 91., 5.0);
    let n_edge = bin_edges.len();
    let bin_centers =
        0.5 * (bin_edges.slice(s![..n_edge - 1]).to_owned() + bin_edges.slice(s![1..]));
    for (gas, gas_spec_aks) in spectrum_aks.into_iter() {
        log::info!("Binning {gas} AKs");
        let ak_array = bin_by_sza(&gas_spec_aks, bin_edges.view());
        tables.insert(
            gas,
            AkTable {
                bins: AkBinType::SZA,
                aks: ak_array,
            },
        );
    }

    let table_set = AkTableSet {
        tables: tables,
        sza_bin_centers: bin_centers,
        pressure,
        altitude,
    };
    Ok(table_set)
}

fn make_one_gas_ak_table(aks: &[AkInfo]) -> error_stack::Result<SpectrumAks, CalcError> {
    let nspec = aks
        .iter()
        .map(|row| row.ispec)
        .max()
        .ok_or(CalcError::EmptyVec)?;
    let nlev = check_num_levels(&aks)?;
    let mut spectrum_aks = SpectrumAks::new(nspec, nlev);

    let mut last_ispec: usize = 0;
    let mut irow: usize = 0;
    let mut ilev: usize = 0;
    let mut zmin = 0.0;
    let mut sza = 0.0;
    let mut airmass = 0.0;

    let mut all_z = Array2::from_elem((nspec, nlev), f64::NAN);
    let mut all_p = Array2::from_elem((nspec, nlev), f64::NAN);

    for info in aks {
        if info.ispec != last_ispec {
            last_ispec = info.ispec;
            ilev = 0;
            irow = last_ispec - 1;
            zmin = info.zmin;
            sza = info.sza;
            airmass = info.airmass;

            spectrum_aks.zmin[irow] = zmin;
            spectrum_aks.sza[irow] = sza;
            spectrum_aks.airmass[irow] = airmass;
        } else {
            ilev += 1;
            if approx::abs_diff_ne!(info.zmin, zmin) {
                return Err(CalcError::InconsistentValue {
                    field: "zmin",
                    ispec: info.ispec,
                }
                .into());
            }

            if approx::abs_diff_ne!(info.sza, sza) {
                return Err(CalcError::InconsistentValue {
                    field: "sza",
                    ispec: info.ispec,
                }
                .into());
            }

            if approx::abs_diff_ne!(info.airmass, airmass) {
                return Err(CalcError::InconsistentValue {
                    field: "airmass",
                    ispec: info.ispec,
                }
                .into());
            }
        }

        all_z[(irow, ilev)] = info.z;
        all_p[(irow, ilev)] = info.p;
        spectrum_aks.ak[(irow, ilev)] = info.ak;
    }

    let z = all_z.mean_axis(Axis(0)).ok_or(CalcError::EmptyVec)?;
    let p = all_p.mean_axis(Axis(0)).ok_or(CalcError::EmptyVec)?;
    spectrum_aks.z = z;
    spectrum_aks.p = p;

    Ok(spectrum_aks)
}

fn check_num_levels(aks: &[AkInfo]) -> Result<usize, CalcError> {
    let num_levels = count_levels_by_spec(aks);
    let mut expected = None;
    for (ispec, nlev) in num_levels.into_iter() {
        if let Some((i, n)) = expected {
            if n != nlev {
                return Err(CalcError::InconsistentNumLevels { i1: i, i2: ispec });
            }
        } else if expected.is_none() {
            expected = Some((ispec, nlev))
        }
    }

    if let Some((_, n)) = expected {
        Ok(n)
    } else {
        Err(CalcError::EmptyVec)
    }
}

fn count_levels_by_spec(aks: &[AkInfo]) -> HashMap<usize, usize> {
    let mut counts = HashMap::new();

    for info in aks {
        let ispec = info.ispec;
        let c = counts.entry(ispec).or_default();
        *c += 1;
    }

    counts
}

fn check_level_coords(
    spectrum_aks: &HashMap<String, SpectrumAks>,
) -> Result<(Array1<f64>, Array1<f64>), CalcError> {
    let mut opt_expected_z = None;
    let mut opt_expected_p = None;
    let mut base_gas = None;

    for (gas, table) in spectrum_aks.iter() {
        if opt_expected_p.is_none() {
            opt_expected_p = Some(&table.p);
            opt_expected_z = Some(&table.z);
            base_gas = Some(gas);
        } else {
            let expected_p = opt_expected_p.unwrap();
            let expected_z = opt_expected_z.unwrap();
            if !expected_p.abs_diff_eq(&table.p, 1e-3) {
                return Err(CalcError::InconsistentLevels {
                    field: "p",
                    base_gas: base_gas.unwrap().to_string(),
                    other_gas: gas.to_string(),
                });
            }
            if !expected_z.abs_diff_eq(&table.z, 1e-3) {
                return Err(CalcError::InconsistentLevels {
                    field: "z",
                    base_gas: base_gas.unwrap().to_string(),
                    other_gas: gas.to_string(),
                });
            }
        }
    }

    if opt_expected_p.is_none() {
        Err(CalcError::EmptyVec)
    } else {
        Ok((
            opt_expected_p.unwrap().to_owned(),
            opt_expected_z.unwrap().to_owned(),
        ))
    }
}

fn bin_by_sza(spec_aks: &SpectrumAks, bin_edges: ArrayView1<f64>) -> Array2<f64> {
    let nlev = spec_aks.z.len();
    let nbin = bin_edges.len() - 1;
    let mut bin_counts = Array1::<usize>::zeros(nbin);
    let mut table = Array2::<f64>::zeros((nlev, nbin));

    for (ispec, sza) in spec_aks.sza.iter().copied().enumerate() {
        if let Some(ibin) = find_bin_index(sza, bin_edges) {
            Zip::from(table.column_mut(ibin))
                .and(spec_aks.ak.row(ispec))
                .for_each(|a, &b| {
                    *a += b;
                });
            bin_counts[ibin] += 1;
        }
    }

    for (ibin, bin_count) in bin_counts.iter().copied().enumerate() {
        if bin_count == 0 {
            table.column_mut(ibin).fill(f64::NAN);
        } else {
            let nspec = bin_count as f64;
            table.column_mut(ibin).mapv_inplace(|v| v / nspec);
        }
    }

    table
}

fn find_bin_index(sza: f64, bin_edges: ArrayView1<f64>) -> Option<usize> {
    for i in 0..bin_edges.len() - 1 {
        if sza >= bin_edges[i] && sza < bin_edges[i + 1] {
            return Some(i);
        }
    }
    None
}
