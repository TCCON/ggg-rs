#[cfg(feature = "netcdf")]
use std::{eprintln, fmt::Debug, format, ops::Mul, str::FromStr};
use std::{
    io::{BufRead, BufReader, Lines},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

#[cfg(feature = "netcdf")]
use approx::AbsDiffEq;
#[cfg(feature = "netcdf")]
use netcdf::NcTypeDescriptor;

#[allow(dead_code)]
pub fn test_data_dir() -> PathBuf {
    let crate_root = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(crate_root).join("test-data")
}

pub fn remove_file_if_exists(file: &Path) -> std::io::Result<()> {
    if file.exists() {
        std::fs::remove_file(file)
    } else {
        Ok(())
    }
}

pub fn compare_output_text_files(expected_dir: &Path, output_dir: &Path, out_file_name: &str) {
    let mut child = Command::new("diff")
        .arg("-q")
        .arg(expected_dir.join(out_file_name))
        .arg(output_dir.join(out_file_name))
        .stdout(Stdio::null())
        .spawn()
        .expect("Spawning diff process should not fail");

    let is_same = child
        .wait()
        .expect("Waiting for diff process should not fail")
        .success();
    assert!(is_same, "{out_file_name} did not match expected.");
}

/// Compare two arrays with [`approx::relative_eq!`].
///
/// # Parameters
/// - The first parameter is the array with the expected values
/// - The second parameter is the array with the actual values
/// - The third parameter is the max relative different. [`approx`]
///   will compare the absolute differences to the larger value times
///   this multiplier.
///
/// # Side effects
/// If the two arrays do not agree to within the default EPSILON for their
/// type and the given relative error, this will panic, display the two
/// arrays' debug representations, and save them as Numpy binaries to
/// the current directory.
macro_rules! assert_arrays_rel_eq {
    ($expected:expr, $actual:expr, $max_relative:expr) => {
        if !approx::relative_eq!($actual, $expected, max_relative = $max_relative) {

            // Dump to disk (ignoring Result so it doesn't mask the panic)
            let _ = ndarray_npy::write_npy("debug_actual.npy", $actual);
            let _ = ndarray_npy::write_npy("debug_expected.npy", $expected);

            panic!(
                "Array assertion failed!\nleft = {:?}\nright = {:?} \nActual and Expected arrays have been dumped to 'debug_actual.npy' and 'debug_expected.npy', access with numpy.load.",
                $actual, $expected
            );
        }
    };
}

/// Compare an array of plain floats to expected values in a [`netcdf::File`].
///
/// This will use a relative comparison of 1e-7 times the larger value in each comparison.
/// That was necessary for comparing the CO2 prior loaded from L2 files in the `write-timeavg-netcdf`
/// code.
///
/// # See also
/// [`compare_to_netcdf_values_eq`] - for integer arrays
/// [`compare_to_netcdf_quantities`] - for [`uom::si::Quantity`] arrays
#[cfg(feature = "netcdf")]
pub fn compare_to_netcdf_values_approx<T, D>(
    expected_ds: &netcdf::File,
    nc_var: &str,
    data: &ndarray::Array<T, D>,
) where
    T: Copy
        + NcTypeDescriptor
        + approx::RelativeEq
        + Debug
        + ndarray_npy::WritableElement
        + From<f32>,
    <T as approx::AbsDiffEq>::Epsilon: Clone + From<f32>,
    D: ndarray::Dimension,
{
    let expected_data = crate::nc_utils::get_var_data::<T, D>(expected_ds, nc_var).expect(
        &format!("Could not get variable '{nc_var}' from the validation netCDF file"),
    );
    eprintln!("Checking against {nc_var}");

    // The relative tolerance of 1e-7 was selected for timeavg read_l2 tests of the CO2 prior
    let rtol: <T as AbsDiffEq>::Epsilon = 1e-7.into();
    assert_arrays_rel_eq!(&expected_data, data, rtol)
}

/// Compare an array of plain numbers to expected values in a [`netcdf::File`].
///
/// This will use exact equality, so is only useful for integers. Hence, `T`
/// is limited to types that implement `Eq`, not just `PartialEq`.
///
/// # See also
/// [`compare_to_netcdf_values_approx`] - for float arrays
/// [`compare_to_netcdf_quantities`] - for [`uom::si::Quantity`] arrays
#[cfg(feature = "netcdf")]
pub fn compare_to_netcdf_values_eq<T, D>(
    expected_ds: &netcdf::File,
    nc_var: &str,
    data: &ndarray::Array<T, D>,
) where
    T: Copy + NcTypeDescriptor + Eq + Debug,
    D: ndarray::Dimension,
{
    let expected_data = crate::nc_utils::get_var_data::<T, D>(expected_ds, nc_var).expect(
        &format!("Could not get variable '{nc_var}' from the validation netCDF file"),
    );
    eprintln!("Checking against {nc_var}");
    assert_eq!(expected_data, data)
}

/// Compare an array of [`uom::si::Quantity`] values to expected values in a [`netcdf::File`].
///
/// This uses the same relative comparison as [`compare_to_netcdf_values_approx`] for the
/// same reason. Note that this requires the netCDF variable read to have the "units" attribute
/// and for those units to be parseable by [`crate::nc_utils::get_var_data_quantity`].
///
/// The actual comparison is done by converting the quantities back to `f64` values in their
/// base units. This keeps the trait bounds simpler and ensures that we do an apples-to-apples
/// comparison, but if the quantities contained `f32` values, you could see some value drift.
#[cfg(feature = "netcdf")]
pub fn compare_to_netcdf_quantities<T, D, Q>(
    expected_ds: &netcdf::File,
    nc_var: &str,
    data: &ndarray::ArrayRef<Q, D>,
) where
    T: Copy + NcTypeDescriptor,
    D: ndarray::Dimension,
    Q: FromStr<Err = uom::str::ParseQuantityError>
        + Copy
        + Mul<T, Output = Q>
        + crate::array_ops::FloatConversion,
{
    let expected_data = crate::nc_utils::get_var_data_quantity::<T, D, Q>(expected_ds, nc_var)
        .expect(&format!(
            "Could not get variable '{nc_var}' from the validation netCDF file"
        ));
    let expected_data = expected_data.mapv(|q| q.into_f64());
    let data = data.mapv(|q| q.into_f64());
    eprintln!("Checking against {nc_var}");
    // The relative tolerance of 1e-7 was selected for timeavg read_l2 tests of the CO2 prior
    assert_arrays_rel_eq!(&expected_data, &data, 1e-7)
}

/// Iterate over fenced blocks in a Markdown file.
///
/// Fenced blocks are the blocks that start and end with three backticks.
/// They optionally have a tag immediately following the opening backticks
/// to specify the language in the example. This will create an iterator
/// that returns each fenced block tagged with `tag` in each of the files
/// listed in `files`. `files` may be anything that can become an iterator
/// over pathlike objects.
pub fn iter_fenced_blocks<P, F, I>(tag: &str, files: I) -> FencedBlocks<P, F>
where
    P: AsRef<Path>,
    F: Iterator<Item = P>,
    I: IntoIterator<IntoIter = F>,
{
    FencedBlocks::new(tag, files)
}

pub struct FencedBlock {
    pub text: String,
    pub file: PathBuf,
    pub line: usize,
}

pub struct FencedBlocks<P, F>
where
    P: AsRef<Path>,
    F: Iterator<Item = P>,
{
    fence_start: String,
    files: F,
    lines: Option<Lines<BufReader<std::fs::File>>>,
    curr_file: Option<PathBuf>,
    line_num: usize,
}

impl<P, F> Iterator for FencedBlocks<P, F>
where
    P: AsRef<Path>,
    F: Iterator<Item = P>,
{
    type Item = std::io::Result<FencedBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.lines.is_none() {
                if let Err(e) = self.open_next_file()? {
                    return Some(Err(e));
                }
            }

            let next_line = self.get_next_line();
            // The `if self.lines.is_none()` block should return if it couldn't make self.lines be Some,
            // so flattening in get_next_line() means that we only have a None if we ran out of lines in the file.
            let opt_line = match next_line {
                Some(Ok(line)) => Some(line),
                Some(Err(e)) => return Some(Err(e)),
                None => None,
            };

            if let Some(line) = opt_line {
                if line.starts_with(&self.fence_start) && !line.contains("#notest") {
                    return Some(self.get_block(self.line_num));
                }
            } else {
                // Ran out of lines in the file, so set lines back to None so that we advance to the next file
                // next time through the loop
                self.lines = None;
            }
        }
    }
}

impl<P, F> FencedBlocks<P, F>
where
    P: AsRef<Path>,
    F: Iterator<Item = P>,
{
    fn new<I: IntoIterator<IntoIter = F>>(tag: &str, files: I) -> Self {
        Self {
            fence_start: format!("```{tag}"),
            files: files.into_iter(),
            lines: None,
            curr_file: None,
            line_num: 0,
        }
    }

    fn open_next_file(&mut self) -> Option<std::io::Result<()>> {
        let next_file = self.files.next()?;
        let f = match std::fs::File::open(next_file.as_ref()) {
            Ok(f) => f,
            Err(e) => return Some(Err(e)),
        };
        let rdr = BufReader::new(f);
        self.lines = Some(rdr.lines());
        self.curr_file = Some(next_file.as_ref().to_path_buf());
        self.line_num = 0;
        Some(Ok(()))
    }

    fn get_block(&mut self, starting_line: usize) -> std::io::Result<FencedBlock> {
        let mut text = String::new();
        // We should only be here if we found a line starting with the opening of
        // a fenced block, so the next line should be the actual first line of the fenced
        // block.
        loop {
            let next_line = match self.get_next_line() {
                Some(Ok(line)) => line,
                Some(Err(e)) => return Err(e),
                None => {
                    let msg = format!("fenced block starting at line {starting_line} was still unclosed at the end of the file");
                    return Err(std::io::Error::other(msg));
                }
            };

            if next_line.starts_with("```") {
                let block = FencedBlock {
                    text,
                    file: self.curr_file.clone().unwrap(),
                    line: self.line_num,
                };
                return Ok(block);
            }

            if !text.is_empty() {
                // lines() doesn't return newlines, so add them back in for all but the last line
                text.push('\n');
            }
            text.push_str(&next_line);
        }
    }

    fn get_next_line(&mut self) -> Option<std::io::Result<String>> {
        self.line_num += 1;
        self.lines.as_mut().map(|it| it.next()).flatten()
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_fenced_iter() {
        let data_root = test_data_dir();
        let md_files = [
            data_root.join("inputs/test_utils/fenced1.md"),
            data_root.join("inputs/test_utils/fenced2.md"),
            data_root.join("inputs/test_utils/fenced3.md"),
        ];
        let it = iter_fenced_blocks("toml", md_files);

        let expected = ["key1 = 1\nkey2 = 2", "key3 = 3", "key4 = \"4\"\nkey5 = '5'"];
        for (s, exp) in it.zip_eq(expected) {
            assert_eq!(s.unwrap().text, exp);
        }
    }
}
