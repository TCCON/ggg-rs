use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use ggg_rs::utils::{self, FileBuf};

use crate::CliError;

const N_GLOBAL_HEADER_LINES: usize = 2;
const I_BLOCK_SPECTRUM_INDEX: usize = 0;
const I_BLOCK_SIZE_INDEX: usize = 2;

pub(crate) struct JacFileLines {
    f: FileBuf<BufReader<File>>,
    global_index: usize,
    in_block_index: usize,
    block_length: Option<usize>,
}

impl JacFileLines {
    pub(crate) fn open(path: &Path) -> Result<Self, CliError> {
        let f = FileBuf::open(path)?;
        Ok(Self {
            f,
            global_index: 0,
            in_block_index: 0,
            block_length: None,
        })
    }

    fn parse_block_size(block_size_line: &str) -> Result<usize, CliError> {
        let shape = utils::parse_file_shape_info(block_size_line, Some(4))
            .map_err(|e| CliError::GggError(utils::GggError::HeaderError(e)))?;
        // parse_file_shape_info checks the number of values, so we know there are at least 4 values
        // and the indexing is safe.
        let nfp = shape[2];
        let nlev_atm = shape[3];

        // A Jacobian block (as of 2026-08-24) has 1 spectrum line, 1 location line, 1 shape line,
        // nfp rows of partial derivatives, nlev_atm rows of single-level PDs, and 6 additional lines.
        Ok(3 + nfp + nlev_atm + 6)
    }
}

impl Iterator for JacFileLines {
    type Item = Result<JacBlockLine, CliError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf = String::new();
        let n_res = self.f.read_line(&mut buf);
        match (n_res, self.in_block_index) {
            (Ok(0), 0) => return None,
            (Ok(0), _) => {
                return Some(Err(CliError::Custom(
                    "Jacobian file ended in the middle of a block".to_string(),
                )))
            }
            (Err(e), _) => return Some(Err(CliError::IoError(e))),
            _ => (),
        }

        self.global_index += 1;
        if self.global_index <= N_GLOBAL_HEADER_LINES {
            return Some(Ok(JacBlockLine::GlobalHeader(buf)));
        }

        if self.in_block_index == I_BLOCK_SPECTRUM_INDEX {
            self.in_block_index += 1;
            return Some(Ok(JacBlockLine::Spectrum(buf)));
        }

        if self.in_block_index == I_BLOCK_SIZE_INDEX {
            self.in_block_index += 1;
            self.block_length = match Self::parse_block_size(&buf) {
                Ok(n) => Some(n),
                Err(e) => return Some(Err(e)),
            };
            return Some(Ok(JacBlockLine::BlockSize(buf)));
        }

        if self
            .block_length
            .is_some_and(|n| self.in_block_index == n - 1)
        {
            self.in_block_index = 0;
            self.block_length = None;
        } else {
            // block length has not been set yet; cannot be at the end of the block.
            self.in_block_index += 1;
        }

        Some(Ok(JacBlockLine::Other(buf)))
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum JacBlockLine {
    GlobalHeader(String),
    Spectrum(String),
    BlockSize(String),
    Other(String),
}

impl JacBlockLine {
    #[allow(unused)]
    pub(crate) fn is_global_header(&self) -> bool {
        if let Self::GlobalHeader(_) = self {
            true
        } else {
            false
        }
    }

    #[allow(unused)]
    pub(crate) fn is_spectrum(&self) -> bool {
        if let Self::Spectrum(_) = self {
            true
        } else {
            false
        }
    }

    #[allow(unused)]
    pub(crate) fn is_block_size(&self) -> bool {
        if let Self::BlockSize(_) = self {
            true
        } else {
            false
        }
    }

    #[allow(unused)]
    pub(crate) fn is_other(&self) -> bool {
        if let Self::Other(_) = self {
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use ggg_rs::test_utils;

    use super::*;

    #[test]
    fn test_jac_file_iter() {
        let jac_file = test_utils::test_data_dir()
            .join("inputs")
            .join("averaging-kernels")
            .join("jac")
            .join("j_hcl_5625.test");
        let mut lines =
            JacFileLines::open(&jac_file).expect("should be able to open the test file");

        let next_line = lines
            .next()
            .expect("should have a first line")
            .expect("should be able to read first line");
        assert!(
            next_line.is_global_header(),
            "first line is not global header"
        );

        let next_line = lines
            .next()
            .expect("should have a second line")
            .expect("should be able to read second line");
        assert!(
            next_line.is_global_header(),
            "second line is not global header"
        );

        let expected_spec = [
            "ae20160815sgeeaa.0031",
            "ae20160815sgeeaa.0032",
            "ae20160815sgeeaa.0041",
            "ae20160815sgeeaa.0042",
            "ae20160815sgeeaa.0051",
        ];
        let mut ispec = 0;
        for line in lines {
            let line = line.expect("should not error reading test file");
            if let JacBlockLine::Spectrum(spec) = line {
                assert_eq!(spec.trim(), expected_spec[ispec]);
                ispec += 1;
            }
        }
    }
}
