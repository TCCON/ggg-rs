use chrono::{DateTime, TimeZone};
use itertools::Itertools;
use num_traits::Float;
use std::fmt::Debug;

#[derive(Debug, thiserror::Error)]
pub enum InterpolationError {
    #[error("Input arrays were different lengths (x.len() = {x_len}, y.len() = {y_len}")]
    InputLengthMismatch { x_len: usize, y_len: usize },
    #[error(
        "Input arrays were too short, needed at least {req_len} elements but got only {actual_len}"
    )]
    InputTooShort { req_len: usize, actual_len: usize },
    #[error(
        "Requested output coordinate ({out}) falls outside the input domain ({left} to {right})"
    )]
    OutOfDomain {
        left: String,
        right: String,
        out: String,
    },
}

pub trait InterpolationMethod {
    fn interp1d<F: Float + Debug>(
        &self,
        input_x: &[F],
        input_y: &[F],
        output_x: F,
    ) -> Result<F, InterpolationError>;

    fn interp1d_to_time<Z: TimeZone>(
        &self,
        input_t: &[DateTime<Z>],
        input_y: &[f64],
        output_t: DateTime<Z>,
    ) -> Result<f64, InterpolationError> {
        let input_x = input_t.iter().map(datetime_to_float).collect_vec();

        let output_x = datetime_to_float(&output_t);

        self.interp1d(input_x.as_slice(), input_y, output_x)
    }

    fn check_1d_inputs<F: Float + Debug>(
        &self,
        input_x: &[F],
        input_y: &[F],
        output_x: F,
        must_be_in_bounds: bool,
        min_len: usize,
    ) -> Result<(), InterpolationError> {
        if input_x.len() != input_y.len() {
            return Err(InterpolationError::InputLengthMismatch {
                x_len: input_x.len(),
                y_len: input_y.len(),
            });
        }

        // Now we know both are the same length, so only need to test 1
        if input_x.len() < min_len {
            return Err(InterpolationError::InputTooShort {
                req_len: min_len,
                actual_len: input_x.len(),
            });
        }

        if must_be_in_bounds {
            let left_bound = if let Some(x) = input_x.iter().copied().reduce(F::min) {
                x
            } else {
                // We only get None if the input is empty. If we got here, the min_len must have been 0, which means
                // the interpolator must handle the 0 length case
                return Ok(());
            };

            // If left bound was Some, this must also be Some.
            let right_bound = input_x
                .iter()
                .copied()
                .reduce(F::max)
                .expect("input_x.max() should return Some if input_x.min() returned Some()");

            if output_x < left_bound || output_x > right_bound {
                return Err(InterpolationError::OutOfDomain {
                    left: format!("{left_bound:?}"),
                    right: format!("{right_bound:?}"),
                    out: format!("{output_x:?}"),
                });
            }
        }

        Ok(())
    }
}

pub struct ConstantValueInterp {
    allow_extrapolation: bool,
}

impl ConstantValueInterp {
    pub fn new(allow_extrapolation: bool) -> Self {
        Self {
            allow_extrapolation,
        }
    }
}

impl InterpolationMethod for ConstantValueInterp {
    fn interp1d<F: Float + Debug>(
        &self,
        input_x: &[F],
        input_y: &[F],
        output_x: F,
    ) -> Result<F, InterpolationError> {
        self.check_1d_inputs(input_x, input_y, output_x, !self.allow_extrapolation, 1)?;
        // Checking the inputs ensures that output_x is in the domain of input_x if we do not allow extrapolation, so
        // we can safely find the nearest x value.
        let (i_closest, _) = input_x
            .iter()
            .enumerate()
            .fold(None, |acc, (i, x)| {
                let new_diff = (*x - output_x).abs();
                if let Some((curr_i, curr_diff)) = acc {
                    if new_diff < curr_diff {
                        Some((i, new_diff))
                    } else {
                        Some((curr_i, curr_diff))
                    }
                } else {
                    Some((i, new_diff))
                }
            })
            .expect("Inputs must have at least 1 element");

        Ok(input_y[i_closest])
    }
}

fn datetime_to_float<Z: TimeZone>(t: &DateTime<Z>) -> f64 {
    let ts = t.timestamp() as f64;
    let ts_frac = t.timestamp_subsec_nanos() as f64;
    ts + ts_frac / 1e9
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;
    use chrono::{NaiveDateTime, Utc};

    #[test]
    fn test_constant_value_error_checks() {
        let interpolator = ConstantValueInterp::new(false);

        let err = interpolator.interp1d(&[1.0], &[1.0, 1.0], 2.0).unwrap_err();
        match err {
            InterpolationError::InputLengthMismatch { x_len, y_len } => {
                assert_eq!(x_len, 1, "x_len in error is incorrect");
                assert_eq!(y_len, 2, "y_len in error is incorrect");
            }
            _ => assert!(
                false,
                "Expected InputLengthMismatch error, did not get that"
            ),
        }

        let err = interpolator.interp1d(&[], &[], 2.0).unwrap_err();
        match err {
            InterpolationError::InputTooShort {
                req_len,
                actual_len,
            } => {
                assert_eq!(req_len, 1, "req_len in error is incorrect");
                assert_eq!(actual_len, 0, "actual_len in error is incorrect");
            }
            _ => assert!(false, "Expected InputTooShort error, did not get that"),
        }

        let err = interpolator
            .interp1d(&[1.0, 2.0], &[2.0, 4.0], 0.0)
            .unwrap_err();
        match err {
            InterpolationError::OutOfDomain {
                left: _,
                right: _,
                out: _,
            } => (),
            _ => assert!(false, "Expected OutOfDomain error, did not get that"),
        }
    }

    #[test]
    fn test_constant_value_no_extrap() {
        let interpolator = ConstantValueInterp::new(false);
        let x = [1.0, 2.0, 3.0];
        let y = [2.0, 4.0, 6.0];

        let y_out = interpolator
            .interp1d(x.as_slice(), y.as_slice(), 1.25)
            .unwrap();
        assert_abs_diff_eq!(y_out, 2.0);
    }

    #[test]
    fn test_constant_value_with_extrap() {
        let interpolator = ConstantValueInterp::new(true);
        let x = [1.0, 2.0, 3.0];
        let y = [2.0, 4.0, 6.0];

        // Ensure the correct value for an in-bounds value
        let y_out = interpolator
            .interp1d(x.as_slice(), y.as_slice(), 1.25)
            .unwrap();
        assert_abs_diff_eq!(y_out, 2.0);

        let y_out = interpolator
            .interp1d(x.as_slice(), y.as_slice(), 10.0)
            .unwrap();
        assert_abs_diff_eq!(y_out, 6.0);
    }

    #[test]
    fn test_constant_value_time_no_extrap() {
        let interpolator = ConstantValueInterp::new(false);
        let t = make_test_datetimes();
        let y = [2.0, 4.0, 6.0];

        let t_out = NaiveDateTime::parse_from_str("2023-08-26 09:04", "%Y-%m-%d %H:%M")
            .unwrap()
            .and_local_timezone(Utc)
            .unwrap();

        let y_out = interpolator.interp1d_to_time(&t, &y, t_out).unwrap();
        assert_abs_diff_eq!(y_out, 4.0);

        // Confirm that out-of-bounds times give an error like out-of-bounds values
        let t_out = NaiveDateTime::parse_from_str("2023-08-26 09:11", "%Y-%m-%d %H:%M")
            .unwrap()
            .and_local_timezone(Utc)
            .unwrap();
        interpolator.interp1d_to_time(&t, &y, t_out).unwrap_err();
    }

    #[test]
    fn test_constant_value_time_with_extrap() {
        let interpolator = ConstantValueInterp::new(true);
        let t = make_test_datetimes();
        let y = [2.0, 4.0, 6.0];

        let t_out = NaiveDateTime::parse_from_str("2023-08-26 09:04", "%Y-%m-%d %H:%M")
            .unwrap()
            .and_local_timezone(Utc)
            .unwrap();

        let y_out = interpolator.interp1d_to_time(&t, &y, t_out).unwrap();
        assert_abs_diff_eq!(y_out, 4.0);

        // Confirm that out-of-bounds times give an error like out-of-bounds values
        let t_out = NaiveDateTime::parse_from_str("2023-08-26 09:11", "%Y-%m-%d %H:%M")
            .unwrap()
            .and_local_timezone(Utc)
            .unwrap();

        let y_out = interpolator.interp1d_to_time(&t, &y, t_out).unwrap();
        assert_abs_diff_eq!(y_out, 6.0);
    }

    fn make_test_datetimes() -> [DateTime<Utc>; 3] {
        let fmt = "%Y-%m-%d %H:%M";
        [
            NaiveDateTime::parse_from_str("2023-08-26 09:00", fmt)
                .unwrap()
                .and_local_timezone(Utc)
                .unwrap(),
            NaiveDateTime::parse_from_str("2023-08-26 09:05", fmt)
                .unwrap()
                .and_local_timezone(Utc)
                .unwrap(),
            NaiveDateTime::parse_from_str("2023-08-26 09:10", fmt)
                .unwrap()
                .and_local_timezone(Utc)
                .unwrap(),
        ]
    }
}
