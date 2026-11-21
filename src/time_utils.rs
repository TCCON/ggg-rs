use std::f32::consts::PI;

use chrono::{DateTime, Datelike, NaiveDateTime, Utc};
use uom::si::f32::Angle;

/// Equation of time using a 1-based day of year.
/// Adapted from Python's pvlib: https://pvlib-python.readthedocs.io/en/stable/:
///     Copyright (c) 2023 pvlib python Contributors
///     Copyright (c) 2014 PVLIB python Development Team
///     Copyright (c) 2013 Sandia National Laboratories
///     All rights reserved.
fn equation_of_time_spencer71(dayofyear: f32) -> f32 {
    let day_angle = (2.0 * PI / 365.0) * (dayofyear - 1.0);
    let eot = (1440.0 / 2.0 / PI)
        * (0.0000075 + 0.001868 * day_angle.cos()
            - 0.032077 * day_angle.sin()
            - 0.014615 * (2.0 * day_angle).cos()
            - 0.040849 * (2.0 * day_angle).sin());
    eot
}

/// Compute solar time for a given UTC time and longitude.
pub fn solar_apparent_time(lon: Angle, utc_time: DateTime<Utc>) -> NaiveDateTime {
    // This is different than the original Matlab code Debra found; looking at plots, it seems
    // like the matlab code has a phase shift compared to Wikipedia (https://en.wikipedia.org/wiki/Equation_of_time).
    // Both versions appear to use the sign convention of sun - clock, so that adding the EoT
    // to the clock time to get the solar time is correct.
    let eq_of_time = equation_of_time_spencer71(utc_time.ordinal() as f32);
    let lon_deg = lon.get::<uom::si::angle::degree>();
    let solar_days_offset = lon_deg * 4.0 / 1440.0 + eq_of_time / 1440.0;
    let solar_time = utc_time.naive_utc()
        + chrono::Duration::nanoseconds((solar_days_offset * 86_400.0 * 1_000_000_000.0) as i64);
    solar_time
}
