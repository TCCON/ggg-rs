//! Common errors across the ggg-rs crate

/// Errors related to working with datetimes
#[derive(Debug, thiserror::Error)]
pub enum DateTimeError {
    #[error("Year {0}, month {1}, day {2} is not a valid date")]
    InvalidYearMonthDay(i32, u32, u32),
    #[error("Year {year} month {month} does not have {n} {weekday}s")]
    NoNthWeekday{year: i32, month: u32, n: u8, weekday: chrono::Weekday},
    #[error("{0} falls in the repeated hour of the DST -> standard transition, cannot determine the timezone")]
    AmbiguousDst(chrono::NaiveDateTime),
    #[error("Error adding timezone to naive datetime: {0}")]
    InvalidTimezone(String),
}