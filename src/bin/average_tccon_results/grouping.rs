use ggg_rs::averaging::grouping::{FrequencyRange, FrequencyWindowGrouper};

pub(crate) fn default_tccon_grouper() -> FrequencyWindowGrouper {
    let ranges = vec![
        FrequencyRange::new(1800.0, 4000.0, Some("mir")),
        FrequencyRange::new::<&str>(4000.0, 11000.0, None),
        FrequencyRange::new(11000.0, 15000.0, Some("vis")),
    ];
    FrequencyWindowGrouper::new(ranges)
}
