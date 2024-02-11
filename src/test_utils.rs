use std::path::PathBuf;

pub(crate) fn test_data_dir() -> PathBuf {
    PathBuf::from(file!())
        .parent().unwrap()
        .parent().unwrap()
        .join("test-data")
}
