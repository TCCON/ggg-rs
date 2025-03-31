use std::{path::{Path, PathBuf}, process::{Command, Stdio}};

pub(crate) fn test_data_dir() -> PathBuf {
    let crate_root = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(crate_root).join("test-data")
}

pub fn compare_output_text_files(expected_dir: &Path, output_dir: &Path, out_file_name: &str) {
    let mut child = Command::new("diff")
        .arg("-q")
        .arg(expected_dir.join(out_file_name))
        .arg(output_dir.join(out_file_name))
        .stdout(Stdio::null())
        .spawn()
        .expect("Spawning diff process should not fail");

    let is_same = child.wait()
        .expect("Waiting for diff process should not fail")
        .success();
    assert!(is_same, "{out_file_name} did not match expected.");
}
