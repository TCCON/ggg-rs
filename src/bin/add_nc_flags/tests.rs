use super::FilterSet;
use std::path::PathBuf;

#[test]
fn test_book_examples() {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let md_file = crate_root.join("book/src/postproc/add_nc_flags_toml.md");
    let block_iter = ggg_rs::test_utils::iter_fenced_blocks("toml", [md_file]);
    for block in block_iter {
        let block = block.expect("should be able to read fenced block");
        let res: Result<FilterSet, _> = toml::from_str(&block.text);
        assert!(
            res.is_ok(),
            "could not deserialize an example in line {} of file {}:\n\n{}\n\nerror was\n\n{}",
            block.line,
            block.file.display(),
            block.text,
            res.unwrap_err()
        );
    }
}
