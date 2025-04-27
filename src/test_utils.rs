use std::{
    io::{BufRead, BufReader, Lines},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

#[allow(dead_code)]
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

    let is_same = child
        .wait()
        .expect("Waiting for diff process should not fail")
        .success();
    assert!(is_same, "{out_file_name} did not match expected.");
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

pub struct FencedBlocks<P, F>
where
    P: AsRef<Path>,
    F: Iterator<Item = P>,
{
    fence_start: String,
    files: F,
    lines: Option<Lines<BufReader<std::fs::File>>>,
    line_num: usize,
}

impl<P, F> Iterator for FencedBlocks<P, F>
where
    P: AsRef<Path>,
    F: Iterator<Item = P>,
{
    type Item = std::io::Result<String>;

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
                if line.starts_with(&self.fence_start) {
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
        self.line_num = 0;
        Some(Ok(()))
    }

    fn get_block(&mut self, starting_line: usize) -> std::io::Result<String> {
        let mut block = String::new();
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
                return Ok(block);
            }

            if !block.is_empty() {
                // lines() doesn't return newlines, so add them back in for all but the last line
                block.push('\n');
            }
            block.push_str(&next_line);
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
            assert_eq!(s.unwrap(), exp);
        }
    }
}
