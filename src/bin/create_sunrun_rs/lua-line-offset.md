# Mapping Lua error line numbers back to the sunrun TOML

## Problem

When a Lua snippet in a site's `xx_sunrun.toml` (the `init_lua`, `lua`, or
`condition` fields) fails at runtime, the error/traceback line numbers currently
point at Rust code, e.g.:

```
runtime error: src/bin/create_sunrun_rs/site_config.rs:329:5: attempt to perform arithmetic on a nil value (local 'mm')
stack traceback:
    [C]: in metamethod 'div'
    src/bin/create_sunrun_rs/site_config.rs:329:5: in global 'julian'
    src/bin/create_sunrun_rs/site_config.rs:329:37: in global 'correct_pres'
    src/bin/create_sunrun_rs/site_config.rs:337:1: in main chunk
```

Anatomy of each frame: `<chunk-name>:<lua-line>`.

- `...site_config.rs:329` / `...site_config.rs:337` are the **chunk names**.
  When `set_name` is not called, `mlua` defaults the chunk name to the Rust
  caller location (`file:line` of the `.load(...)` call). That is why the base
  number looks absurd (`init_str` has nothing near 329 lines).
- The **second** number (`5`, `37`, `1`) is the real Lua line *within the
  chunk*.

Goal: make the reported line numbers match the line numbers in the source TOML
file, so users can jump straight to the offending line.

## Why there is no config flag

Lua's `load` has no "starting line" parameter, and `mlua` exposes no line-offset
option on `Chunk`. The standard workaround is to **prepend blank lines** to the
Lua source: Lua counts lines from the first byte of the chunk, so prepending
*N* newlines makes every reported line become `N + lua_line`. Choose *N* so that
Lua line 1 lands on the TOML line where the block's content begins. Pair this
with `Chunk::set_name(<toml filename>)` so the prefix reads like the file too.

## Implementation sketch

Versions in use: `mlua = 0.11` (`set_name` is a builder returning `Self`, not a
`Result`), `toml = 0.8` (supports `toml::Spanned`).

### 1. Capture the span of each Lua field

Wrap the Lua string fields in `toml::Spanned<String>` so deserialization records
the byte range of the value in the source document:

```rust
use toml::Spanned;

#[serde(default)]
pub(crate) init_lua: Option<Spanned<String>>,
#[serde(default)]
pub(crate) lua: Option<Spanned<String>>,
// and `condition` on ModRowTarget::LuaCondition if we want its errors mapped too
```

`spanned.span()` returns a `Range<usize>` of byte offsets into the original TOML
text, and `spanned.get_ref()` / `into_inner()` yields the string. This means
`SiteConfig` deserialization must **retain the raw TOML text** (keep the `&str`
passed to `toml::from_str`) and the **path** to the TOML file, threading both
down to where the Lua is loaded (`do_lua`, `applies_to_row`) — either as extra
args or stored on `SiteConfig`.

### 2. Compute the content-start line

The span starts at the opening delimiter, so skip past `"`, `"""`, `'`, or `'''`
(and the single leading newline that TOML trims after `"""`/`'''`), then count
newlines up to the content start. The newline count equals the number of blank
lines to prepend (Lua line 1 then maps onto that TOML line).

```rust
/// Number of blank lines to prepend so Lua line 1 maps onto the TOML file line.
fn offset_for_span(src: &str, span: std::ops::Range<usize>) -> usize {
    let after_delim = &src[span.start..];
    let content_start = if let Some(rest) = after_delim
        .strip_prefix("\"\"\"")
        .or_else(|| after_delim.strip_prefix("'''"))
    {
        // triple-quoted: TOML trims one immediately-following newline
        let trimmed = rest
            .strip_prefix("\r\n")
            .or_else(|| rest.strip_prefix('\n'))
            .unwrap_or(rest);
        span.start
            + (rest.as_ptr() as usize - after_delim.as_ptr() as usize)
            + (rest.len() - trimmed.len())
    } else {
        span.start + 1 // single quote / apostrophe
    };
    src[..content_start].bytes().filter(|&b| b == b'\n').count()
}
```

### 3. Prepend and name the chunk

```rust
let n = offset_for_span(raw_toml, spanned.span());
let padded = format!("{}{}", "\n".repeat(n), spanned.get_ref());

lua_engine
    .load(&padded)
    .set_name(toml_path.display().to_string()) // e.g. "pa_sunrun.toml"
    .exec()
    .map_err(|e| {
        // NB: print the ORIGINAL code (spanned.get_ref()), not `padded`,
        // so the error body does not show leading blank lines.
        GggError::custom(format!(
            "Error ...\nCode was:\n\n{}\n\nError was:\n\n{e}",
            spanned.get_ref()
        ))
    })?;
```

Result: errors read like `pa_sunrun.toml:47: attempt to perform arithmetic on a
nil value`, matching the file.

## Sites to update

All three `lua_engine.load(...)` call sites in
`src/bin/create_sunrun_rs/site_config.rs`:

- `ModRowValues::do_lua` — the `init_lua` load (currently ~line 329).
- `ModRowValues::do_lua` — the `lua` load (currently ~line 337).
- `ModRowTarget::applies_to_row` — the `LuaCondition { condition }` load
  (currently ~line 201).

## Notes / caveats

- The prepended newlines only affect the copy handed to Lua; keep the unpadded
  original for display in error messages.
- `set_name` respects Lua's `LUA_IDSIZE` limit (~60 chars) and will truncate a
  long path in messages — prefer the file name (or a short relative path) over a
  full absolute path.
- Tests in the `tests` module build `ModRowValues`/`ModRowTarget` directly with
  plain `String`s (no TOML source). Switching fields to `Spanned<String>` means
  either updating `ModRowValues::new` and the test constructors, or providing a
  synthetic span (offset 0) for the no-file case so line numbers still work
  (they'd just be relative to the snippet).
