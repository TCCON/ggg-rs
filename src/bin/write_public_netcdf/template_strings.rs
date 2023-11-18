//! Transform variable names to include in attribute strings.
//! 
//! When specifying public variable names and properties in the config file,
//! it can be helpful to be able to include the private variable
//! name in the string. This cuts down on repetition. To indicate
//! that the private variable name should be substituted in, use
//! `{name}` in the string. For instance, if the private variable
//! was `xco2`, then the string:
//! 
//! ```text
//! "{name} is a column average"
//! ```
//! 
//! would become "xco2 is a column average." Of course, sometimes
//! you don't want to use the variable name exactly as given. If
//! you wanted to add a description to variables like "co2_vsf" and
//! "ch4_vsf" that uses just the specie name, you can do that:
//! 
//! ```text
//! "{trim(name, '', '_vsf')} VMR scale factor"
//! ```
//! 
//! becomes "co2 VMR scale factor" for the `co2_vsf` variable and
//! "ch4 VMR scale factor" for `ch4_vsf`.
//! 
//! Generally, the syntax to use is `{IDENTIFIER}` when you want to
//! insert a variable name as is and `{FUNCTION(IDENTIFIED, ARG1, ARG2, ...)}`
//! when we want to apply a transform. The available identifiers are:
//! 
//! - `name`: the name of the private variable
//! - `pubname`: the name of the public variable (note that, for obvious reasons,
//!   you can't use this when defining the public variable name).
//! 
//! The available transformation functions are:
//! 
//! - `upper(ID)`: creates an uppercase version of `ID`
//! - `lower(ID)`: creates a lowercase version of `ID`
//! - `trim(ID, START, END)`: removes `START` from the beginning of `ID` if `ID`
//!    begins with it and likewise removes `END` from the end of `ID`. `{trim(name, 'prior_', '_wet')}`
//!    would remove "prior_" from the beginning and "_wet" from the end of the private
//!    variable name. So "prior_co2_wet" would become "co2", but "xco2_dry" is unchanged,
//!    as neither the start nor end match.
//! - `replace(ID, FROM, TO)`: changes any occurrences of the substring `FROM` into `TO`
//!    in the name. For example, `{replace(name, '_1', '')}` would change "prior_1co2" into
//!    "prior_co2" (replacing the "_1" with nothing).
//! - `regex(ID, PATTERN, REP)`: uses the [regex replace](https://docs.rs/regex/latest/regex/struct.Regex.html#method.replace)
//!    method to provide more comprehensive replacement capability. See the [`regex`] documentation
//!    for detailed discussion of replacement. Internally, this creates a regex as
//!    `re = Regex::new(PATTERN)` then calls `re.replace(ID, REP)`. For a simple example,
//!    to extract a species name from e.g. "prior_co2" one could do 
//!    `{regex(name, "prior_([a-z0-9]+)", "$1")}`. (*Note: in the TOML file, you may wish
//!    to use the single quoted literal string to allow backslashes inside the regex pattern.*)
//!    If your regex pattern is invalid, you will get an error while reading the config.
//! - `map(ID, MAP_KEY)`: uses the `mappings` section of your configuration file to map variable
//!    names to arbitrary values. For instance, when defining standard variable names, we should
//!    spell out the names of our retrieved species. We could create a TOML config that include
//!    the section below, then use `{map(name, 'stdnames')}` to convert "xco2" to "carbon_dioxide"
//!    and "xch4" to "methane". The second argument is the key within the `mappings` section of the
//!    configuration. This allows you to map the same variables to different strings depending on the
//!    case.
//! 
//! *Example mappings section:*
//! 
//! ```toml
//! [mappings.stdnames]
//! xco2 = "carbon_dioxide"
//! xch4 = "methane"
//! ```

// TODO: allow nesting/chaining functions e.g. either `upper(trim(name, ...))` or `trim(name,...) | upper`
//  (I like the second one visually, but it might be harder to code.)
// TODO: ensure that we test the strings when the config is loaded so that any errors due to regex or maps happen at a reasonable time
// TODO: allow the value of the thing in the private file to be one of the identifiers OR arbitrary netCDF attributes?
// TODO (maybe): add an `if_missing` transform that returns the first argument if it is available, the second if not. E.g. if
//   called as `if_missing(units, map(name, 'default_units'))` it would use the variable name to figure out the default units
//   only if there were not units in the private file.
use std::collections::HashMap;

use pest::{Parser, iterators::{Pairs, Pair}};
use pest_derive::Parser;
use regex::Regex;

#[derive(Parser)]
#[grammar = "bin/write_public_netcdf/template_strings.pest"]
struct TemplateStringParser;


#[derive(Debug, thiserror::Error)]
pub enum TokenError {
    #[error("expected a {expected} but did not find one")]
    MissingToken{expected: String},
    #[error("at character {index}, expected a {expected}, got something else")]
    WrongToken{expected: String, index: usize},
    #[error("at character {index}, '{got}' is not one of the allowed identifiers")]
    UnknownId{got: String, index: usize},
    #[error("at character {index}, '{got}' is not one of the replacement functions")]
    UnknownFunction{got: String, index: usize},
    #[error("in the replacement at character {index}, the map key '{key}' is not defined in the maps section")]
    UnknownMapKey{key: String, index: usize},
    #[error("at character {0}, cannot use 'name' in this template string")]
    NoPrivate(usize),
    #[error("at character {0}, cannot use 'pubname' in this template string")]
    NoPublic(usize),
    #[error("expected {0} argument(s), only got {}", .0 - 1)]
    MissingArg(usize),
    #[error("extra argument ({value}) at character {index}, replacement function {function} only takes {expected_number}")]
    ExtraArgs{expected_number: usize, index: usize, function: String, value: String},
    #[error("the regex pattern in the replacement at {index} is not valid: {err}")]
    BadRegex{err: regex::Error, index: usize},
}

impl TokenError {
    fn missing_token<S: Into<String>>(expected: S) -> Self {
        Self::MissingToken { expected: expected.into() }
    }

    fn wrong_token<E: Into<String>>(expected: E, span: pest::Span) -> Self {
        Self::WrongToken { expected: expected.into(), index: span.start() }
    }

    fn unknown_id<S: Into<String>>(got: S, span: pest::Span) -> Self {
        Self::UnknownId { got: got.into(), index: span.start() }
    }

    fn unknown_function<S: Into<String>>(got: S, span: pest::Span) -> Self {
        Self::UnknownFunction { got: got.into(), index: span.start() }
    }

    fn unknown_map_key<S: Into<String>>(key: S, span: pest::Span) -> Self {
        Self::UnknownMapKey { key: key.into(), index: span.start() }
    }

    fn no_private(span: pest::Span) -> Self {
        Self::NoPrivate(span.start())
    }

    fn no_public(span: pest::Span) -> Self {
        Self::NoPublic(span.start())
    }

    fn missing_arg(arg_num: usize) -> Self {
        Self::MissingArg(arg_num)
    }

    fn extra_arg<S: Into<String>, V: Into<String>>(expected_number: usize, function: S, arg_value: V, span: pest::Span) -> Self {
        Self::ExtraArgs { expected_number, index: span.start(), function: function.into(), value: arg_value.into() }
    }

    fn bad_regex(err: regex::Error, span: pest::Span) -> Self {
        Self::BadRegex { err, index: span.start() }
    }
}


pub fn apply_template_transformations(
    template_str: &str, 
    private_name: Option<&str>, 
    public_name: Option<&str>,
    all_maps: &HashMap<String, HashMap<String, String>>) -> Result<String, TokenError> {
    let parsed_str = TemplateStringParser::parse(Rule::attribute, template_str).unwrap()
        .next().unwrap(); // getting the attribute rule should never fail

    let mut final_str = String::new();
    for part in parsed_str.into_inner() {
        match part.as_rule() {
            Rule::literal_part => {
                final_str.push_str(part.as_str());
            },
            Rule::replacement => {
                let s = handle_replacement(part.into_inner(), private_name, public_name, all_maps)?;
                final_str.push_str(&s);
            },
            _ => unreachable!()
        }
    }
    Ok(final_str)
}

fn handle_replacement(
    replacement: Pairs<'_, Rule>, 
    private_name: Option<&str>, 
    public_name: Option<&str>, 
    all_maps: &HashMap<String, HashMap<String, String>>
) -> Result<String, TokenError> {
    for part in replacement {
        match part.as_rule() {
            Rule::identifier => {
                return get_id_arg(Some(part), private_name, public_name)
                    .map(|s| s.to_string());
            },
            Rule::function => {
                let mut inner = part.clone().into_inner();
                // We know that the next token must be the function name if this matched,
                // so okay to unwrap.
                let fxn_name = inner.next().unwrap();

                // See the .pest file - the next token will be a function_args, which 
                // contains indivual arg tokens. Those are what we want.
                let args = inner.next()
                    .ok_or_else(|| TokenError::missing_token("function arguments"))?;

                let mut args = if let Rule::function_args = args.as_rule() {
                    args.into_inner()
                } else {
                    return Err(TokenError::wrong_token("function arguments", args.as_span()));
                };
                

                match fxn_name.as_str() {
                    "upper" => {
                        let varname = get_id_arg(args.next(), private_name, public_name)?;
                        check_end_of_args(args.next(), 1, fxn_name.as_str())?;
                        return Ok(varname.to_uppercase())
                    },
                    "lower" => {
                        let varname = get_id_arg(args.next(), private_name, public_name)?;
                        check_end_of_args(args.next(), 1, fxn_name.as_str())?;
                        return Ok(varname.to_lowercase())
                    },
                    "trim" => {
                        let varname = get_id_arg(args.next(), private_name, public_name)?;
                        let start = get_string_arg(args.next(), 1)?;
                        let end = get_string_arg(args.next(), 2)?;
                        check_end_of_args(args.next(), 2, fxn_name.as_str())?;
                        return Ok(transform_trim(varname, start, end).to_string())
                    },
                    "replace" => {
                        let varname = get_id_arg(args.next(), private_name, public_name)?;
                        let from = get_string_arg(args.next(), 1)?;
                        let to = get_string_arg(args.next(), 2)?;
                        check_end_of_args(args.next(), 2, fxn_name.as_str())?;
                        return Ok(transform_replace(varname, from, to))
                    },
                    "regex" => {
                        let varname = get_id_arg(args.next(), private_name, public_name)?;
                        let pattern = get_string_arg(args.next(), 1)?;
                        let rep = get_string_arg(args.next(), 2)?;
                        check_end_of_args(args.next(), 2, fxn_name.as_str())?;
                        let new = transform_regex(varname, pattern, rep)
                            .map_err(|e| TokenError::bad_regex(e, part.as_span()))?;
                        return Ok(new)
                    },
                    "map" => {
                        let varname = get_id_arg(args.next(), private_name, public_name)?;
                        let key = get_string_arg(args.next(), 1)?;
                        check_end_of_args(args.next(), 1, fxn_name.as_str())?;
                        let new = transform_map(varname, key, all_maps)
                            .ok_or_else(|| TokenError::unknown_map_key(key, part.as_span()))?;
                        return Ok(new.to_string())
                    },
                    _ => {
                        return Err(TokenError::unknown_function(fxn_name.as_str(), fxn_name.as_span()))
                    }
                }
            },
            _ => unreachable!()
        }
    }
    unreachable!()
}

fn get_id_arg<'a>(arg: Option<Pair<'_, Rule>>, private_name: Option<&'a str>, public_name: Option<&'a str>) -> Result<&'a str, TokenError> {
    let arg = arg.ok_or_else(|| TokenError::missing_token("identifier"))?;

    match arg.as_rule() {
        Rule::identifier => {},
        _ => return Err(TokenError::wrong_token("identifier (e.g. name or pubname)", arg.as_span()))
    }

    match arg.as_str() {
        "name" => return private_name.ok_or_else(|| TokenError::no_private(arg.as_span()) ),
        "pubname" => return public_name.ok_or_else(|| TokenError::no_public( arg.as_span() ) ),
        _ => return Err(TokenError::unknown_id(arg.as_str(), arg.as_span()))
    }
}

fn get_string_arg<'a>(arg: Option<Pair<'a, Rule>>, arg_num: usize) -> Result<&'a str, TokenError> {
    let arg = arg
        .ok_or_else(|| TokenError::missing_arg(arg_num))?;
    
    let value = if let Rule::arg = arg.as_rule() {
        arg.into_inner().next().unwrap()
    } else {
        return Err(TokenError::wrong_token("function argument", arg.as_span()));
    };

    match value.as_rule() {
        Rule::single_quote_str | Rule::double_quote_str => Ok(value.as_str()),
        _ => Err(TokenError::wrong_token("quoted string", value.as_span()))
    }
}

fn check_end_of_args(arg: Option<Pair<'_, Rule>>, arg_num: usize, function: &str) -> Result<(), TokenError> {
    if let Some(a) = arg {
        Err(TokenError::extra_arg(arg_num, function, a.as_str(), a.as_span()))
    } else {
        Ok(())
    }
}

fn transform_trim<'a>(original: &'a str, start: &str, end: &str) -> &'a str {
    let out = if original.starts_with(start) {
        original.split_at(start.len()).1
    } else {
        original
    };

    let out = if out.ends_with(end) {
        out.split_at(out.len() - end.len()).0
    } else {
        out
    };

    out
}

fn transform_replace(original: &str, from: &str, to: &str) -> String {
    original.replace(from, to)
}

fn transform_regex(original: &str, pattern: &str, rep: &str) -> Result<String, regex::Error> {
    let re = Regex::new(pattern)?;
    Ok(re.replace(original, rep).to_string())
}

fn transform_map<'a>(original: &'a str, map_key: &str, all_maps: &'a HashMap<String, HashMap<String, String>>) -> Option<&'a str> {
    let inner_map = all_maps.get(map_key)?;
    Some(inner_map.get(original).map(|s| s.as_str()).unwrap_or(original))
}




#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parsing_no_replacement() {
        let input = "This is a string with no replacements";
        let s = apply_template_transformations(input, Some(""), None, &HashMap::new()).unwrap();
        assert_eq!(s, input);
    }

    #[test]
    fn test_parsing_wrong_num_args() {
        let err = apply_template_transformations(
            "{upper(name, 'extra')}",
            Some("xco2"),
            None,
            &HashMap::new()
        ).unwrap_err();

        // Can simplify if end up deriving PartialEq on this err type
        if let TokenError::ExtraArgs { expected_number, index, function, value } = err {
            assert_eq!(expected_number, 1);
            assert_eq!(index, 13);
            assert_eq!(&function, "upper");
            assert_eq!(&value, "'extra'");
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_parsing_wrong_arg_type() {
        let err = apply_template_transformations(
            "{upper('oops')}",
            Some("xco2"),
            None,
            &HashMap::new()
        ).unwrap_err();

        if let TokenError::WrongToken { expected, index } = err {
            assert_eq!(&expected, "identifier (e.g. name or pubname)");
            assert_eq!(index, 7);
        }
    }

    #[test]
    fn test_parsing_no_public() {
        let err = apply_template_transformations(
            "{pubname}", 
            Some("xco2"),
            None,
            &HashMap::new()
        ).unwrap_err();

        if let TokenError::NoPublic(i) = err {
            assert_eq!(i, 1);
        }else {
            assert!(false);
        }
    }

    #[test]
    fn test_parsing_simple_replacements() {
        let s = apply_template_transformations(
            "This private name is {name}",
            Some("xco2"),
            None,
            &HashMap::new()
        ).unwrap();
        assert_eq!(s, "This private name is xco2");

        let s = apply_template_transformations(
            "This public name is {pubname}",
            Some(""),
            Some("xch4"),
            &HashMap::new()
        ).unwrap();
        assert_eq!(s, "This public name is xch4");

        let s = apply_template_transformations(
            "This {name} is on X2007",
            Some("xco2"),
            None,
            &HashMap::new()
        ).unwrap();
        assert_eq!(s, "This xco2 is on X2007");
    }

    #[test]
    fn test_parsing_uppercase() {
        let s = apply_template_transformations(
            "{upper(name)} is a column average", 
            Some("xco2"),
            None,
            &HashMap::new()
        ).unwrap();
        assert_eq!(&s, "XCO2 is a column average");
    }

    #[test]
    fn test_parsing_lowercase() {
        let s = apply_template_transformations(
            "{lower(name)} is a column average",
            Some("XCO2"),
            None,
            &HashMap::new()
        ).unwrap();
        assert_eq!(&s, "xco2 is a column average");
    }

    #[test]
    fn test_parsing_trim_replacements() {
        let s = apply_template_transformations(
            "A priori {trim(name,'prior_','')} profile",
            Some("prior_co2"),
            None,
            &HashMap::new()
        ).unwrap();
        assert_eq!(&s, "A priori co2 profile");

        let s = apply_template_transformations(
            "Gas {trim(name,'','_vsf')} scale factor",
            Some("co2_vsf"),
            None,
            &HashMap::new()
        ).unwrap();
        assert_eq!(&s, "Gas co2 scale factor");
    }

    #[test]
    fn test_parsing_regex_replacements() {
        let s = apply_template_transformations(
            "prior_{regex(name, 'prior_1([a-z0-9]+)_wet', '$1')}",
            Some("prior_1co2_wet"),
            None,
            &HashMap::new()
        ).unwrap();
        assert_eq!(&s, "prior_co2");

        let s = apply_template_transformations(
            "Col variable - {regex(name, '([a-z0-9]+_\\d+)_ovc_([a-z0-9]+)', '$2 original vertical column in the $1')} window",
            Some("o2_7885_ovc_h2o"),
            None,
            &HashMap::new(),
        ).unwrap();
        assert_eq!(&s, "Col variable - h2o original vertical column in the o2_7885 window");
    }

    #[test]
    fn test_parsing_map_replacements() {
        let inner_map = HashMap::from_iter([
            ("co2".to_string(), "carbon dioxide".to_string()),
            ("ch4".to_string(), "methane".to_string())
        ]);

        let mapping = HashMap::from_iter([
            ("species".to_string(), inner_map)
        ]);

        let s = apply_template_transformations(
            "column average mole fraction of {map(name, 'species')} in parts per million",
            Some("co2"),
            None,
            &mapping
        ).unwrap();
        assert_eq!(&s, "column average mole fraction of carbon dioxide in parts per million");

        let s = apply_template_transformations(
            "{map(name, 'species')} in parts per billion",
            Some("ch4"),
            None,
            &mapping
        ).unwrap();
        assert_eq!(&s, "methane in parts per billion");
    }

    #[test]
    fn test_transform_trim() {
        let s = transform_trim("xco2", "", "");
        assert_eq!(s, "xco2");
        let s = transform_trim("xco2", "x", "");
        assert_eq!(s, "co2");
        let s = transform_trim("abc_def", "abc_", "");
        assert_eq!(s, "def");
        let s = transform_trim("xco2", "abc_", "");
        assert_eq!(s, "xco2");

        let s = transform_trim("xco2_", "", "");
        assert_eq!(s, "xco2_");
        let s = transform_trim("xco2_", "", "_");
        assert_eq!(s, "xco2");
        let s = transform_trim("xco2_vsf", "", "_vsf");
        assert_eq!(s, "xco2");
        let s = transform_trim("xco2_insb", "", "_");
        assert_eq!(s, "xco2_insb");

        let s = transform_trim("pxco2s", "p", "s");
        assert_eq!(s, "xco2");
        let s = transform_trim("pre_xco2_suf", "pre_", "_suf");
        assert_eq!(s, "xco2");
        let s = transform_trim("a_xco2_b", "p", "s");
        assert_eq!(s, "a_xco2_b");
    }
}