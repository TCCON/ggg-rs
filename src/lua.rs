use std::{collections::HashMap, path::PathBuf, sync::Arc};

use mlua::{Lua, LuaSerdeExt};
use serde::{Deserialize, Serialize};

use crate::utils::{get_ggg_path, GggError};

pub fn setup_ggg_lua() -> error_stack::Result<Lua, GggError> {
    let lua_engine = Lua::new();

    setup_ggg_lua_globals(&lua_engine)?;
    setup_ggg_lua_functions(&lua_engine)
        .map_err(|e| GggError::lua("Error setting up GGG Lua functions", e))?;
    Ok(lua_engine)
}

fn setup_ggg_lua_globals(lua_engine: &Lua) -> error_stack::Result<(), GggError> {
    let globals = lua_engine.globals();
    let gggpath = get_ggg_path()?;
    globals
        .set("GGGPATH", gggpath)
        .map_err(|e| GggError::lua("Error setting GGGPATH variable in Lua interpreter", e))?;
    Ok(())
}

fn setup_ggg_lua_functions(lua_engine: &Lua) -> mlua::Result<()> {
    // Claude offered two ways to load "builtin" functions -
    // one used package.preload, the other directly assigns
    // to the "loaded" table. The difference is when the table
    // is constructed, and Claude suggests that for simple builtins,
    // going directly to loaded is unlikely to add much overhead.
    // I agree, but there isn't much downside to implementing
    // the lazy loading approach, it's just more verbose.
    let package: mlua::Table = lua_engine.globals().get("package")?;
    let preload: mlua::Table = package.get("preload")?;
    // This makes it so that `local gggutils = require("ggg")` will call
    // `ggg_lua_module_table` the first time that `require` is called,
    // and cache the table of functions returned by `ggg_lua_module_table`.
    preload.set(
        "ggg",
        lua_engine.create_function(|lua, ()| ggg_lua_module_table(&lua))?,
    )?;
    Ok(())
}

fn ggg_lua_module_table(lua_engine: &Lua) -> mlua::Result<mlua::Table> {
    let m = lua_engine.create_table()?;
    m.set("read_csv", lua_engine.create_function(read_csv_lua)?)?;
    m.set("read_json", lua_engine.create_function(read_json_lua)?)?;
    Ok(m)
}

fn read_csv_lua(
    lua_engine: &Lua,
    (csv_file, delimiter, has_headers): (PathBuf, Option<u8>, Option<bool>),
) -> mlua::Result<mlua::Value> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(has_headers.unwrap_or(true))
        .delimiter(delimiter.unwrap_or(b','))
        .from_path(csv_file)
        .map_err(|e| mlua::Error::ExternalError(Arc::new(e)))?;

    let mut data = vec![];
    for res in rdr.deserialize() {
        let row: HashMap<String, CsvValue> =
            res.map_err(|e| mlua::Error::ExternalError(Arc::new(e)))?;
        data.push(row);
    }
    lua_engine.to_value(&data)
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
enum CsvValue {
    Integer(i64),
    Float(f64),
    String(String),
}

fn read_json_lua(lua_engine: &Lua, (json_file,): (String,)) -> mlua::Result<mlua::Value> {
    let f = std::fs::File::open(json_file).unwrap();
    let val: serde_json::Value = serde_json::from_reader(f).unwrap();
    lua_engine.to_value(&val)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::test_utils::test_data_dir;

    #[test]
    fn test_lua_globals() {
        let lua = Lua::new();
        let expected_gggpath = get_ggg_path().expect("getting GGGPATH should succeed");
        setup_ggg_lua_globals(&lua).expect("Setting globals should succeed");
        let lua_gggpath: String = lua
            .load("GGGPATH")
            .eval()
            .expect("executing Lua code should succeed");
        assert_eq!(lua_gggpath, expected_gggpath);
    }

    #[test]
    fn test_lua_csv() {
        let csv_file = test_data_dir()
            .join("inputs")
            .join("lua")
            .join("simple.csv");
        let lua = Lua::new();
        setup_ggg_lua_functions(&lua).expect("set up of Lua functions should work");
        lua.globals()
            .set("filepath", csv_file)
            .expect("setting filepath variable should work");
        let code = r#"local ggg = require("ggg")
        local csv = ggg.read_csv(filepath)
        return csv"#;
        let lua_value = lua
            .load(code)
            .eval()
            .expect("executing Lua code should succeed");
        let lua_data: Vec<HashMap<String, CsvValue>> = lua
            .from_value(lua_value)
            .expect("conversion to a Rust value should succeed");
        let expected_data: Vec<HashMap<String, CsvValue>> = vec![
            HashMap::from_iter([
                ("site".to_string(), CsvValue::String("pa".to_string())),
                ("id_num".to_string(), CsvValue::Integer(1)),
                ("lat".to_string(), CsvValue::Float(45.5)),
                ("lon".to_string(), CsvValue::Float(-90.1)),
            ]),
            HashMap::from_iter([
                ("site".to_string(), CsvValue::String("oc".to_string())),
                ("id_num".to_string(), CsvValue::Integer(2)),
                ("lat".to_string(), CsvValue::Float(36.6)),
                ("lon".to_string(), CsvValue::Float(-105.2)),
            ]),
        ];
        assert_eq!(lua_data, expected_data);
    }

    #[test]
    fn test_lua_edit_csv() {
        let csv_file = test_data_dir()
            .join("inputs")
            .join("lua")
            .join("simple.csv");
        let lua = Lua::new();
        setup_ggg_lua_functions(&lua).expect("set up of Lua functions should work");
        lua.globals()
            .set("filepath", csv_file)
            .expect("setting filepath variable should work");
        let code = r#"local ggg = require("ggg")
        local csv = ggg.read_csv(filepath)
        csv[1]["id_num"] = 0
        csv[1]["lat"] = -10.0
        csv[1]["lon"] = 20.0
        csv[2]["site"] = "df"
        csv[2]["id_num"] = csv[2]["id_num"] + 10
        return csv"#;
        let lua_value = lua
            .load(code)
            .eval()
            .expect("executing Lua code should succeed");
        let lua_data: Vec<HashMap<String, CsvValue>> = lua
            .from_value(lua_value)
            .expect("conversion to a Rust value should succeed");
        let expected_data: Vec<HashMap<String, CsvValue>> = vec![
            HashMap::from_iter([
                ("site".to_string(), CsvValue::String("pa".to_string())),
                ("id_num".to_string(), CsvValue::Integer(0)),
                ("lat".to_string(), CsvValue::Float(-10.0)),
                ("lon".to_string(), CsvValue::Float(20.0)),
            ]),
            HashMap::from_iter([
                ("site".to_string(), CsvValue::String("df".to_string())),
                ("id_num".to_string(), CsvValue::Integer(12)),
                ("lat".to_string(), CsvValue::Float(36.6)),
                ("lon".to_string(), CsvValue::Float(-105.2)),
            ]),
        ];
        assert_eq!(lua_data, expected_data);
    }

    #[test]
    fn test_lua_json() {
        let json_file = test_data_dir()
            .join("inputs")
            .join("lua")
            .join("simple.json");
        let lua = Lua::new();
        setup_ggg_lua_functions(&lua).expect("set up of Lua functions should work");
        lua.globals()
            .set("filepath", json_file)
            .expect("setting filepath variable should work");
        let code = r#"local ggg = require("ggg")
        local data = ggg.read_json(filepath)
        return data"#;
        let lua_value = lua
            .load(code)
            .eval()
            .expect("executing Lua code should succeed");
        let lua_data: serde_json::Value = lua
            .from_value(lua_value)
            .expect("conversion to a Rust value should succeed");
        let expected_data = serde_json::json!({
            "date": ["2026-01-01", "2026-01-02"],
            "pres": [1013.25, 1000.0],
            "met_sn": 12345
        });
        assert_eq!(lua_data, expected_data);
    }
}
