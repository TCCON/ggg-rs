//! Directional `skip`: `skip(read)` writes a field but leaves it at `Default`
//! on read, `skip(write)` reads a field but never writes it.

use ggg_rs_derive::{ReadNetcdf, WriteNetcdf};

// ---------------------------------------------------------------------------
// skip(read): a write-only placeholder
// ---------------------------------------------------------------------------

/// Writes `xco2` a second time, in ppb. The placeholder field itself holds
/// nothing; the data comes from the struct.
fn to_ppb(data: &Placeholder, _unit: &(), _field: &str, _nc: &str) -> Vec<f64> {
    data.xco2.iter().map(|v| v * 1000.0).collect()
}

#[derive(Debug, PartialEq, ReadNetcdf, WriteNetcdf)]
struct Placeholder {
    #[netcdf(dimensions("time"))]
    xco2: Vec<f64>,

    // `map` has no `read = ...`, which `ReadNetcdf` would otherwise reject.
    #[netcdf(
        skip(read),
        dimensions("time"),
        kind = "values",
        map(write = to_ppb)
    )]
    xco2_ppb: (),
}

#[test]
fn test_skip_read_is_written_but_not_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("placeholder.nc");
    let data = Placeholder {
        xco2: vec![0.4, 0.41],
        xco2_ppb: (),
    };
    {
        let mut file = netcdf::create(&path).unwrap();
        data.write_netcdf(&mut file.root_mut().unwrap()).unwrap();
    }

    let file = netcdf::open(&path).unwrap();
    let ppb: Vec<f64> = file.variable("xco2_ppb").unwrap().get_values(..).unwrap();
    assert_eq!(ppb, vec![400.0, 410.0]);
    assert_eq!(
        Placeholder::read_netcdf(&file.root().unwrap()).unwrap(),
        data
    );
}

/// `skip(read)` must leave the field at `Default` even when the file has a
/// matching variable, so a non-trivial `Default` shows it was not read.
#[derive(Debug, PartialEq, WriteNetcdf)]
struct HasNotes {
    #[netcdf(dimensions("time"))]
    notes: Vec<i32>,
}

#[derive(Debug, PartialEq, ReadNetcdf)]
struct IgnoresNotes {
    #[netcdf(skip(read), dimensions("time"))]
    notes: Vec<i32>,
}

#[test]
fn test_skip_read_ignores_existing_variable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("notes.nc");
    {
        let mut file = netcdf::create(&path).unwrap();
        HasNotes {
            notes: vec![1, 2, 3],
        }
        .write_netcdf(&mut file.root_mut().unwrap())
        .unwrap();
    }
    let file = netcdf::open(&path).unwrap();
    let read = IgnoresNotes::read_netcdf(&file.root().unwrap()).unwrap();
    assert_eq!(read, IgnoresNotes { notes: vec![] });
}

// ---------------------------------------------------------------------------
// skip(write): read but never written
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, ReadNetcdf, WriteNetcdf)]
struct ReadOnly {
    #[netcdf(dimensions("time"))]
    xco2: Vec<f64>,

    #[netcdf(skip(write), dimensions("time"))]
    flag: Vec<i32>,
}

#[derive(WriteNetcdf)]
struct Source {
    #[netcdf(dimensions("time"))]
    xco2: Vec<f64>,
    #[netcdf(dimensions("time"))]
    flag: Vec<i32>,
}

#[test]
fn test_skip_write_is_not_written() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("read_only_out.nc");
    {
        let mut file = netcdf::create(&path).unwrap();
        ReadOnly {
            xco2: vec![1.0, 2.0],
            flag: vec![7, 8],
        }
        .write_netcdf(&mut file.root_mut().unwrap())
        .unwrap();
    }
    let file = netcdf::open(&path).unwrap();
    assert!(file.variable("xco2").is_some());
    assert!(file.variable("flag").is_none());
}

#[test]
fn test_skip_write_is_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("read_only_in.nc");
    {
        let mut file = netcdf::create(&path).unwrap();
        Source {
            xco2: vec![1.0, 2.0],
            flag: vec![7, 8],
        }
        .write_netcdf(&mut file.root_mut().unwrap())
        .unwrap();
    }
    let file = netcdf::open(&path).unwrap();
    let read = ReadOnly::read_netcdf(&file.root().unwrap()).unwrap();
    assert_eq!(
        read,
        ReadOnly {
            xco2: vec![1.0, 2.0],
            flag: vec![7, 8],
        }
    );
}

// ---------------------------------------------------------------------------
// skip(read, write): the same as bare `skip`
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, ReadNetcdf, WriteNetcdf)]
struct BothWays {
    #[netcdf(dimensions("time"))]
    xco2: Vec<f64>,
    #[netcdf(skip(read, write))]
    cache: Vec<u8>,
}

#[test]
fn test_skip_read_write_skips_both() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("both.nc");
    {
        let mut file = netcdf::create(&path).unwrap();
        BothWays {
            xco2: vec![1.0],
            cache: vec![1, 2],
        }
        .write_netcdf(&mut file.root_mut().unwrap())
        .unwrap();
    }
    let file = netcdf::open(&path).unwrap();
    assert!(file.variable("cache").is_none());
    let read = BothWays::read_netcdf(&file.root().unwrap()).unwrap();
    assert_eq!(
        read,
        BothWays {
            xco2: vec![1.0],
            cache: vec![],
        }
    );
}

// ---------------------------------------------------------------------------
// One-sided skip with the other direction's `custom`
// ---------------------------------------------------------------------------

fn write_answer(
    _data: &CustomWriteOnly,
    value: &i32,
    _field: &str,
    nc: &str,
    grp: &mut netcdf::GroupMut<'_>,
) -> netcdf::Result<()> {
    grp.add_attribute(nc, *value * 2)?;
    Ok(())
}

#[derive(Debug, PartialEq, ReadNetcdf, WriteNetcdf)]
struct CustomWriteOnly {
    #[netcdf(dimensions("time"))]
    xco2: Vec<f64>,
    #[netcdf(skip(read), custom(write = write_answer))]
    answer: i32,
}

#[test]
fn test_skip_read_with_custom_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("custom.nc");
    let data = CustomWriteOnly {
        xco2: vec![1.0],
        answer: 21,
    };
    {
        let mut file = netcdf::create(&path).unwrap();
        data.write_netcdf(&mut file.root_mut().unwrap()).unwrap();
    }
    let file = netcdf::open(&path).unwrap();
    let attr = file.attribute("answer").unwrap().value().unwrap();
    assert_eq!(attr, netcdf::AttributeValue::Int(42));
    let read = CustomWriteOnly::read_netcdf(&file.root().unwrap()).unwrap();
    assert_eq!(
        read,
        CustomWriteOnly {
            xco2: vec![1.0],
            answer: 0,
        }
    );
}
