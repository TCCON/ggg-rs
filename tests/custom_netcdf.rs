use ggg_rs_derive::{ReadNetcdf, WriteNetcdf};

/// Custom write: split `(lat, lon)` pairs into two variables named from the
/// netCDF name, and record the Rust field name so the test can tell the two
/// name arguments apart.
fn write_coords(
    _data: &Located,
    coords: &[(f64, f64)],
    field_name: &str,
    nc_name: &str,
    grp: &mut netcdf::GroupMut<'_>,
) -> netcdf::Result<()> {
    let dim = format!("{nc_name}_n");
    grp.add_dimension(&dim, coords.len())?;
    let lats: Vec<f64> = coords.iter().map(|c| c.0).collect();
    let lons: Vec<f64> = coords.iter().map(|c| c.1).collect();
    let mut lat = grp.add_variable::<f64>(&format!("{nc_name}_lat"), &[&dim])?;
    lat.put_values(&lats, ..)?;
    lat.put_attribute("field_name", field_name)?;
    let mut lon = grp.add_variable::<f64>(&format!("{nc_name}_lon"), &[&dim])?;
    lon.put_values(&lons, ..)?;
    Ok(())
}

/// Custom read: the inverse of [`write_coords`]. Errors if the stored field
/// name does not match, so a swapped `field_name`/`nc_name` would fail.
fn read_coords(
    field_name: &str,
    nc_name: &str,
    grp: &netcdf::Group<'_>,
) -> netcdf::Result<Vec<(f64, f64)>> {
    let get = |suffix: &str| {
        let name = format!("{nc_name}_{suffix}");
        grp.variable(&name)
            .ok_or_else(|| netcdf::Error::Str(format!("custom read: missing variable '{name}'")))
    };
    let lat = get("lat")?;
    let lon = get("lon")?;
    let stored: String = lat
        .attribute_value("field_name")
        .ok_or_else(|| netcdf::Error::Str("custom read: no field_name attribute".to_string()))??
        .try_into()?;
    if stored != field_name {
        return Err(netcdf::Error::Str(format!(
            "custom read: stored field name '{stored}' != '{field_name}'"
        )));
    }
    let lats = lat.get_values::<f64, _>(..)?;
    let lons = lon.get_values::<f64, _>(..)?;
    Ok(lats.into_iter().zip(lons).collect())
}

// The container-level `describe` and `unpack` must pass over the custom field
// rather than reject it.
#[derive(Debug, PartialEq, ReadNetcdf, WriteNetcdf)]
#[netcdf(describe, unpack)]
struct Located {
    /// Observation locations.
    #[netcdf(rename = "site", custom(write = write_coords, read = read_coords))]
    coords: Vec<(f64, f64)>,

    /// An ordinary field alongside the custom one.
    #[netcdf(dimensions("time"))]
    xco2: Vec<f64>,
}

fn sample() -> Located {
    Located {
        coords: vec![(45.0, -90.0), (34.5, -118.25)],
        xco2: vec![400.0, 401.0, 402.0],
    }
}

fn write(data: &Located, path: &std::path::Path) {
    let mut file = netcdf::create(path).unwrap();
    let mut root = file.root_mut().unwrap();
    data.write_netcdf(&mut root).unwrap();
}

#[test]
fn test_custom_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("custom.nc");
    let data = sample();
    write(&data, &path);

    let file = netcdf::open(&path).unwrap();
    let read = Located::read_netcdf(&file.root().unwrap()).unwrap();
    assert_eq!(data, read);
}

/// The custom write function, not a derived helper, produced the variables:
/// it received the renamed netCDF name and the Rust field name separately.
#[test]
fn test_custom_write_layout() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("custom.nc");
    write(&sample(), &path);

    let file = netcdf::open(&path).unwrap();
    assert!(file.variable("site").is_none());
    assert!(file.variable("coords").is_none());
    let lat = file.variable("site_lat").expect("site_lat variable");
    assert_eq!(vec![45.0, 34.5], lat.get_values::<f64, _>(..).unwrap());
    let lon = file.variable("site_lon").expect("site_lon variable");
    assert_eq!(vec![-90.0, -118.25], lon.get_values::<f64, _>(..).unwrap());
    let stored: String = lat
        .attribute_value("field_name")
        .unwrap()
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!("coords", stored);
    // The container `describe` did not try to attach to the custom field.
    assert!(lat.attribute("description").is_none());
    // ...but still applies to the ordinary one.
    assert!(
        file.variable("xco2")
            .unwrap()
            .attribute("description")
            .is_some()
    );
}

/// An error from the custom read function propagates out of `read_netcdf`.
#[test]
fn test_custom_read_error_propagates() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("custom.nc");
    {
        let mut file = netcdf::create(&path).unwrap();
        let mut root = file.root_mut().unwrap();
        root.add_dimension("time", 1).unwrap();
        let mut v = root.add_variable::<f64>("xco2", &["time"]).unwrap();
        v.put_values(&[400.0], ..).unwrap();
    }

    let file = netcdf::open(&path).unwrap();
    let err = Located::read_netcdf(&file.root().unwrap()).expect_err("missing site vars");
    let msg = err.to_string();
    assert!(
        msg.contains("custom read: missing variable 'site_lat'"),
        "unexpected error message: {msg}"
    );
}
