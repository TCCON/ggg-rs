use ncdfcat_lib::{
    attributes::{AttrMergeFirst, AttrMergeLast, AttrMergeStrat},
    data_editors::{DataEditor, EditorInfo},
    dimensions::{DimMergeConcat, DimMergeGrow},
    error::ConcatError,
    Concatenator,
};
use ndarray::ArrayD;

pub(crate) fn setup_concat() -> Concatenator {
    let mut concatenator = Concatenator::default();

    concatenator.set_default_root_attr_strat(AttrMergeFirst::new_boxed());
    concatenator.set_default_root_var_attr_strat(AttrMergeLast::new_boxed());

    concatenator.add_dim_merge_strat("time", DimMergeConcat::new_boxed());
    concatenator.add_dim_merge_strat("prior_time", DimMergeConcat::new_boxed());
    concatenator.add_dim_merge_strat("daily_error_date", DimMergeConcat::new_boxed());
    concatenator.add_dim_merge_strat("specname", DimMergeGrow::new_boxed());

    concatenator.add_root_var_common_attr_strat("units", CFUnitsResolver::new_boxed());

    concatenator.add_data_editor("prior_index", PriorIndexEditor::new_boxed());

    concatenator
}

struct CFUnitsResolver;

impl CFUnitsResolver {
    fn new_boxed() -> Box<dyn AttrMergeStrat> {
        Box::new(Self)
    }
}

impl AttrMergeStrat for CFUnitsResolver {
    fn resolve_conflict(
        &self,
        curr_val: &netcdf::AttributeValue,
        new_val: netcdf::AttributeValue,
    ) -> Result<Option<netcdf::AttributeValue>, ConcatError> {
        let curr_str = get_attr_as_string(curr_val)?;
        let new_str = get_attr_as_string(&new_val)?;

        let curr_norm_str = if curr_str == "" { "1" } else { curr_str };
        let new_norm_str = if new_str == "" { "1" } else { new_str };

        if new_norm_str == curr_str {
            return Ok(None);
        }

        if new_norm_str == curr_norm_str {
            return Ok(Some(netcdf::AttributeValue::Str(curr_norm_str.to_string())));
        }

        Err(ConcatError::AttrConcat {
            reason: "Mixing ratio units attribute values are not equal after regularizing"
                .to_string(),
            val1: curr_val.to_owned(),
            val2: new_val.to_owned(),
        })
    }
}

fn get_attr_as_string(attr: &netcdf::AttributeValue) -> Result<&str, ConcatError> {
    match attr {
        netcdf::AttributeValue::Str(s) => Ok(&s),
        netcdf::AttributeValue::Strs(items) => {
            if items.len() == 1 {
                Ok(&items[0])
            } else {
                Err(ConcatError::custom(
                    "Attribute must be a single string, not an array of multiple",
                ))
            }
        }
        _ => Err(ConcatError::custom("Attribute must be a string")),
    }
}

struct PriorIndexEditor;

impl PriorIndexEditor {
    fn new_boxed() -> Box<dyn DataEditor> {
        Box::new(Self)
    }
}

impl DataEditor for PriorIndexEditor {
    fn edit_i8_array(
        &mut self,
        array: ndarray::ArrayD<i8>,
        info: &ncdfcat_lib::data_editors::EditorInfo,
    ) -> Result<ndarray::ArrayD<i8>, ncdfcat_lib::error::ConcatError> {
        let array = edit_prior_index(array, info)?;
        Ok(array)
    }

    fn edit_i16_array(
        &mut self,
        array: ndarray::ArrayD<i16>,
        info: &ncdfcat_lib::data_editors::EditorInfo,
    ) -> Result<ndarray::ArrayD<i16>, ncdfcat_lib::error::ConcatError> {
        let array = edit_prior_index(array, info)?;
        Ok(array)
    }

    fn edit_i32_array(
        &mut self,
        array: ndarray::ArrayD<i32>,
        info: &ncdfcat_lib::data_editors::EditorInfo,
    ) -> Result<ndarray::ArrayD<i32>, ncdfcat_lib::error::ConcatError> {
        let array = edit_prior_index(array, info)?;
        Ok(array)
    }

    fn edit_i64_array(
        &mut self,
        array: ndarray::ArrayD<i64>,
        info: &ncdfcat_lib::data_editors::EditorInfo,
    ) -> Result<ndarray::ArrayD<i64>, ncdfcat_lib::error::ConcatError> {
        let array = edit_prior_index(array, info)?;
        Ok(array)
    }

    fn edit_u8_array(
        &mut self,
        _array: ndarray::ArrayD<u8>,
        _info: &ncdfcat_lib::data_editors::EditorInfo,
    ) -> Result<ndarray::ArrayD<u8>, ncdfcat_lib::error::ConcatError> {
        unimplemented!("prior_index expected to be a signed integer")
    }

    fn edit_u16_array(
        &mut self,
        _array: ndarray::ArrayD<u16>,
        _info: &ncdfcat_lib::data_editors::EditorInfo,
    ) -> Result<ndarray::ArrayD<u16>, ncdfcat_lib::error::ConcatError> {
        unimplemented!("prior_index expected to be a signed integer")
    }

    fn edit_u32_array(
        &mut self,
        _array: ndarray::ArrayD<u32>,
        _info: &ncdfcat_lib::data_editors::EditorInfo,
    ) -> Result<ndarray::ArrayD<u32>, ncdfcat_lib::error::ConcatError> {
        unimplemented!("prior_index expected to be a signed integer")
    }

    fn edit_u64_array(
        &mut self,
        _array: ndarray::ArrayD<u64>,
        _info: &ncdfcat_lib::data_editors::EditorInfo,
    ) -> Result<ndarray::ArrayD<u64>, ncdfcat_lib::error::ConcatError> {
        unimplemented!("prior_index expected to be a signed integer")
    }

    fn edit_f32_array(
        &mut self,
        _array: ndarray::ArrayD<f32>,
        _info: &ncdfcat_lib::data_editors::EditorInfo,
    ) -> Result<ndarray::ArrayD<f32>, ncdfcat_lib::error::ConcatError> {
        unimplemented!("prior_index expected to be a signed integer")
    }

    fn edit_f64_array(
        &mut self,
        _array: ndarray::ArrayD<f64>,
        _info: &ncdfcat_lib::data_editors::EditorInfo,
    ) -> Result<ndarray::ArrayD<f64>, ncdfcat_lib::error::ConcatError> {
        unimplemented!("prior_index expected to be a signed integer")
    }

    fn edit_char_array(
        &mut self,
        _array: ndarray::ArrayD<ncdfcat_lib::variables::NcChar>,
        _info: &ncdfcat_lib::data_editors::EditorInfo,
    ) -> Result<ndarray::ArrayD<ncdfcat_lib::variables::NcChar>, ncdfcat_lib::error::ConcatError>
    {
        unimplemented!("prior_index expected to be a signed integer")
    }
}

fn edit_prior_index<T: num_traits::Signed + Copy + TryFrom<usize>>(
    mut array: ArrayD<T>,
    info: &EditorInfo,
) -> Result<ArrayD<T>, ConcatError> {
    let istart: T = get_prior_time_start_index(info)?.try_into().map_err(|_| {
        ConcatError::custom(format!(
            "Error converting start index to appropriate type for prior_index (possibly an overflow)"
        ))
    })?;
    array.map_inplace(|i| *i = *i + istart);
    Ok(array)
}

fn get_prior_time_start_index(info: &EditorInfo) -> Result<usize, ConcatError> {
    let prior_time_extent = info
        .curr_dim_extents
        .get("prior_time")
        .ok_or_else(|| ConcatError::custom("Expected 'prior_time' dimension"))?;
    let istart = match prior_time_extent {
        netcdf::Extent::Slice { start, stride: _ } => *start,
        netcdf::Extent::SliceEnd {
            start,
            end: _,
            stride: _,
        } => *start,
        netcdf::Extent::SliceCount {
            start,
            count: _,
            stride: _,
        } => *start,
        netcdf::Extent::Index(i) => *i,
    };
    Ok(istart)
}
