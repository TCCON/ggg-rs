use error_stack::ResultExt;

use crate::interface::TranscriptionError;

pub fn add_common_attrs(var: &mut netcdf::VariableMut, descr: &str, units: &str, std_name: &str, long_name: Option<&str>)
    -> error_stack::Result<(), TranscriptionError> {
        var.add_attribute("standard_name", std_name)
            .change_context_lazy(|| TranscriptionError::nc_error(format!("adding standard_name attribute to {std_name}")))?;

        if let Some(ln) = long_name {
            var.add_attribute("long_name", ln)
        } else {
            var.add_attribute("long_name", std_name.replace("_", " "))
        }.change_context_lazy(|| TranscriptionError::nc_error(format!("adding standard_name attribute to {std_name}")))?;
        
        var.add_attribute("description", descr)
            .change_context_lazy(|| TranscriptionError::nc_error(format!("adding description attribute to {std_name}")))?;

        var.add_attribute("units", units)
            .change_context_lazy(|| TranscriptionError::nc_error(format!("adding units attribute to {std_name}")))?;

        Ok(())
    }