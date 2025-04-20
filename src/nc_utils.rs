use ndarray::ArrayD;
use netcdf::{types::{FloatType, IntType}, Extents};

/// A type that can hold a variety of arrays that might be stored
/// in a netCDF file. It is best created by reading from a netCDF
/// variable with its `get_from` method.
pub enum NcArray {
    I8(ArrayD<i8>),
    I16(ArrayD<i16>),
    I32(ArrayD<i32>),
    I64(ArrayD<i64>),
    U8(ArrayD<u8>),
    U16(ArrayD<u16>),
    U32(ArrayD<u32>),
    U64(ArrayD<u64>),
    F32(ArrayD<f32>),
    F64(ArrayD<f64>),
    Char(ArrayD<u8>),
}

impl NcArray {
    /// Retrieve data from a netCDF variable and construct the appropriate variant.
    /// 
    /// # Panics
    /// Compound, opaque, enum, and variable length types are not supported, and
    /// may never be, due to their rarity.
    pub fn get_from(var: &netcdf::Variable) -> netcdf::Result<Self> {
        match var.vartype() {
            netcdf::types::NcVariableType::Compound(_) => {
                unimplemented!("reading netCDF Compound types as a generic array")
            },
            netcdf::types::NcVariableType::Opaque(_) => {
                unimplemented!("reading netCDF Opaque types as a generic array")
            },
            netcdf::types::NcVariableType::Enum(_) => {
                unimplemented!("reading netCDF Enum types as a generic array")
            },
            netcdf::types::NcVariableType::Vlen(_) => {
                unimplemented!("reading netCDF variable length types as a generic array")
            },
            netcdf::types::NcVariableType::String => todo!(),
            netcdf::types::NcVariableType::Int(IntType::I8) => {
                let values = var.get::<i8, _>(Extents::All)?;
                Ok(Self::I8(values))
            },
            netcdf::types::NcVariableType::Int(IntType::I16) => {
                let values = var.get::<i16, _>(Extents::All)?;
                Ok(Self::I16(values))
            },
            netcdf::types::NcVariableType::Int(IntType::I32) => {
                let values = var.get::<i32, _>(Extents::All)?;
                Ok(Self::I32(values))
            },
            netcdf::types::NcVariableType::Int(IntType::I64) => {
                let values = var.get::<i64, _>(Extents::All)?;
                Ok(Self::I64(values))
            },
            netcdf::types::NcVariableType::Int(IntType::U8) => {
                let values = var.get::<u8, _>(Extents::All)?;
                Ok(Self::U8(values))
            },
            netcdf::types::NcVariableType::Int(IntType::U16) => {
                let values = var.get::<u16, _>(Extents::All)?;
                Ok(Self::U16(values))
            },
            netcdf::types::NcVariableType::Int(IntType::U32) => {
                let values = var.get::<u32, _>(Extents::All)?;
                Ok(Self::U32(values))
            },
            netcdf::types::NcVariableType::Int(IntType::U64) => {
                let values = var.get::<u64, _>(Extents::All)?;
                Ok(Self::U64(values))
            },
            netcdf::types::NcVariableType::Float(FloatType::F32) => {
                let values = var.get::<f32, _>(Extents::All)?;
                Ok(Self::F32(values))
            },
            netcdf::types::NcVariableType::Float(FloatType::F64) => {
                let values = var.get::<f64, _>(Extents::All)?;
                Ok(Self::F64(values))
            },
            netcdf::types::NcVariableType::Char => {
                let values = var.get::<u8, _>(Extents::All)?;
                Ok(Self::Char(values))
            },
        }
    }

    /// Create a variable in a netCDF group and write this data to it.
    /// Since this writes data, if you need to set options on the variable
    /// that must be done pre-write (e.g., compression), you must match
    /// on this enum's variants and create the variable yourself (for now at least).
    pub fn put_to<'g>(&self, grp: &'g mut netcdf::GroupMut, name: &str, dims: &[&str]) -> netcdf::Result<netcdf::VariableMut<'g>> {
        match self {
            NcArray::I8(arr) => {
                let mut var = grp.add_variable::<i8>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
            NcArray::I16(arr) => {
                let mut var = grp.add_variable::<i16>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
            NcArray::I32(arr) => {
                let mut var = grp.add_variable::<i32>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
            NcArray::I64(arr) => {
                let mut var = grp.add_variable::<i64>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
            NcArray::U8(arr) => {
                let mut var = grp.add_variable::<u8>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
            NcArray::U16(arr) => {
                let mut var = grp.add_variable::<u16>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
            NcArray::U32(arr) => {
                let mut var = grp.add_variable::<u32>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
            NcArray::U64(arr) => {
                let mut var = grp.add_variable::<u64>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
            NcArray::F32(arr) => {
                let mut var = grp.add_variable::<f32>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
            NcArray::F64(arr) => {
                let mut var = grp.add_variable::<f64>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
            NcArray::Char(arr) => {
                let mut var = grp.add_variable::<u8>(name, dims)?;
                var.put(arr.view(), Extents::All)?;
                Ok(var)
            },
        }
    }
}