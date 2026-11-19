use proc_macro::TokenStream;

mod common_netcdf;
mod common_subset;
mod concat;
mod read_netcdf;
mod subset;
mod write_netcdf;

#[doc = include_str!("../docs/subset.md")]
#[proc_macro_derive(Subset, attributes(subset))]
pub fn derive_subset(input: TokenStream) -> TokenStream {
    subset::expand(input)
}

#[doc = include_str!("../docs/concat.md")]
#[proc_macro_derive(Concat, attributes(concat))]
pub fn derive_concat(input: TokenStream) -> TokenStream {
    concat::expand(input)
}

#[doc = include_str!("../docs/write_netcdf.md")]
#[proc_macro_derive(WriteNetcdf, attributes(netcdf))]
pub fn derive_write_netcdf(input: TokenStream) -> TokenStream {
    write_netcdf::expand(input)
}

#[doc = include_str!("../docs/read_netcdf.md")]
#[proc_macro_derive(ReadNetcdf, attributes(netcdf))]
pub fn derive_read_netcdf(input: TokenStream) -> TokenStream {
    read_netcdf::expand(input)
}
