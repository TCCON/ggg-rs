use ggg_rs_derive::Concat;

#[derive(Concat)]
struct Bad {
    values: Vec<u8>,
    #[concat(skip = "last")]
    name: String,
}

fn main() {}
