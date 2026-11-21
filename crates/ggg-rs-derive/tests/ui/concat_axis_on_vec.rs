use ggg_rs_derive::Concat;

#[derive(Concat)]
struct Bad {
    #[concat(axis = 1)]
    values: Vec<u8>,
}

fn main() {}
