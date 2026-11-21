use ggg_rs_derive::Concat;

#[derive(Concat)]
struct Bad {
    #[concat(skip = "first", kind = "vec")]
    values: Vec<u8>,
}

fn main() {}
