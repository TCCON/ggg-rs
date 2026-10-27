use ggg_rs_derive::Concat;

#[derive(Concat)]
struct Bad {
    #[concat(kind = "list")]
    values: Vec<u8>,
}

fn main() {}
