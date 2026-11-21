use ggg_rs_derive::Concat;

type Hidden = Vec<u8>;

#[derive(Concat)]
struct Bad {
    values: Hidden,
}

fn main() {}
