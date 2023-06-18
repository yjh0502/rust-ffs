#![no_main]

use ffs::*;
use libfuzzer_sys::fuzz_target;

use arbitrary::Arbitrary;

#[derive(Arbitrary, Debug)]
pub enum Op {
    Mknod {
        parent: u64,
        mode: u32,
        name: String,
    },
    Mkdir {
        parent: u64,
        mode: u32,
        name: String,
    },
    Unlink {
        parent: u64,
        name: String,
    },
    Rmdir {
        parent: u64,
        name: String,
    },
}

fuzz_target!(|ops: Vec<Op>| {
    const BYTES: &'static [u8] = include_bytes!("../../images/ffs2-vnd.img");

    let mut clone = BYTES.to_vec();
    let mut ffs = FFS::new(clone.as_mut()).unwrap();

    for op in ops {
        match op {
            Op::Mknod { parent, name, mode } => {
                ffs.mknod0(parent, &name, mode).ok();
            }
            Op::Mkdir { parent, name, mode } => {
                ffs.mkdir0(parent, &name, mode).ok();
            }
            Op::Unlink { parent, name } => {
                ffs.unlink0(parent, &name).ok();
            }
            Op::Rmdir { parent, name } => {
                ffs.rmdir0(parent, &name).ok();
            }
        }
    }
});
