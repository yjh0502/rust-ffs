#![no_main]

use ffs::*;
use libfuzzer_sys::fuzz_target;

use arbitrary::Arbitrary;

#[derive(Arbitrary, Debug)]
pub enum Op {
    Resize { idx: usize, size: u64 },
}

const IFREG: usize = 0o100000; /* Regular file. */

fuzz_target!(|ops: Vec<Op>| {
    const BYTES: &'static [u8] = include_bytes!("../../images/ffs2-vnd.img");

    let mut clone = BYTES.to_vec();
    let mut ffs = FFS::new(clone.as_mut()).unwrap();

    let parent: u64 = 2;
    let count = 4;
    let inodes = (0..count)
        .map(|i| {
            let name = format!("file{}", i);
            let attr = ffs.mknod0(parent, &name, IFREG as u32).unwrap();
            attr.ino
        })
        .collect::<Vec<_>>();

    for op in ops {
        match op {
            Op::Resize { idx, size } => {
                let ino = inodes[idx % count];
                ffs.setattr0(ino, None, None, None, Some(size), None, None, None, None)
                    .ok();
            }
        }
    }
});
