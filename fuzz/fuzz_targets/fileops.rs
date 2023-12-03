#![no_main]

use ffs::*;
use libfuzzer_sys::fuzz_target;

use arbitrary::Arbitrary;

#[derive(Arbitrary, Debug)]
pub enum Op {
    Mknod {
        parent: u8,
        name: u8,
    },
    Mkdir {
        parent: u8,
        name: u8,
    },
    Unlink {
        parent: u8,
        name: u8,
    },
    Rmdir {
        parent: u8,
        name: u8,
    },
    Rename {
        parent: u8,
        name: u8,
        newparent: u8,
        newname: u8,
    },
}

const IFREG: usize = 0o100000; /* Regular file. */
const IFDIR: usize = 0o040000; /* Directory file. */

fuzz_target!(|ops: Vec<Op>| {
    const BYTES: &'static [u8] = include_bytes!("../../images/ffs2-vnd.img");

    let mut clone = BYTES.to_vec();
    let mut ffs = FFS::new(clone.as_mut()).unwrap();

    let root: u64 = 2;
    let mut dirs = vec![root];

    let names = (0..256).map(|i| format!("name{}", i)).collect::<Vec<_>>();

    for op in ops {
        match op {
            Op::Mknod { parent, name } => {
                let parent = dirs[parent as usize % dirs.len()];
                let name = &names[name as usize];
                ffs.mknod0(parent, &name, IFREG as u32).ok();
            }
            Op::Mkdir { parent, name } => {
                let parent = dirs[parent as usize % dirs.len()];
                let name = &names[name as usize];
                if let Ok(attr) = ffs.mkdir0(parent, &name, IFDIR as u32) {
                    dirs.push(attr.ino);
                }
            }
            Op::Unlink { parent, name } => {
                let parent = dirs[parent as usize % dirs.len()];
                let name = &names[name as usize];
                ffs.unlink0(parent, &name).ok();
            }
            Op::Rmdir { parent, name } => {
                let parent = dirs[parent as usize % dirs.len()];
                let name = &names[name as usize];
                ffs.rmdir0(parent, &name).ok();
            }
            Op::Rename {
                parent,
                name,
                newparent,
                newname,
            } => {
                let parent = dirs[parent as usize % dirs.len()];
                let name = &names[name as usize];
                let newparent = dirs[newparent as usize % dirs.len()];
                let newname = &names[newname as usize];
                ffs.rename0(parent, &name, newparent, &newname).ok();
            }
        }
    }
});
