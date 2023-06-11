use log::*;
use std::{
    mem::transmute,
    slice::{from_raw_parts, from_raw_parts_mut},
};

use crate::consts::*;

// dir.h
pub const MAXDIRSIZE: usize = 0x7fffffff;

pub const DIR_ROUNDUP: usize = 4;
pub fn directsiz(len: u8) -> usize {
    let len = len as usize;
    (std::mem::size_of::<Direct>() + (len + 1) + 3) & (!3)
}

pub const DT_UNKNOWN: u8 = 0;
pub const DT_FIFO: u8 = 1;
pub const DT_CHR: u8 = 2;
pub const DT_DIR: u8 = 4;
pub const DT_BLK: u8 = 6;
pub const DT_REG: u8 = 8;
pub const DT_LNK: u8 = 10;
pub const DT_SOCK: u8 = 12;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct Direct {
    pub d_ino: u32,    /* inode number of entry */
    pub d_reclen: u16, /* length of this record */
    pub d_type: u8,    /* file type, see below */
    pub d_namlen: u8,  /* length of string in d_name */
}

// write direct to given buffer
fn direct_write(buf: &mut [u8], mut direct: Direct, name: &[u8]) {
    let sz = directsiz(name.len() as u8);
    assert!(sz <= buf.len(), "{} > {}", sz, buf.len());

    direct.d_namlen = name.len() as u8;
    direct.d_reclen = buf.len() as u16;

    trace!("direct_write={:?}", direct);

    // write direct
    let dp: &mut Direct = unsafe { transmute(buf.as_mut_ptr()) };
    *dp = direct;

    // write name
    let namebuf: &mut [u8] = unsafe {
        let buf: *mut u8 = transmute(&mut buf[std::mem::size_of::<Direct>()]);
        from_raw_parts_mut(buf, direct.d_namlen as usize + 1)
    };
    namebuf[0..name.len()].copy_from_slice(name);
    namebuf[name.len()] = 0;
}

pub enum AppendResult {
    InPlace(usize),
    Failed,
}

pub fn direct_init(mut blkbuf: &mut [u8]) {
    assert!(blkbuf.len() % DEV_BSIZE == 0);

    while !blkbuf.is_empty() {
        let dp = Direct::default();
        direct_write(&mut blkbuf[0..DEV_BSIZE], dp, &[]);

        blkbuf = &mut blkbuf[DEV_BSIZE..];
    }
}

pub fn direct_append(blkbuf: &mut [u8], direct: Direct, name: &[u8]) -> AppendResult {
    let sz = directsiz(name.len() as u8);

    let mut offset = 0;
    while blkbuf.len() >= offset + directsiz(0) {
        let dp: &Direct = unsafe { transmute(&blkbuf[offset]) };
        assert!(dp.d_reclen > 0);

        let dpsz = if dp.d_ino == 0 {
            // empty slot
            assert!(dp.d_namlen == 0);
            0
        } else {
            assert!(dp.d_namlen > 0);
            directsiz(dp.d_namlen)
        };

        // check if fits
        if dpsz + sz <= dp.d_reclen as usize {
            let mut dpnambuf = [0u8; DEV_BSIZE];
            unsafe {
                let buf: *const u8 = transmute(&blkbuf[offset + std::mem::size_of::<Direct>()]);
                let src = from_raw_parts(buf, dp.d_namlen as usize);
                (&mut dpnambuf[..dp.d_namlen as usize]).copy_from_slice(src);
            };

            let buf = &mut blkbuf[offset..offset + dp.d_reclen as usize];
            let (buf0, buf1) = buf.split_at_mut(dpsz);

            if dp.d_ino > 0 {
                direct_write(buf0, *dp, &dpnambuf[..dp.d_namlen as usize]);
            }
            direct_write(buf1, direct, name);
            return AppendResult::InPlace(offset);
        }

        offset += dp.d_reclen as usize;
        continue;
    }

    // should be block boundary
    assert!(offset % DEV_BSIZE == 0);
    AppendResult::Failed
}

pub fn direct_parse(blk: &[u8]) -> Vec<(&Direct, &str)> {
    let mut out = Vec::new();

    let mut offset = 0;
    while offset < blk.len() {
        let dp: &Direct = unsafe { transmute(&blk[offset]) };
        if dp.d_ino == 0 {
            offset += dp.d_reclen as usize;
            continue;
        }
        assert!(dp.d_reclen > 0);
        assert!(dp.d_namlen > 0);

        trace!("direct_parse: offset={}/{}, dp={:?}", offset, blk.len(), dp);

        let namebuf: &[u8] = unsafe {
            let buf: *const u8 = transmute(&blk[offset + std::mem::size_of::<Direct>()]);
            from_raw_parts(buf, dp.d_namlen as usize)
        };

        offset += dp.d_reclen as usize;

        let filename = if let Ok(s) = std::str::from_utf8(&namebuf) {
            s
        } else {
            "???"
        };

        out.push((dp, filename));
    }
    out
}

pub fn direct_empty(blk: &[u8], ino: u32, ino_parent: u32) -> bool {
    let mut offset = 0;
    while offset < blk.len() {
        let dp: &Direct = unsafe { transmute(&blk[offset]) };
        if dp.d_reclen == 0 {
            return false;
        }
        if dp.d_ino == 0 {
            offset += dp.d_reclen as usize;
            continue;
        }

        let namebuf: &[u8] = unsafe {
            let buf: *const u8 = transmute(&blk[offset + std::mem::size_of::<Direct>()]);
            from_raw_parts(buf, dp.d_namlen as usize)
        };

        offset += dp.d_reclen as usize;

        if namebuf == b"." && dp.d_ino == ino {
            continue;
        }
        if namebuf == b".." && dp.d_ino == ino_parent {
            continue;
        }
        return false;
    }
    return true;
}
