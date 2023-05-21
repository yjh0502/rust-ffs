use anyhow::Result;
use log::*;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{
    mem::transmute,
    slice::{from_raw_parts, from_raw_parts_mut},
};

// dinode.h
/* File permissions. */
const IEXEC: usize = 0o000100; /* Executable. */
const IWRITE: usize = 0o000200; /* Writeable. */
const IREAD: usize = 0o000400; /* Readable. */
const ISVTX: usize = 0o001000; /* Sticky bit. */
const ISGID: usize = 0o002000; /* Set-gid. */
const ISUID: usize = 0o004000; /* Set-uid. */

/* File types. */
const IFMT: usize = 0o170000; /* Mask of file type. */
const IFIFO: usize = 0o010000; /* Named pipe (fifo). */
const IFCHR: usize = 0o020000; /* Character device. */
const IFDIR: usize = 0o040000; /* Directory file. */
const IFBLK: usize = 0o060000; /* Block device. */
const IFREG: usize = 0o100000; /* Regular file. */
const IFLNK: usize = 0o120000; /* Symbolic link. */
const IFSOCK: usize = 0o140000; /* UNIX domain socket. */
const IFWHT: usize = 0o160000; /* Whiteout. */

// dir.h
const MAXDIRSIZE: usize = 0x7fffffff;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct Direct {
    d_ino: u32,    /* inode number of entry */
    d_reclen: u16, /* length of this record */
    d_type: u8,    /* file type, see below */
    d_namlen: u8,  /* length of string in d_name */
}

const DIR_ROUNDUP: usize = 4;
fn directsiz(len: u8) -> usize {
    let len = len as usize;
    (std::mem::size_of::<Direct>() + (len + 1) + 3) & (!3)
}

const DT_UNKNOWN: u8 = 0;
const DT_FIFO: u8 = 1;
const DT_CHR: u8 = 2;
const DT_DIR: u8 = 4;
const DT_BLK: u8 = 6;
const DT_REG: u8 = 8;
const DT_LNK: u8 = 10;
const DT_SOCK: u8 = 12;

// inode.h

const ROOTINO: usize = 2;
const NXADDR: usize = 2;
const NDADDR: usize = 12;
const NIADDR: usize = 3;

#[repr(C)]
#[derive(Clone, Copy)]
union Ufs1DinodeU {
    oldids: [u16; 2], /*   4: Ffs: old user and group ids. */
    inumber: u32,     /*   4: Lfs: inode number. */
}

impl Default for Ufs1DinodeU {
    fn default() -> Self {
        Ufs1DinodeU { inumber: 0 }
    }
}

impl std::fmt::Debug for Ufs1DinodeU {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe {
            f.debug_struct("Ufs1DinodeU")
                .field("oldids", &self.oldids)
                .field("inumber", &self.inumber)
                .finish()
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct Ufs1Dinode {
    di_mode: u16,  /*   0: IFMT, permissions; see below. */
    di_nlink: i16, /*   2: File link count. */
    di_u: Ufs1DinodeU,
    di_size: u64,         /*   8: File byte count. */
    di_atime: i32,        /*  16: Last access time. */
    di_atimensec: i32,    /*  20: Last access time. */
    di_mtime: i32,        /*  24: Last modified time. */
    di_mtimensec: i32,    /*  28: Last modified time. */
    di_ctime: i32,        /*  32: Last inode change time. */
    di_ctimensec: i32,    /*  36: Last inode change time. */
    di_db: [i32; NDADDR], /*  40: Direct disk blocks. */
    di_ib: [i32; NIADDR], /*  88: Indirect disk blocks. */
    di_flags: u32,        /* 100: Status flags (chflags). */
    di_blocks: i32,       /* 104: Blocks actually held. */
    di_gen: u32,          /* 108: Generation number. */
    di_uid: u32,          /* 112: File owner. */
    di_gid: u32,          /* 116: File group. */
    di_spare: [i32; 2],   /* 120: Reserved; currently unused */
}

impl From<Ufs2Dinode> for Ufs1Dinode {
    fn from(ino: Ufs2Dinode) -> Ufs1Dinode {
        Ufs1Dinode {
            di_mode: ino.di_mode,
            di_nlink: ino.di_nlink,
            di_u: Ufs1DinodeU::default(),
            di_size: ino.di_size,
            di_atime: ino.di_atime as i32,
            di_mtime: ino.di_mtime as i32,
            di_ctime: ino.di_ctime as i32,
            di_mtimensec: ino.di_mtimensec,
            di_atimensec: ino.di_atimensec,
            di_ctimensec: ino.di_ctimensec,
            di_db: ino.di_db.map(|x| x as i32),
            di_ib: ino.di_ib.map(|x| x as i32),
            di_flags: ino.di_flags,
            di_blocks: ino.di_blocks as i32,
            di_gen: ino.di_gen as u32,
            di_uid: ino.di_uid,
            di_gid: ino.di_gid,
            di_spare: [0; 2],
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
struct Ufs2Dinode {
    di_mode: u16,           /*   0: IFMT, permissions; see below. */
    di_nlink: i16,          /*   2: File link count. */
    di_uid: u32,            /*   4: File owner. */
    di_gid: u32,            /*   8: File group. */
    di_blksize: u32,        /*  12: Inode blocksize. */
    di_size: u64,           /*  16: File byte count. */
    di_blocks: u64,         /*  24: Bytes actually held. */
    di_atime: i64,          /*  32: Last access time. */
    di_mtime: i64,          /*  40: Last modified time. */
    di_ctime: i64,          /*  48: Last inode change time. */
    di_birthtime: i64,      /*  56: Inode creation time. */
    di_mtimensec: i32,      /*  64: Last modified time. */
    di_atimensec: i32,      /*  68: Last access time. */
    di_ctimensec: i32,      /*  72: Last inode change time. */
    di_birthnsec: i32,      /*  76: Inode creation time. */
    di_gen: i32,            /*  80: Generation number. */
    di_kernflags: u32,      /*  84: Kernel flags. */
    di_flags: u32,          /*  88: Status flags (chflags). */
    di_extsize: i32,        /*  92: External attributes block. */
    di_extb: [i64; NXADDR], /*  96: External attributes block. */
    di_db: [i64; NDADDR],   /* 112: Direct disk blocks. */
    di_ib: [i64; NIADDR],   /* 208: Indirect disk blocks. */
    di_spare: [i64; 3],     /* 232: Reserved; currently unused */
}

impl From<Ufs1Dinode> for Ufs2Dinode {
    fn from(ino: Ufs1Dinode) -> Ufs2Dinode {
        Ufs2Dinode {
            di_mode: ino.di_mode,
            di_nlink: ino.di_nlink,
            di_uid: ino.di_uid,
            di_gid: ino.di_gid,
            di_blksize: 0,
            di_size: ino.di_size,
            di_blocks: ino.di_blocks as u64,
            di_atime: ino.di_atime as i64,
            di_mtime: ino.di_mtime as i64,
            di_ctime: ino.di_ctime as i64,
            di_birthtime: 0,
            di_mtimensec: ino.di_mtimensec,
            di_atimensec: ino.di_atimensec,
            di_ctimensec: ino.di_ctimensec,
            di_birthnsec: 0,
            di_gen: ino.di_gen as i32,
            di_kernflags: 0,
            di_flags: ino.di_flags,
            di_extsize: 0,
            di_extb: [0; NXADDR],
            di_db: ino.di_db.map(|x| x as i64),
            di_ib: ino.di_ib.map(|x| x as i64),
            di_spare: [0; 3],
        }
    }
}

impl Ufs2Dinode {
    fn empty(d: Duration) -> Self {
        let epoch = d.as_secs() as i64;
        let epoch_ns = d.subsec_nanos() as i32;

        Ufs2Dinode {
            di_mode: 0,
            di_nlink: 0,
            di_uid: 1, //TODO
            di_gid: 1, //TODO
            di_blksize: 0,
            di_size: 0,
            di_blocks: 0,
            di_atime: epoch,
            di_mtime: epoch,
            di_ctime: epoch,
            di_birthtime: epoch,
            di_mtimensec: epoch_ns,
            di_atimensec: epoch_ns,
            di_ctimensec: epoch_ns,
            di_birthnsec: epoch_ns,
            di_gen: 0,
            di_kernflags: 0,
            di_flags: 0,
            di_extsize: 0,
            di_extb: [0i64; NXADDR],
            di_db: [0i64; NDADDR],
            di_ib: [0i64; NIADDR],
            di_spare: [0i64; 3],
        }
    }
}

// cylinder group
const CG_MAGIC: i32 = 0x090255;

#[repr(C)]
#[derive(Debug, Clone)]
struct Cg {
    cg_firstfield: i32,       /* historic cyl groups linked list */
    cg_magic: i32,            /* magic number */
    cg_time: i32,             /* time last written */
    cg_cgx: u32,              /* we are the cgx'th cylinder group */
    cg_ncyl: i16,             /* number of cyl's this cg */
    cg_niblk: i16,            /* number of inode blocks this cg */
    cg_ndblk: u32,            /* number of data blocks this cg */
    cg_cs: Csum,              /* cylinder summary information */
    cg_rotor: u32,            /* position of last used block */
    cg_frotor: u32,           /* position of last used frag */
    cg_irotor: u32,           /* position of last used inode */
    cg_frsum: [u32; MAXFRAG], /* counts of available frags */
    // FFS1 only
    cg_btotoff: i32, /* (int32) block totals per cylinder */
    // FFS1 only
    cg_boff: i32,            /* (u_int16) free block positions */
    cg_iusedoff: u32,        /* (u_int8) used inode map */
    cg_freeoff: u32,         /* (u_int8) free block map */
    cg_nextfreeoff: u32,     /* (u_int8) next available space */
    cg_clustersumoff: u32,   /* (u_int32) counts of avail clusters */
    cg_clusteroff: u32,      /* (u_int8) free cluster map */
    cg_nclusterblks: u32,    /* number of clusters this cg */
    cg_ffs2_niblk: u32,      /* number of inode blocks this cg */
    cg_initediblk: u32,      /* last initialized inode */
    cg_sparecon32: [i32; 3], /* reserved for future use */
    cg_ffs2_time: i64,       /* time last written */
    cg_sparecon64: [i64; 3], /* reserved for future use */
                             /* actually longer */
}

impl Fs {
    fn cg_bitmap<'a, 'b, F>(&'a self, buf: &'b [u8], c: usize, f: F) -> &'b [u8]
    where
        F: FnOnce(&Cg) -> (usize, usize),
    {
        let cgbuf = self.cgbuf(buf, c);
        let cg: &Cg = unsafe { transmute(cgbuf.as_ptr()) };
        let (offset, size) = f(cg);
        unsafe { from_raw_parts(&cgbuf[offset], size) }
    }

    fn cg_bitmap_mut<'a, 'b, F>(&'a self, buf: &'b mut [u8], c: usize, f: F) -> &'b mut [u8]
    where
        F: FnOnce(&Cg) -> (usize, usize),
    {
        let cgbuf = self.cgbuf_mut(buf, c);
        let cg: &Cg = unsafe { transmute(cgbuf.as_ptr()) };
        let (offset, size) = f(cg);
        unsafe { from_raw_parts_mut(&mut cgbuf[offset], size) }
    }

    fn cg_inosused<'a, 'b>(&'a self, buf: &'b [u8], c: usize) -> &'b [u8] {
        self.cg_bitmap(buf, c, |cg| {
            (cg.cg_iusedoff as usize, cg.niblk() as usize / 8)
        })
    }

    fn cg_inosused_mut<'a, 'b>(&'a self, buf: &'b mut [u8], c: usize) -> &'b mut [u8] {
        self.cg_bitmap_mut(buf, c, |cg| {
            (cg.cg_iusedoff as usize, cg.niblk() as usize / 8)
        })
    }

    fn cg_blksfree<'a, 'b>(&'a self, buf: &'b [u8], c: usize) -> &'b [u8] {
        self.cg_bitmap(buf, c, |cg| {
            (cg.cg_freeoff as usize, cg.cg_ndblk as usize / 8)
        })
    }

    fn cg_blksfree_mut<'a, 'b>(&'a self, buf: &'b mut [u8], c: usize) -> &'b mut [u8] {
        self.cg_bitmap_mut(buf, c, |cg| {
            (cg.cg_freeoff as usize, cg.cg_ndblk as usize / 8)
        })
    }
}

impl Cg {
    /*
     * Macros for access to cylinder group array structures
     */
    /*
    #define cg_blktot(cgp) \
        (((cgp)->cg_magic != CG_MAGIC) \
        ? (((struct ocg *)(cgp))->cg_btot) \
        : ((int32_t *)((u_int8_t *)(cgp) + (cgp)->cg_btotoff)))
    #define cg_blks(fs, cgp, cylno) \
        (((cgp)->cg_magic != CG_MAGIC) \
        ? (((struct ocg *)(cgp))->cg_b[cylno]) \
        : ((int16_t *)((u_int8_t *)(cgp) + \
        (cgp)->cg_boff) + (cylno) * (fs)->fs_nrpos))
    #define cg_chkmagic(cgp) \
        ((cgp)->cg_magic == CG_MAGIC || ((struct ocg *)(cgp))->cg_magic == CG_MAGIC)
    #define cg_clustersfree(cgp) \
        ((u_int8_t *)((u_int8_t *)(cgp) + (cgp)->cg_clusteroff))
    #define cg_clustersum(cgp) \
        ((int32_t *)((u_int8_t *)(cgp) + (cgp)->cg_clustersumoff))
    */
}

impl Cg {
    fn niblk(&self) -> u32 {
        // TODO: should use fs info
        if self.cg_ffs2_niblk > 0 {
            self.cg_ffs2_niblk
        } else {
            self.cg_niblk as u32
        }
    }
}

const DEV_BSHIFT: usize = 9;
const DEV_BSIZE: usize = 1 << DEV_BSHIFT;

const SBLOCK_UFS1: usize = 8192;
const SBLOCK_UFS2: usize = 65536;
const SBLOCK_PIGGY: usize = 262144;

const SBLOCKSEARCH: [usize; 3] = [SBLOCK_UFS1, SBLOCK_UFS2, SBLOCK_PIGGY];

const MAXMNTLEN: usize = 468;
const MAXVOLLEN: usize = 32;

const NOCSPTRS: usize = (128 / 8) - 4;

const FSMAXSNAP: usize = 20;

const SBSIZE: usize = 8192;

const FS_UFS1_MAGIC: i32 = 0x011954;
const FS_UFS2_MAGIC: i32 = 0x19540119;

const MINBSIZE: usize = 4096;
const MAXBSIZE: usize = 64 * 1024;

const MAXFRAG: usize = 8;

/*
 * Per cylinder group information; summarized in blocks allocated
 * from first cylinder group data blocks.  These blocks have to be
 * read in from fs_csaddr (size fs_cssize) in addition to the
 * super block.
 */
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct Csum {
    cs_ndir: i32,   /* number of directories */
    cs_nbfree: i32, /* number of free blocks */
    cs_nifree: i32, /* number of free inodes */
    cs_nffree: i32, /* number of free frags */
}

#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct CsumTotal {
    cs_ndir: i64,       /* number of directories */
    cs_nbfree: i64,     /* number of free blocks */
    cs_nifree: i64,     /* number of free inodes */
    cs_nffree: i64,     /* number of free frags */
    cs_spare: [i64; 4], /* future expansion */
}

#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct Fs {
    fs_firstfield: i32, /* historic file system linked list, */
    fs_unused_1: i32,   /*     used for incore super blocks */
    fs_sblkno: i32,     /* addr of super-block / frags */
    fs_cblkno: i32,     /* offset of cyl-block / frags */
    fs_iblkno: i32,     /* offset of inode-blocks / frags */
    fs_dblkno: i32,     /* offset of first data / frags */
    fs_cgoffset: i32,   /* cylinder group offset in cylinder */
    fs_cgmask: i32,     /* used to calc mod fs_ntrak */
    fs_ffs1_time: i32,  /* last time written */
    fs_ffs1_size: i32,  /* # of blocks in fs / frags */
    fs_ffs1_dsize: i32, /* # of data blocks in fs */
    fs_ncg: u32,        /* # of cylinder groups */
    fs_bsize: i32,      /* size of basic blocks / bytes */
    fs_fsize: i32,      /* size of frag blocks / bytes */
    fs_frag: i32,       /* # of frags in a block in fs */
    /* these are configuration parameters */
    fs_minfree: i32,  /* minimum percentage of free blocks */
    fs_rotdelay: i32, /* # of ms for optimal next block */
    fs_rps: i32,      /* disk revolutions per second */
    /* these fields can be computed from the others */
    fs_bmask: i32,  /* ``blkoff'' calc of blk offsets */
    fs_fmask: i32,  /* ``fragoff'' calc of frag offsets */
    fs_bshift: i32, /* ``lblkno'' calc of logical blkno */
    fs_fshift: i32, /* ``numfrags'' calc # of frags */
    /* these are configuration parameters */
    fs_maxcontig: i32, /* max # of contiguous blks */
    fs_maxbpg: i32,    /* max # of blks per cyl group */
    /* these fields can be computed from the others */
    fs_fragshift: i32, /* block to frag shift */
    fs_fsbtodb: i32,   /* fsbtodb and dbtofsb shift constant */
    fs_sbsize: i32,    /* actual size of super block */
    fs_csmask: i32,    /* csum block offset (now unused) */
    fs_csshift: i32,   /* csum block number (now unused) */
    fs_nindir: i32,    /* value of NINDIR */
    fs_inopb: u32,     /* inodes per file system block */
    fs_nspf: i32,      /* DEV_BSIZE sectors per frag */
    /* yet another configuration parameter */
    fs_optim: i32, /* optimization preference, see below */
    /* these fields are derived from the hardware */
    fs_npsect: i32,     /* DEV_BSIZE sectors/track + spares */
    fs_interleave: i32, /* DEV_BSIZE sector interleave */
    fs_trackskew: i32,  /* sector 0 skew, per track */
    /* fs_id takes the space of the unused fs_headswitch and fs_trkseek fields */
    fs_id: [i32; 2], /* unique filesystem id */
    /* sizes determined by number of cylinder groups and their sizes */
    fs_ffs1_csaddr: i32, /* blk addr of cyl grp summary area */
    fs_cssize: i32,      /* cyl grp summary area size / bytes */
    fs_cgsize: i32,      /* cyl grp block size / bytes */
    /* these fields are derived from the hardware */
    fs_ntrak: i32, /* tracks per cylinder */
    fs_nsect: i32, /* DEV_BSIZE sectors per track */
    fs_spc: i32,   /* DEV_BSIZE sectors per cylinder */
    /* this comes from the disk driver partitioning */
    fs_ncyl: i32, /* cylinders in file system */
    /* these fields can be computed from the others */
    fs_cpg: i32, /* cylinders per group */
    fs_ipg: u32, /* inodes per group */
    fs_fpg: i32, /* blocks per group * fs_frag */
    /* this data must be re-computed after crashes */
    fs_ffs1_cstotal: Csum, /* cylinder summary information */
    /* these fields are cleared at mount time */
    fs_fmod: i8,                 /* super block modified flag */
    fs_clean: i8,                /* file system is clean flag */
    fs_ronly: i8,                /* mounted read-only flag */
    fs_ffs1_flags: i8,           /* see FS_ below */
    fs_fsmnt: [u8; MAXMNTLEN],   /* name mounted on */
    fs_volname: [u8; MAXVOLLEN], /* volume name */
    fs_swuid: u64,               /* system-wide uid */
    fs_pad: i32,                 /* due to alignment of fs_swuid */
    /* these fields retain the current block allocation info */
    fs_cgrotor: i32,            /* last cg searched */
    fs_ocsp: [usize; NOCSPTRS], /* padding; was list of fs_cs buffers */
    fs_contigdirs: usize,       /* # of contiguously allocated dirs */
    fs_csp: usize,              /* cg summary info buffer for fs_cs */
    fs_maxcluster: usize,       /* max cluster in each cyl group */
    fs_active: usize,           /* reserved for snapshots */
    fs_cpc: i32,                /* cyl per cycle in postbl */
    /* this area is only allocated if fs_ffs1_flags & FS_FLAGS_UPDATED */
    fs_maxbsize: i32,              /* maximum blocking factor permitted */
    fs_spareconf64: [i64; 17],     /* old rotation block list head */
    fs_sblockloc: i64,             /* offset of standard super block */
    fs_cstotal: CsumTotal,         /* cylinder summary information */
    fs_time: i64,                  /* time last written */
    fs_size: i64,                  /* number of blocks in fs */
    fs_dsize: i64,                 /* number of data blocks in fs */
    fs_csaddr: i64,                /* blk addr of cyl grp summary area */
    fs_pendingblocks: i64,         /* blocks in process of being freed */
    fs_pendinginodes: u32,         /* inodes in process of being freed */
    fs_snapinum: [u32; FSMAXSNAP], /* space reserved for snapshots */
    /* back to stuff that has been around a while */
    fs_avgfilesize: u32,    /* expected average file size */
    fs_avgfpdir: u32,       /* expected # of files per directory */
    fs_sparecon: [i32; 26], /* reserved for future constants */
    fs_flags: u32,          /* see FS_ flags below */
    fs_fscktime: i32,       /* last time fsck(8)ed */
    fs_contigsumsize: i32,  /* size of cluster summary array */
    fs_maxsymlinklen: i32,  /* max length of an internal symlink */
    fs_inodefmt: i32,       /* format of on-disk inodes */
    fs_maxfilesize: u64,    /* maximum representable file size */
    fs_qbmask: i64,         /* ~fs_bmask - for use with quad size */
    fs_qfmask: i64,         /* ~fs_fmask - for use with quad size */
    fs_state: i32,          /* validate fs_clean field */
    fs_postblformat: i32,   /* format of positional layout tables */
    fs_nrpos: i32,          /* number of rotational positions */
    fs_postbloff: i32,      /* (u_int16) rotation block list head */
    fs_rotbloff: i32,       /* (u_int8) blocks for each rotation */
    fs_magic: i32,          /* magic number */
    fs_space: [u8; 1],      /* list of blocks for each rotation */
                            /* actually longer */
}

impl Fs {
    /*
     * Turn file system block numbers into disk block addresses.
     * This maps file system blocks to DEV_BSIZE (a.k.a. 512-byte) size disk
     * blocks.
     */
    fn fsbtodb(&self, b: i64) -> i64 {
        b << self.fs_fsbtodb
    }
    fn dbtofsb(&self, b: i64) -> i64 {
        b >> self.fs_fsbtodb
    }

    /*
     * Cylinder group macros to locate things in cylinder groups.
     * They calc file system addresses of cylinder group data structures.
     */
    fn cgbase(&self, c: i64) -> i64 {
        self.fs_fpg as i64 * c
    }
    /* data zone */
    fn cgdata(&self, c: i64) -> i64 {
        self.cgdmin(c) + self.fs_minfree as i64
    }
    /* meta data */
    fn cgmeta(&self, c: i64) -> i64 {
        self.cgdmin(c)
    }
    /* 1st data */
    fn cgdmin(&self, c: i64) -> i64 {
        self.cgstart(c) + self.fs_dblkno as i64
    }
    /* inode blk */
    fn cgimin(&self, c: i64) -> i64 {
        self.cgstart(c) + self.fs_iblkno as i64
    }
    /* super blk */
    fn cgsblock(&self, c: i64) -> i64 {
        self.cgstart(c) + self.fs_sblkno as i64
    }
    /* cg block */
    fn cgtod(&self, c: i64) -> i64 {
        self.cgstart(c) + self.fs_cblkno as i64
    }
    fn cgstart(&self, c: i64) -> i64 {
        self.cgbase(c) + self.fs_cgoffset as i64 * (c & !self.fs_cgmask as i64)
    }

    /*
     * Macros for handling inode numbers:
     *     inode number to file system block offset.
     *     inode number to cylinder group number.
     *     inode number to file system block address.
     */
    fn ino_to_cg(&self, x: i64) -> i64 {
        x / self.fs_ipg as i64
    }
    fn ino_to_fsba(&self, x: i64) -> i64 {
        self.cgimin(self.ino_to_cg(x))
            + self.blkstofrags((x % self.fs_ipg as i64) / self.fs_inopb as i64)
    }
    fn ino_to_fsbo(&self, x: i64) -> i64 {
        x % self.fs_inopb as i64
    }

    /*
     * Give cylinder group number for a file system block.
     * Give frag block number in cylinder group for a file system block.
     */
    fn dtog(&self, d: i64) -> i64 {
        d / self.fs_fpg as i64
    }
    fn dtogd(&self, d: i64) -> i64 {
        d % self.fs_fpg as i64
    }

    /*
     * Number of disk sectors per block/fragment; assumes DEV_BSIZE byte
     * sector size.
     */
    fn nspb(&self) -> i64 {
        (self.fs_nspf as i64) << (self.fs_fragshift as i64)
    }

    /* Number of inodes per file system fragment (fs->fs_fsize) */
    fn inopf(&self) -> i64 {
        self.fs_inopb as i64 >> self.fs_fragshift as i64
    }

    /*
     * The following macros optimize certain frequently calculated
     * quantities by using shifts and masks in place of divisions
     * modulos and multiplications.
     */

    /* calculates (loc % fs->fs_bsize) */
    fn blkoff(&self, loc: i64) -> i64 {
        loc & self.fs_qbmask
    }

    /* calculates (loc % fs->fs_fsize) */
    fn fragoff(&self, loc: i64) -> i64 {
        loc & self.fs_qfmask
    }

    /*
    #define lblktosize(fs, blk)	/* calculates ((off_t)blk * fs->fs_bsize) */ \
        ((off_t)(blk) << (fs)->fs_bshift)
    #define lblkno(fs, loc)		/* calculates (loc / fs->fs_bsize) */ \
        ((loc) >> (fs)->fs_bshift)
    */

    /* calculates (loc / fs->fs_fsize) */
    fn numfrags(&self, loc: i64) -> i64 {
        loc >> self.fs_fshift
    }

    /* calculates roundup(size, fs->fs_bsize) */
    fn blkroundup(&self, size: i64) -> i64 {
        (size + self.fs_qbmask) & self.fs_bmask as i64
    }

    /* calculates roundup(size, fs->fs_fsize) */
    fn fragroundup(&self, size: i64) -> i64 {
        (size + self.fs_qfmask) & self.fs_fmask as i64
    }
    /* calculates (frags / fs->fs_frag) */
    fn fragstoblks(&self, frag: i64) -> i64 {
        frag >> self.fs_fragshift
    }

    /* calculates (blks * fs->fs_frag) */
    fn blkstofrags(&self, blks: i64) -> i64 {
        blks << self.fs_fragshift as i64
    }
    fn fragnum(&self, fsb: i64) -> i64 {
        fsb & (self.fs_frag as i64 - 1)
    }
    fn blknum(&self, fsb: i64) -> i64 {
        fsb & !(self.fs_frag as i64 - 1)
    }
}

fn howmany(a: usize, b: usize) -> usize {
    (a + b - 1) / b
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlkPos {
    Direct(u16),
    Indirect1(u16),
    Indirect2(u16, u16),
    Indirect3(u16, u16, u16),
}

fn setbit(buf: &mut [u8], idx: usize) {
    buf[idx / 8] |= 1 << (idx % 8);
}
fn clrbit(buf: &mut [u8], idx: usize) {
    buf[idx / 8] &= !(1 << (idx % 8));
}
fn isset(buf: &[u8], idx: usize) -> bool {
    buf[idx / 8] & (1 << (idx % 8)) != 0
}
fn isclr(buf: &[u8], idx: usize) -> bool {
    buf[idx / 8] & (1 << (idx % 8)) == 0
}

fn direct_append(blkbuf: &mut [u8], direct: Direct, name: &[u8]) -> bool {
    assert_eq!(direct.d_namlen as usize, name.len());

    let mut offset = 0;
    while blkbuf.len() >= offset + directsiz(0) {
        let dp: &Direct = unsafe { transmute(&blkbuf[offset]) };
        if dp.d_ino == 0 {
            break;
        }

        let sz = directsiz(dp.d_namlen);
        offset += sz;
        continue;
    }

    let sz = directsiz(name.len() as u8);
    if offset + sz > blkbuf.len() {
        return false;
    }

    // write direct
    {
        let dp: &mut Direct = unsafe { transmute(&mut blkbuf[offset]) };
        *dp = direct;
    }

    // write name
    {
        let namebuf: &mut [u8] = unsafe {
            let buf: *mut u8 = transmute(&mut blkbuf[offset + std::mem::size_of::<Direct>()]);
            from_raw_parts_mut(buf, direct.d_namlen as usize)
        };
        namebuf.copy_from_slice(name);
    }

    true
}

fn direct_parse(blk: &[u8]) -> Vec<(&Direct, &str)> {
    let mut out = Vec::new();

    let mut blkdata = blk;
    while blkdata.len() >= directsiz(0) {
        let dp: &Direct = unsafe { transmute(&blkdata[0]) };
        if dp.d_ino == 0 {
            break;
        }

        let sz = directsiz(dp.d_namlen);
        let namebuf: &[u8] = unsafe {
            let buf: *const u8 = transmute(&blkdata[std::mem::size_of::<Direct>()]);
            from_raw_parts(buf, dp.d_namlen as usize)
        };

        blkdata = &blkdata[sz..];

        let filename = if let Ok(s) = std::str::from_utf8(&namebuf) {
            s
        } else {
            "???"
        };

        out.push((dp, filename));
    }
    out
}

// extra helpers
impl Fs {
    fn check0(&self) {
        // consistency check
        assert!(self.fs_ncg >= 1);
        assert!(self.fs_cpg >= 1);

        if self.fs_magic == FS_UFS1_MAGIC {
            if self.fs_ncg as i32 * self.fs_cpg < self.fs_ncyl
                || (self.fs_ncg as i32 - 1) * self.fs_cpg >= self.fs_ncyl
            {
                todo!();
            }
        }

        assert!((self.fs_sbsize as usize) < SBSIZE);
        assert!((self.fs_bsize as usize).is_power_of_two());
        assert!((self.fs_bsize as usize) >= MINBSIZE);
        assert!((self.fs_bsize as usize) <= MAXBSIZE);

        assert!((self.fs_fsize as usize).is_power_of_two());
        assert!(self.fs_fsize <= self.fs_bsize);
        assert!(self.fs_fsize >= self.fs_bsize / MAXFRAG as i32);

        let blkidxsz = if self.v2() {
            std::mem::size_of::<i64>()
        } else {
            std::mem::size_of::<i32>()
        };
        assert_eq!(self.fs_nindir, self.fs_bsize / blkidxsz as i32);
    }

    fn fs_alt(&self, buf: &[u8]) -> Self {
        let blk_alt = self.cgsblock(self.fs_ncg as i64 - 1);
        let offset_alt = self.fsbtodb(blk_alt) as usize * DEV_BSIZE;

        let fs_alt: &Fs = unsafe { transmute(&buf[offset_alt]) };
        fs_alt.clone()
    }

    fn cgbuf_mut<'a, 'b>(&'a self, buf: &'b mut [u8], c: usize) -> &'b mut [u8] {
        assert!(c <= self.fs_ncg as usize);

        let offset = self.fsbtodb(self.cgtod(c as i64)) as usize * DEV_BSIZE;
        &mut buf[offset..(offset + self.fs_cgsize as usize)]
    }

    fn cgbuf<'a, 'b>(&'a self, buf: &'b [u8], c: usize) -> &'b [u8] {
        assert!(c <= self.fs_ncg as usize);

        let offset = self.fsbtodb(self.cgtod(c as i64)) as usize * DEV_BSIZE;
        &buf[offset..(offset + self.fs_cgsize as usize)]
    }

    fn cg(&self, buf: &[u8], c: usize) -> Cg {
        let cgbuf = self.cgbuf(buf, c);
        let cg: &Cg = unsafe { transmute(&cgbuf[0]) };
        assert_eq!(cg.cg_magic, CG_MAGIC);

        cg.clone()
    }

    fn blk0_range(&self, blk: i64) -> std::ops::Range<usize> {
        let start = (blk * self.fs_fsize as i64) as usize;
        let end = ((blk + self.fs_frag as i64) * self.fs_fsize as i64) as usize;
        start..end
    }

    fn blk0_mut<'a, 'b>(&'a self, buf: &'b mut [u8], blk: i64) -> &'b mut [u8] {
        &mut buf[self.blk0_range(blk)]
    }

    fn blk0<'a, 'b>(&'a self, buf: &'b [u8], blk: i64) -> &'b [u8] {
        &buf[self.blk0_range(blk)]
    }

    fn blk_indir_at<'a, 'b>(&'a self, buf: &'b [u8], blk: i64, idx: usize) -> i64 {
        let nindir = self.fs_nindir as usize;
        assert!(idx < nindir);

        let indir = self.blk0(buf, blk);
        if self.v2() {
            let s = unsafe { from_raw_parts::<i64>(transmute(indir.as_ptr()), nindir) };
            s[idx]
        } else {
            let s = unsafe { from_raw_parts::<i32>(transmute(indir.as_ptr()), nindir) };
            s[idx] as i64
        }
    }

    fn blk_indir_set<'a, 'b>(&'a self, buf: &'b mut [u8], blk: i64, idx: usize, val: i64) {
        let nindir = self.fs_nindir as usize;
        assert!(idx < nindir);

        let indir = self.blk0(buf, blk);

        if self.v2() {
            let s = unsafe { from_raw_parts_mut::<i64>(transmute(indir.as_ptr()), nindir) };
            s[idx] = val;
        } else {
            let s = unsafe { from_raw_parts_mut::<i32>(transmute(indir.as_ptr()), nindir) };
            s[idx] = val as i32;
        }
    }

    fn blkpos<'a>(&'a self, blkno: usize) -> BlkPos {
        if blkno < NDADDR {
            return BlkPos::Direct(blkno as u16);
        }

        let ref_per_blk = self.fs_nindir as usize;

        let mut blkno = blkno - NDADDR;
        if blkno < ref_per_blk {
            return BlkPos::Indirect1(blkno as u16);
        }

        let ref_per_blk2 = ref_per_blk * ref_per_blk;

        blkno -= ref_per_blk;
        if blkno < ref_per_blk2 {
            let b0 = blkno / ref_per_blk;
            let b1 = blkno - b0 * ref_per_blk;
            return BlkPos::Indirect2(b0 as u16, b1 as u16);
        }
        blkno -= ref_per_blk2;

        let ref_per_blk3 = ref_per_blk2 * ref_per_blk;
        if blkno < ref_per_blk3 {
            let b0 = blkno / ref_per_blk2;
            blkno -= b0 * ref_per_blk2;
            let b1 = blkno / ref_per_blk;
            let b2 = blkno - b0 * ref_per_blk;
            return BlkPos::Indirect3(b0 as u16, b1 as u16, b2 as u16);
        } else {
            todo!();
        }
    }

    fn blkat_mut<'a, 'b, 'c>(
        &'a self,
        buf: &'b mut [u8],
        ino: &'c Ufs2Dinode,
        blkno: usize,
    ) -> &'b mut [u8] {
        match self.blkpos(blkno) {
            BlkPos::Direct(blkno) => {
                let blk = ino.di_db[blkno as usize];
                self.blk0_mut(buf, blk)
            }
            BlkPos::Indirect1(b0) => {
                self.blk0_mut(buf, self.blk_indir_at(buf, ino.di_ib[0], b0 as usize))
            }
            BlkPos::Indirect2(b0, b1) => {
                let indir0 = self.blk_indir_at(buf, ino.di_ib[1], b0 as usize);
                self.blk0_mut(buf, self.blk_indir_at(buf, indir0, b1 as usize))
            }
            BlkPos::Indirect3(b0, b1, b2) => {
                let indir0 = self.blk_indir_at(buf, ino.di_ib[2], b0 as usize);
                let indir1 = self.blk_indir_at(buf, indir0, b1 as usize);
                self.blk0_mut(buf, self.blk_indir_at(buf, indir1, b1 as usize))
            }
        }
    }

    fn blkat<'a, 'b>(&'a self, buf: &'a [u8], ino: &'b Ufs2Dinode, blkno: usize) -> &'a [u8] {
        match self.blkpos(blkno) {
            BlkPos::Direct(blkno) => {
                let blk = ino.di_db[blkno as usize];
                self.blk0(buf, blk)
            }
            BlkPos::Indirect1(b0) => {
                self.blk0(buf, self.blk_indir_at(buf, ino.di_ib[0], b0 as usize))
            }
            BlkPos::Indirect2(b0, b1) => {
                let indir0 = self.blk_indir_at(buf, ino.di_ib[1], b0 as usize);
                self.blk0(buf, self.blk_indir_at(buf, indir0, b1 as usize))
            }
            BlkPos::Indirect3(b0, b1, b2) => {
                let indir0 = self.blk_indir_at(buf, ino.di_ib[2], b0 as usize);
                let indir1 = self.blk_indir_at(buf, indir0, b1 as usize);
                self.blk0(buf, self.blk_indir_at(buf, indir1, b2 as usize))
            }
        }
    }

    fn read(&self, buf: &[u8], ino: &Ufs2Dinode) -> Vec<u8> {
        let mut out = Vec::with_capacity(ino.di_size as usize);

        while out.len() < ino.di_size as usize {
            let blkno = out.len() / self.fs_bsize as usize;
            let blk = self.blkat(buf, ino, blkno);
            let blkbuf = &blk[..blk.len().min(ino.di_size as usize - out.len())];
            out.extend_from_slice(blkbuf);
        }

        assert!(
            out.len() == ino.di_size as usize,
            "{}/{}",
            out.len(),
            ino.di_size
        );

        out
    }
}

impl Fs {
    fn v2(&self) -> bool {
        self.fs_magic == FS_UFS2_MAGIC
    }

    fn blk_free<'a, 'b>(&self, buf: &mut [u8], blkno: i64, frags: i64) {
        let c = self.dtog(blkno) as usize;

        let fragoff = blkno % self.fs_frag as i64;
        let fragrem = self.fs_frag as i64 - fragoff;
        assert!(fragrem >= frags);
        if fragoff == 0 {
            assert_eq!(frags, 8);
        }

        // clear bitmap
        let map = self.cg_blksfree_mut(buf, c);
        for blk in blkno..(blkno + frags) {
            assert!(isset(map, blk as usize));
            clrbit(map, blk as usize);
        }
    }

    // allocate whole block
    fn blk_alloc<'a, 'b>(&self, buf: &mut [u8], nblk: i64) -> i64 {
        assert_eq!(nblk, self.fs_frag as i64);

        let c = 0;
        let cg = self.cg(buf, c);
        let blksfreemap = self.cg_blksfree_mut(buf, c);

        let blks_per_cg = (cg.cg_ndblk / self.fs_frag as u32) as i64;
        for blk in 0..blks_per_cg {
            let blkno = self.blkstofrags(blk);
            let mut filled = false;
            for i in 0..self.fs_frag as i64 {
                if isclr(blksfreemap, (blkno + i) as usize) {
                    filled = true;
                    break;
                }
            }

            if filled {
                continue;
            }

            for i in 0..self.fs_frag as i64 {
                clrbit(blksfreemap, (blkno + i) as usize);
            }
            let blkbuf = self.blk0_mut(buf, blkno);
            blkbuf.fill(0);

            // TODO: accounting
            return blkno;
        }
        todo!("blk_alloc");
    }

    fn dinode<'a, 'b>(&self, buf: &[u8], inumber: usize) -> Ufs2Dinode {
        let blkno = self.ino_to_fsba(inumber as i64);
        let blkbuf = self.blk0(buf, blkno);

        let ino_size = self.fs_bsize as u32 / self.fs_inopb;
        let offset = (self.ino_to_fsbo(inumber as i64) as u64 * ino_size as u64) as usize;

        if self.v2() {
            let dinode: &Ufs2Dinode = unsafe { transmute(&blkbuf[offset]) };
            *dinode
        } else {
            let dinode1: &Ufs1Dinode = unsafe { transmute(&blkbuf[offset]) };
            Ufs2Dinode::from(*dinode1)
        }
    }

    fn dinode_mut<'a, 'b, F, R>(&self, buf: &mut [u8], inumber: usize, f: F) -> R
    where
        F: FnOnce(&mut Ufs2Dinode) -> R,
    {
        let blkno = self.ino_to_fsba(inumber as i64);
        let blkbuf = self.blk0_mut(buf, blkno);

        let ino_size = (self.fs_bsize / self.fs_inopb as i32) as u64;
        let offset = (self.ino_to_fsbo(inumber as i64) as u64 * ino_size) as usize;

        if self.v2() {
            let dinode: &mut Ufs2Dinode = unsafe { transmute(&mut blkbuf[offset]) };
            f(dinode)
        } else {
            let dinode1: &mut Ufs1Dinode = unsafe { transmute(&mut blkbuf[offset]) };
            let mut dinode = Ufs2Dinode::from(*dinode1);
            let ret = f(&mut dinode);
            *dinode1 = Ufs1Dinode::from(dinode);
            ret
        }
    }

    fn dinode_free<'a, 'b>(&self, buf: &mut [u8], inumber: usize) {
        self.dinode_mut(buf, inumber, |inode| {
            *inode = Ufs2Dinode::default();
        });

        let c = self.ino_to_cg(inumber as i64) as usize;
        let map = self.cg_inosused_mut(buf, c);

        assert!(isset(map, inumber));
        clrbit(map, inumber);
    }

    fn dinode_alloc<'a, 'b>(&self, buf: &mut [u8], inumber: usize, inode: &Ufs2Dinode) {
        let c = self.ino_to_cg(inumber as i64) as usize;
        let map = self.cg_inosused_mut(buf, c);
        let mapidx = inumber % self.fs_ipg as usize;

        assert!(isclr(map, mapidx));
        setbit(map, mapidx);

        self.dinode_update(buf, inumber, inode);
    }

    fn dinode_update<'a, 'b>(&self, buf: &mut [u8], inumber: usize, inode: &Ufs2Dinode) {
        self.dinode_mut(buf, inumber, |inode1| {
            *inode1 = *inode;
        })
    }

    fn blkpos_next(&self, pos: BlkPos) -> BlkPos {
        use BlkPos::*;

        let nindir = self.fs_nindir as u16;
        match pos {
            Direct(n) => {
                if n < NDADDR as u16 - 1 {
                    Direct(n + 1)
                } else {
                    Indirect1(0)
                }
            }
            Indirect1(n) => {
                if n < nindir - 1 {
                    Indirect1(n + 1)
                } else {
                    Indirect2(0, 0)
                }
            }
            Indirect2(n0, n1) => {
                if n1 < nindir - 1 {
                    Indirect2(n0, n1)
                } else {
                    if n0 < nindir - 1 {
                        Indirect2(n0 + 1, 0)
                    } else {
                        Indirect3(0, 0, 0)
                    }
                }
            }
            Indirect3(n0, n1, n2) => {
                if n2 == nindir - 1 {
                    Indirect3(n0, n1, n2 + 1)
                } else {
                    if n1 < nindir - 1 {
                        Indirect3(n0, n1 + 1, 0)
                    } else {
                        assert!(n0 < nindir - 1);
                        Indirect3(n0 + 1, 0, 0)
                    }
                }
            }
        }
    }

    fn dinode_maybe_alloc_indir(&self, buf: &mut [u8], indirblkno: i64, idx: usize) -> (i64, bool) {
        match self.blk_indir_at(buf, indirblkno, idx) {
            0 => {
                let blkno = self.blk_alloc(buf, self.fs_frag as i64);
                self.blk_indir_set(buf, indirblkno, idx, blkno);
                (blkno, true)
            }
            blkno => (blkno, false),
        }
    }

    fn dinode_allocat(&self, buf: &mut [u8], dinode: &mut Ufs2Dinode, pos: BlkPos) -> (i64, bool) {
        use BlkPos::*;
        match pos {
            Direct(n) => {
                if dinode.di_db[n as usize] == 0 {
                    dinode.di_db[n as usize] = self.blk_alloc(buf, self.fs_frag as i64);
                    (dinode.di_db[n as usize], true)
                } else {
                    (dinode.di_db[n as usize], false)
                }
            }
            Indirect1(n) => {
                if dinode.di_ib[0] == 0 {
                    dinode.di_ib[0] = self.blk_alloc(buf, self.fs_frag as i64);
                }

                self.dinode_maybe_alloc_indir(buf, dinode.di_ib[0], n as usize)
            }
            Indirect2(n0, n1) => {
                if dinode.di_ib[1] == 0 {
                    dinode.di_ib[1] = self.blk_alloc(buf, self.fs_frag as i64);
                }
                let (blkno0, _) = self.dinode_maybe_alloc_indir(buf, dinode.di_ib[1], n0 as usize);
                self.dinode_maybe_alloc_indir(buf, blkno0, n1 as usize)
            }
            Indirect3(n0, n1, n2) => {
                if dinode.di_ib[2] == 0 {
                    dinode.di_ib[2] = self.blk_alloc(buf, self.fs_frag as i64);
                }
                let (blkno0, _) = self.dinode_maybe_alloc_indir(buf, dinode.di_ib[2], n0 as usize);
                let (blkno1, _) = self.dinode_maybe_alloc_indir(buf, blkno0, n1 as usize);
                self.dinode_maybe_alloc_indir(buf, blkno1, n2 as usize)
            }
        }
    }

    // do not modify di_size, only allocate di_blocks if required
    fn dinode_realloc(&self, buf: &mut [u8], dinode: &mut Ufs2Dinode, blocks: i64) {
        if dinode.di_blocks >= blocks as u64 {
            return;
        }

        // if there's a partial block
        if dinode.di_blocks & (self.fs_frag as u64 - 1) != 0 {
            todo!();
        }

        let mut pos_prev = self.blkpos(self.fragstoblks(dinode.di_blocks as i64) as usize);
        let pos_next = self.blkpos(self.fragstoblks(blocks) as usize);

        while pos_prev != pos_next {
            if let (_, true) = self.dinode_allocat(buf, dinode, pos_prev) {
                dinode.di_blocks += self.fs_frag as u64;
            }
            pos_prev = self.blkpos_next(pos_prev);
        }

        // partial to full block
        if dinode.di_blocks < self.fs_frag as u64 {
            todo!();
        }
    }

    fn dir_read<'a>(&'a self, buf: &'a [u8], inumber: usize) -> Vec<(&'a Direct, &'a str)> {
        let dinode = self.dinode(buf, inumber);
        eprintln!("dir_read: dinode={:?}", dinode);

        let mode = dinode.di_mode as usize & IFMT;
        if mode & IFDIR == 0 {
            todo!();
        }

        let mut out = Vec::new();
        let mut read = 0;
        while read < dinode.di_size as usize {
            let blkno = read / self.fs_bsize as usize;
            let blkbuf = self.blkat(buf, &dinode, blkno);
            let dirs = direct_parse(&blkbuf);
            out.extend(dirs);
            read += blkbuf.len();
        }

        out
    }

    fn dir_lookup(&self, buf: &mut [u8], inumber_p: usize, name: &str) -> Option<Direct> {
        let dinode = self.dinode(buf, inumber_p);

        let mode = dinode.di_mode as usize & IFMT;
        assert_ne!(mode & IFDIR, 0);

        let mut read = 0;
        while read < dinode.di_size as usize {
            let blkno = read / self.fs_bsize as usize;
            let blkbuf = self.blkat(buf, &dinode, blkno);
            read += blkbuf.len();
            for (direct, filename) in direct_parse(&blkbuf) {
                if filename == name {
                    return Some(*direct);
                }
            }
        }
        None
    }

    fn dir_delete(&self, buf: &mut [u8], inumber_p: usize, name: &str) -> Option<Direct> {
        let dinode_p = self.dinode(buf, inumber_p);

        let mode = dinode_p.di_mode as usize & IFMT;
        assert_ne!(mode & IFDIR, 0);

        let mut ret = None;
        let mut blkbuf_out = Vec::with_capacity(self.fs_bsize as usize);
        blkbuf_out.resize(self.fs_bsize as usize, 0);

        let mut read = 0;
        while read < dinode_p.di_size as usize {
            let blkno = read / self.fs_bsize as usize;
            let blkbuf = self.blkat_mut(buf, &dinode_p, blkno);
            read += blkbuf.len();

            let found = direct_parse(&blkbuf)
                .into_iter()
                .find(|(_direct, filename)| *filename == name)
                .is_some();

            if !found {
                continue;
            }

            let mut found = false;
            for (direct, filename) in direct_parse(&blkbuf) {
                if filename == name {
                    ret = Some(*direct);
                    found = true;
                    continue;
                }
                direct_append(&mut blkbuf_out, *direct, filename.as_bytes());
            }

            assert!(found);
            blkbuf.copy_from_slice(&blkbuf_out);
            break;
        }

        ret
    }

    fn dir_append(&self, buf: &mut [u8], dinode: &mut Ufs2Dinode, direct: Direct, name: &[u8]) {
        let frag = self.fs_frag as usize;
        loop {
            let mut nblk = howmany(dinode.di_blocks as usize, frag) as u64;
            if nblk == 0 {
                self.dinode_realloc(buf, dinode, frag as i64);
                nblk = howmany(dinode.di_blocks as usize, frag) as u64;
            }
            assert!(nblk > 0);

            let lastblk = nblk - 1;
            let blk = self.blkat_mut(buf, dinode, lastblk as usize);
            if !direct_append(blk, direct, name) {
                self.dinode_realloc(buf, dinode, (nblk + 1) as i64 * frag as i64);
                continue;
            }

            dinode.di_size = nblk * self.fs_bsize as u64;
            break;
        }
    }

    fn inode_alloc<'a, 'b>(&'a self, buf: &'b mut [u8], dinode: &Ufs2Dinode) -> usize {
        for c in 0..self.fs_ncg {
            let ino = {
                let map = self.cg_inosused_mut(buf, c as usize);
                let mut ino = 0usize;
                while ino < self.fs_ipg as usize {
                    if !isset(map, ino) {
                        break;
                    }
                    ino += 1;
                }
                if ino == self.fs_ipg as usize {
                    continue;
                }

                (self.fs_ipg * c) as usize + ino
            };

            self.dinode_alloc(buf, ino, dinode);
            return ino;
        }
        panic!("out of inodes");
    }
}

#[repr(C)]
struct Bufarea {
    b_bno: usize,
    b_next: usize,
    b_prev: usize,
    b_size: u32,
    b_errs: u32,
    b_flags: u32,

    b_un: usize,
    b_dirty: usize,
}

fn fs(buf: &[u8]) -> Fs {
    for offset in SBLOCKSEARCH {
        if offset + SBSIZE >= buf.len() {
            continue;
        }

        let fs: &Fs = unsafe { transmute(&buf[offset]) };

        if fs.fs_magic != FS_UFS1_MAGIC && fs.fs_magic != FS_UFS2_MAGIC {
            continue;
        }
        if fs.fs_magic == FS_UFS1_MAGIC && offset == SBLOCK_UFS2 {
            continue;
        }
        if fs.fs_magic == FS_UFS2_MAGIC && fs.fs_sblockloc != offset as i64 {
            continue;
        }

        return fs.clone();
    }
    todo!();
}

fn fsck(buf: &[u8]) {
    let fs = fs(buf);

    let fs_v2 = fs.fs_magic == FS_UFS2_MAGIC;

    info!("sb={:?}, v2={}", fs, fs_v2);

    fs.check0();

    let fs_alt = fs.fs_alt(&buf);
    if fs != fs_alt {
        error!("superblock mismatch");
        // assert_eq!(fs, fs_alt);
    }

    // pass0
    for c in 0..fs.fs_ncg {
        let cg = fs.cg(buf, c as usize);

        let inosusedmap = fs.cg_inosused(buf, c as usize);
        let blksfreemap = fs.cg_blksfree(buf, c as usize);
        info!("cg #{}: {:?}", c, cg,);

        let inosused = if fs_v2 {
            cg.cg_initediblk.min(fs.fs_ipg)
        } else {
            fs.fs_ipg
        };

        info!(
            "inosused={}, {}/{:?}",
            inosused,
            inosusedmap.len(),
            inosusedmap,
        );
        debug!("blksfree={}/{:?}", blksfreemap.len(), blksfreemap,);

        let ino_start = fs.fs_ipg * c;
        let ino_end = ino_start + inosused;

        for inumber in ino_start..ino_end {
            let ino = fs.dinode(buf, inumber as usize);

            let mode = ino.di_mode as usize & IFMT;
            if ino.di_mode == 0 {
                let d = Ufs2Dinode::default();
                assert_eq!(d.di_db, ino.di_db);
                assert_eq!(d.di_ib, ino.di_ib);
                assert_eq!(d.di_mode, 0);
                assert_eq!(d.di_size, 0);
                continue;
            }

            assert!(ino.di_size <= fs.fs_maxfilesize);

            if mode == IFDIR {
                assert!(ino.di_size <= MAXDIRSIZE as u64);
            }

            let lndb = (ino.di_size + fs.fs_bsize as u64 - 1) / fs.fs_bsize as u64;
            assert!(lndb <= std::i32::MAX as u64);

            let mut ndb = lndb;
            if mode == IFBLK || mode == IFCHR {
                ndb += 1;
            }
            if mode == IFLNK {
                // TODO
                continue;
            }

            // direct blocks
            for j in ndb..NDADDR as u64 {
                assert_eq!(ino.di_db[j as usize], 0);
            }

            // indirect blocks
            if ndb > NDADDR as u64 {
                let mut nib = 0;
                let mut ndb = ndb - NDADDR as u64;
                while ndb > 0 {
                    ndb = ndb / fs.fs_nindir as u64;
                    nib += 1;
                }
                for j in nib..NIADDR {
                    assert_eq!(ino.di_ib[j as usize], 0);
                }
            }

            if ino.di_nlink <= 0 {
                error!("zero-link inode: ino={}", inumber);
            }

            let data = fs.read(buf, &ino);

            debug!(
                "ino={}, nlink={}, ctime={}, mode={:0o}, size={}/{}/{}",
                inumber,
                ino.di_nlink,
                ino.di_ctime,
                mode,
                ino.di_size,
                data.len(),
                lndb,
            );

            if mode == IFREG {
                if let Ok(s) = std::str::from_utf8(&data) {
                    trace!("content={}", s);
                }
            } else {
                let dirblks = data.len() / DEV_BSIZE;

                for dirblk in 0..dirblks {
                    let mut blkdata = &data[dirblk * DEV_BSIZE..(dirblk + 1) * DEV_BSIZE];

                    while blkdata.len() >= directsiz(0) {
                        let dp: &Direct = unsafe { transmute(&blkdata[0]) };
                        if dp.d_ino == 0 {
                            break;
                        }

                        let sz = directsiz(dp.d_namlen);
                        let namebuf: &[u8] = unsafe {
                            let buf: *const u8 = transmute(&blkdata[std::mem::size_of::<Direct>()]);
                            from_raw_parts(buf, dp.d_namlen as usize)
                        };

                        blkdata = &blkdata[sz..];

                        let filename = if let Ok(s) = std::str::from_utf8(&namebuf) {
                            s
                        } else {
                            "???"
                        };

                        if filename == "." || filename == ".." {
                            continue;
                        }

                        trace!("{}, {:?}, {}", filename, dp, sz);
                    }
                }
            }
        }
    }
}

use fuser::*;

use libc::ENOENT;
use std::ffi::OsStr;

const TTL: Duration = Duration::from_secs(1); // 1 second

struct FFS<'a> {
    buf: &'a mut [u8],
    fs: Fs,
}

fn dinode_attr(ino: u64, dinode: &Ufs2Dinode) -> FileAttr {
    let kind = if dinode.di_mode & IFDIR as u16 != 0 {
        FileType::Directory
    } else if dinode.di_mode & IFREG as u16 != 0 {
        FileType::RegularFile
    } else {
        todo!("unknown file type");
    };

    FileAttr {
        ino,
        size: dinode.di_size as u64,
        blocks: 0,
        atime: UNIX_EPOCH + std::time::Duration::from_secs(dinode.di_atime as u64),
        mtime: UNIX_EPOCH + std::time::Duration::from_secs(dinode.di_mtime as u64),
        ctime: UNIX_EPOCH + std::time::Duration::from_secs(dinode.di_ctime as u64),
        crtime: UNIX_EPOCH + std::time::Duration::from_secs(dinode.di_birthtime as u64),
        kind,
        perm: dinode.di_mode,
        nlink: dinode.di_nlink as u32,
        uid: dinode.di_uid,
        gid: dinode.di_gid,
        rdev: 0,
        flags: 0,
        blksize: 512,
    }
}

impl<'a> Filesystem for FFS<'a> {
    /// Look up a directory entry by name and get its attributes.
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        info!("lookup: parent={}, name={:?}", parent, name);

        let inumber_p = parent + 1;
        let name = name.to_str().unwrap();
        let direct = match self.fs.dir_lookup(self.buf, inumber_p as usize, name) {
            Some(direct) => direct,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let dinode = self.fs.dinode(self.buf, direct.d_ino as usize);

        let inumber = direct.d_ino as u64 - 1;
        reply.entry(&TTL, &dinode_attr(inumber, &dinode), inumber);
    }

    /// Get file attributes.
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let inumber = (ino + 1) as usize;
        info!("getattr: ino={}", ino);

        let dinode = self.fs.dinode(self.buf, inumber);
        if dinode.di_mode == 0 {
            reply.error(ENOENT);
            return;
        }

        reply.attr(&TTL, &dinode_attr(ino, &dinode));
    }

    /// Set file attributes.
    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let inumber = (ino + 1) as usize;
        let mut dinode = self.fs.dinode(self.buf, inumber);

        if let Some(mode) = mode {
            dinode.di_mode = mode as u16;
        }
        if let Some(uid) = uid {
            dinode.di_uid = uid;
        }
        if let Some(gid) = gid {
            dinode.di_gid = gid;
        }
        if let Some(size) = size {
            dinode.di_size = size as u64;
        }
        if let Some(atime) = atime {
            let d = match atime {
                TimeOrNow::SpecificTime(t) => t.duration_since(UNIX_EPOCH).unwrap(),
                TimeOrNow::Now => SystemTime::now().duration_since(UNIX_EPOCH).unwrap(),
            };
            dinode.di_atime = d.as_secs() as i64;
            dinode.di_atimensec = d.subsec_nanos() as i32;
        }
        if let Some(mtime) = mtime {
            let d = match mtime {
                TimeOrNow::SpecificTime(t) => t.duration_since(UNIX_EPOCH).unwrap(),
                TimeOrNow::Now => SystemTime::now().duration_since(UNIX_EPOCH).unwrap(),
            };
            dinode.di_mtime = d.as_secs() as i64;
            dinode.di_mtimensec = d.subsec_nanos() as i32;
        }
        if let Some(ctime) = ctime {
            let d = ctime.duration_since(UNIX_EPOCH).unwrap();
            dinode.di_ctime = d.as_secs() as i64;
            dinode.di_ctimensec = d.subsec_nanos() as i32;
        }
        if let Some(flags) = flags {
            dinode.di_flags = flags;
        }

        self.fs.dinode_update(self.buf, inumber, &dinode);

        reply.attr(&TTL, &dinode_attr(ino, &dinode));
    }

    /// Create file node.
    /// Create a regular file, character device, block device, fifo or socket node.
    fn mknod(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        let d = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => d,
            Err(_) => {
                reply.error(libc::ENOSYS);
                return;
            }
        };

        let inumber_p = (parent + 1) as usize;
        let mut dinode_p = self.fs.dinode(self.buf, inumber_p);
        info!("dinode_p={:?}", dinode_p);

        let mut dinode = Ufs2Dinode::empty(d);
        dinode.di_mode = mode as u16;
        dinode.di_nlink = 1;

        // allocate ino
        let inumber = self.fs.inode_alloc(self.buf, &dinode);

        // update directory
        let name = name.to_str().unwrap();
        let direct = Direct {
            d_ino: inumber as u32,
            d_reclen: 0,
            d_type: DT_REG,
            d_namlen: name.len() as u8,
        };
        self.fs
            .dir_append(self.buf, &mut dinode_p, direct, name.as_bytes());

        // update inode
        self.fs.dinode_update(self.buf, inumber_p, &dinode_p);
        {
            dinode_p.di_nlink += 1;
            self.fs.dinode_update(self.buf, inumber_p, &dinode_p);
        }

        let ino = (inumber - 1) as u64;
        reply.entry(&TTL, &dinode_attr(ino, &dinode), ino);
    }

    /// Create a directory.
    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let d = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => d,
            Err(_) => {
                reply.error(libc::ENOSYS);
                return;
            }
        };

        let inumber_p = (parent + 1) as usize;
        let mut dinode_p = self.fs.dinode(self.buf, inumber_p);
        info!("dinode_p={:?}", dinode_p);

        // make child directory
        // allocate new dinode
        let mut dinode = Ufs2Dinode::empty(d);
        dinode.di_mode = IFDIR as u16 | (mode & 0xff) as u16;
        dinode.di_nlink = 2;

        // allocate ino
        let inumber = self.fs.inode_alloc(self.buf, &dinode);

        // default directory entry
        self.fs.dir_append(
            self.buf,
            &mut dinode,
            Direct {
                d_ino: inumber as u32,
                d_reclen: 0,
                d_type: DT_DIR,
                d_namlen: 1,
            },
            ".".as_bytes(),
        );
        self.fs.dir_append(
            self.buf,
            &mut dinode,
            Direct {
                d_ino: inumber_p as u32,
                d_reclen: 0,
                d_type: DT_DIR,
                d_namlen: 2,
            },
            "..".as_bytes(),
        );

        // parent to child
        let name = name.to_str().unwrap();
        self.fs.dir_append(
            self.buf,
            &mut dinode_p,
            Direct {
                d_ino: inumber as u32,
                d_reclen: 0,
                d_type: DT_DIR,
                d_namlen: name.len() as u8,
            },
            name.as_bytes(),
        );

        dinode_p.di_nlink += 1;

        self.fs.dinode_update(self.buf, inumber, &dinode);
        self.fs.dinode_update(self.buf, inumber_p, &dinode_p);

        let ino = (inumber - 1) as u64;
        reply.entry(&TTL, &dinode_attr(ino, &dinode), ino);
    }

    /// Remove a file.
    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let inumber_p = (parent + 1) as usize;
        let mut dinode_p = self.fs.dinode(self.buf, inumber_p);

        let name = name.to_str().unwrap();
        let inumber = match self.fs.dir_delete(self.buf, inumber_p, name) {
            Some(direct) => direct.d_ino as usize,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let mut dinode = self.fs.dinode(self.buf, inumber);
        dinode_p.di_nlink -= 1;
        dinode.di_nlink -= 1;

        if dinode.di_nlink == 0 {
            self.fs.dinode_free(self.buf, inumber);
        } else {
            self.fs.dinode_update(self.buf, inumber, &dinode);
        }

        self.fs.dinode_update(self.buf, inumber_p, &dinode_p);

        reply.ok();
    }

    /// Rename a file.
    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let inumber_p = (parent + 1) as usize;
        let mut dinode_p = self.fs.dinode(self.buf, inumber_p);

        let inumber_newp = (newparent + 1) as usize;
        let mut dinode_newp = self.fs.dinode(self.buf, inumber_newp);

        if dinode_p.di_mode == 0 || dinode_newp.di_mode == 0 {
            reply.error(libc::ENOENT);
            return;
        }

        let name = name.to_str().unwrap();
        let newname = newname.to_str().unwrap();

        let mut direct = match self.fs.dir_delete(self.buf, inumber_p, name) {
            Some(direct) => direct,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        direct.d_namlen = newname.len() as u8;
        self.fs
            .dir_append(self.buf, &mut dinode_newp, direct, newname.as_bytes());

        dinode_p.di_nlink -= 1;
        dinode_newp.di_nlink += 1;

        self.fs.dinode_update(self.buf, inumber_p, &dinode_p);
        self.fs.dinode_update(self.buf, inumber_newp, &dinode_newp);

        reply.ok();
    }

    /// Create a hard link.
    fn link(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let inumber = (ino + 1) as usize;
        let inumber_p = (newparent + 1) as usize;

        let mut dinode = self.fs.dinode(self.buf, inumber);
        let mut dinode_p = self.fs.dinode(self.buf, inumber_p);

        if dinode.di_mode == 0 || dinode_p.di_mode == 0 {
            reply.error(libc::ENOENT);
            return;
        }

        let newname = newname.to_str().unwrap();
        let direct = Direct {
            d_ino: inumber as u32,
            d_reclen: 0,
            d_type: DT_REG,
            d_namlen: newname.len() as u8,
        };
        self.fs
            .dir_append(self.buf, &mut dinode_p, direct, newname.as_bytes());

        dinode.di_nlink += 1;
        dinode_p.di_nlink += 1;
        self.fs.dinode_update(self.buf, inumber, &dinode);
        self.fs.dinode_update(self.buf, inumber_p, &dinode_p);

        let ino = (inumber - 1) as u64;
        reply.entry(&TTL, &dinode_attr(ino, &dinode), ino);
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        let inumber = (ino + 1) as usize;
        let dinode = self.fs.dinode(self.buf, inumber);
        if dinode.di_mode == 0 {
            reply.error(ENOENT);
            return;
        }

        let data = self.fs.read(self.buf, &dinode);
        let size = std::cmp::min(size as usize, data.len() - offset as usize);
        reply.data(&data[offset as usize..(offset as usize + size)]);
    }

    /// Read directory.
    /// Send a buffer filled using buffer.fill(), with size not exceeding the
    /// requested size. Send an empty buffer on end of stream. fh will contain the
    /// value set by the opendir method, or will be undefined if the opendir method
    /// didn't set any value.
    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        info!("readdir: ino={}, offset={}", ino, offset);

        let inumber = ino + 1;

        let directs = self.fs.dir_read(self.buf, inumber as usize);

        for (i, (direct, name)) in directs.into_iter().enumerate() {
            if i <= offset as usize {
                continue;
            }

            let kind = if direct.d_type & DT_DIR > 0 {
                FileType::Directory
            } else if direct.d_type & DT_REG > 0 {
                FileType::RegularFile
            } else {
                continue;
            };

            if reply.add((direct.d_ino - 1) as u64, (i + 1) as i64, kind, name) {
                break;
            }
        }

        reply.ok();
    }

    /// Read directory.
    /// Send a buffer filled using buffer.fill(), with size not exceeding the
    /// requested size. Send an empty buffer on end of stream. fh will contain the
    /// value set by the opendir method, or will be undefined if the opendir method
    /// didn't set any value.
    fn readdirplus(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectoryPlus,
    ) {
        info!("readdirplus: ino={}, offset={}", ino, offset);

        let inumber = ino + 1;

        let directs = self.fs.dir_read(self.buf, inumber as usize);

        for (i, (direct, name)) in directs.into_iter().enumerate() {
            if i <= offset as usize {
                continue;
            }

            let dinode = self.fs.dinode(self.buf, direct.d_ino as usize);

            let ino = (direct.d_ino - 1) as u64;
            if reply.add(
                ino,
                (i + 1) as i64,
                name,
                &TTL,
                &dinode_attr(ino, &dinode),
                0,
            ) {
                break;
            }
        }

        reply.ok();
    }
}

fn main() -> Result<()> {
    env_logger::init();

    let filepath = std::env::args().nth(1).expect("usage: cmd <file>");
    let mut file = std::fs::read(filepath)?;

    fsck(&file);

    let options = vec![
        MountOption::RW,
        MountOption::AutoUnmount,
        MountOption::AllowRoot,
        MountOption::FSName("ffs".to_string()),
    ];

    let buf = file.as_mut_slice();
    let fs = fs(buf);
    let ffs = FFS { buf, fs };
    fuser::mount2(ffs, "mnt", &options).unwrap();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use memoffset::offset_of;

    #[test]
    fn bufarea_test() {
        assert_eq!(offset_of!(Bufarea, b_bno), 0);
        assert_eq!(offset_of!(Bufarea, b_next), 8);
        assert_eq!(offset_of!(Bufarea, b_prev), 16);
        assert_eq!(offset_of!(Bufarea, b_size), 24);
        assert_eq!(offset_of!(Bufarea, b_errs), 28);
        assert_eq!(offset_of!(Bufarea, b_flags), 32);
        assert_eq!(offset_of!(Bufarea, b_dirty), 48);

        assert_eq!(std::mem::size_of::<Bufarea>(), 56);
    }

    #[test]
    fn fs_test() {
        assert_eq!(offset_of!(Fs, fs_frag), 56);
        assert_eq!(offset_of!(Fs, fs_nspf), 124);
        assert_eq!(offset_of!(Fs, fs_fpg), 188);
        assert_eq!(offset_of!(Fs, fs_fmod), 208);
        assert_eq!(offset_of!(Fs, fs_ffs1_flags), 211);
        assert_eq!(offset_of!(Fs, fs_fsmnt), 212);
        assert_eq!(offset_of!(Fs, fs_volname), 680);
        assert_eq!(offset_of!(Fs, fs_swuid), 712);
        assert_eq!(offset_of!(Fs, fs_pad), 720);
        assert_eq!(offset_of!(Fs, fs_cpc), 856);
        assert_eq!(offset_of!(Fs, fs_snapinum), 1116);
        assert_eq!(offset_of!(Fs, fs_avgfilesize), 1196);
        assert_eq!(offset_of!(Fs, fs_maxfilesize), 1328);

        assert_eq!(std::mem::size_of::<Fs>(), 1384);
    }

    #[test]
    fn inode_test() {
        assert_eq!(std::mem::size_of::<Ufs1Dinode>(), 128);
        assert_eq!(std::mem::size_of::<Ufs2Dinode>(), 256);
    }
}
