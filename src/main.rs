use anyhow::Result;

// inode

const ROOTINO: usize = 2;
const NXADDR: usize = 2;
const NDADDR: usize = 12;
const NIADDR: usize = 3;

#[repr(C)]
union Ufs1DinodeU {
    oldids: [u16; 2], /*   4: Ffs: old user and group ids. */
    inumber: u32,     /*   4: Lfs: inode number. */
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
#[derive(Debug)]
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

#[repr(C)]
#[derive(Debug)]
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

// cylinder group
const CG_MAGIC: i32 = 0x090255;

#[repr(C)]
#[derive(Debug)]
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
    cg_btotoff: i32,          /* (int32) block totals per cylinder */
    cg_boff: i32,             /* (u_int16) free block positions */
    cg_iusedoff: u32,         /* (u_int8) used inode map */
    cg_freeoff: u32,          /* (u_int8) free block map */
    cg_nextfreeoff: u32,      /* (u_int8) next available space */
    cg_clustersumoff: u32,    /* (u_int32) counts of avail clusters */
    cg_clusteroff: u32,       /* (u_int8) free cluster map */
    cg_nclusterblks: u32,     /* number of clusters this cg */
    cg_ffs2_niblk: u32,       /* number of inode blocks this cg */
    cg_initediblk: u32,       /* last initialized inode */
    cg_sparecon32: [i32; 3],  /* reserved for future use */
    cg_ffs2_time: i64,        /* time last written */
    cg_sparecon64: [i64; 3],  /* reserved for future use */
                              /* actually longer */
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
#[derive(Debug, PartialEq, Eq)]
struct Csum {
    cs_ndir: i32,   /* number of directories */
    cs_nbfree: i32, /* number of free blocks */
    cs_nifree: i32, /* number of free inodes */
    cs_nffree: i32, /* number of free frags */
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
struct CsumTotal {
    cs_ndir: i64,       /* number of directories */
    cs_nbfree: i64,     /* number of free blocks */
    cs_nifree: i64,     /* number of free inodes */
    cs_nffree: i64,     /* number of free frags */
    cs_spare: [i64; 4], /* future expansion */
}

#[repr(C)]
#[derive(Debug, PartialEq, Eq)]
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
    fn fsbtodb(&self, b: i32) -> i32 {
        b << self.fs_fsbtodb
    }
    fn dbtofsb(&self, b: i32) -> i32 {
        b >> self.fs_fsbtodb
    }

    /*
     * Cylinder group macros to locate things in cylinder groups.
     * They calc file system addresses of cylinder group data structures.
     */
    fn cgbase(&self, c: i32) -> i32 {
        self.fs_fpg * c
    }
    /* data zone */
    fn cgdata(&self, c: i32) -> i32 {
        self.cgdmin(c) + self.fs_minfree
    }
    /* meta data */
    fn cgmeta(&self, c: i32) -> i32 {
        self.cgdmin(c)
    }
    /* 1st data */
    fn cgdmin(&self, c: i32) -> i32 {
        self.cgstart(c) + self.fs_dblkno
    }
    /* inode blk */
    fn cgimin(&self, c: i32) -> i32 {
        self.cgstart(c) + self.fs_iblkno
    }
    /* super blk */
    fn cgsblock(&self, c: i32) -> i32 {
        self.cgstart(c) + self.fs_sblkno
    }
    /* cg block */
    fn cgtod(&self, c: i32) -> i32 {
        self.cgstart(c) + self.fs_cblkno
    }
    fn cgstart(&self, c: i32) -> i32 {
        self.cgbase(c) + self.fs_cgoffset * (c & !self.fs_cgmask)
    }

    /*
     * Macros for handling inode numbers:
     *     inode number to file system block offset.
     *     inode number to cylinder group number.
     *     inode number to file system block address.
     */
    fn ino_to_cg(&self, x: i32) -> i32 {
        x / self.fs_ipg as i32
    }
    fn ino_to_fsba(&self, x: i32) -> i32 {
        self.cgimin(self.ino_to_cg(x))
            + self.blkstofrags((x % self.fs_ipg as i32) / self.fs_inopb as i32)
    }
    fn ino_to_fsbo(&self, x: i32) -> i32 {
        x % self.fs_inopb as i32
    }

    /*
     * Give cylinder group number for a file system block.
     * Give frag block number in cylinder group for a file system block.
     */
    fn dtog(&self, d: i32) -> i32 {
        d / self.fs_fpg
    }
    fn dtogd(&self, d: i32) -> i32 {
        d % self.fs_fpg
    }

    /*
     * Number of disk sectors per block/fragment; assumes DEV_BSIZE byte
     * sector size.
     */
    fn nspb(&self) -> i32 {
        self.fs_nspf << self.fs_fragshift
    }

    /* Number of inodes per file system fragment (fs->fs_fsize) */
    fn inopf(&self) -> i32 {
        self.fs_inopb as i32 >> self.fs_fragshift
    }

    /*
     * The following macros optimize certain frequently calculated
     * quantities by using shifts and masks in place of divisions
     * modulos and multiplications.
     */
    fn blkoff(&self, loc: i64) -> i64 {
        loc & self.fs_qbmask
    }
    /*
    #define fragoff(fs, loc)	/* calculates (loc % fs->fs_fsize) */ \
        ((loc) & (fs)->fs_qfmask)
    #define lblktosize(fs, blk)	/* calculates ((off_t)blk * fs->fs_bsize) */ \
        ((off_t)(blk) << (fs)->fs_bshift)
    #define lblkno(fs, loc)		/* calculates (loc / fs->fs_bsize) */ \
        ((loc) >> (fs)->fs_bshift)
    #define numfrags(fs, loc)	/* calculates (loc / fs->fs_fsize) */ \
        ((loc) >> (fs)->fs_fshift)
    #define blkroundup(fs, size)	/* calculates roundup(size, fs->fs_bsize) */ \
        (((size) + (fs)->fs_qbmask) & (fs)->fs_bmask)
    #define fragroundup(fs, size)	/* calculates roundup(size, fs->fs_fsize) */ \
        (((size) + (fs)->fs_qfmask) & (fs)->fs_fmask)
    #define fragstoblks(fs, frags)	/* calculates (frags / fs->fs_frag) */ \
        ((frags) >> (fs)->fs_fragshift)
        */
    /* calculates (blks * fs->fs_frag) */
    fn blkstofrags(&self, blks: i32) -> i32 {
        blks << self.fs_fragshift
    }
    /*
    #define fragnum(fs, fsb)	/* calculates (fsb % fs->fs_frag) */ \
        ((fsb) & ((fs)->fs_frag - 1))
    #define blknum(fs, fsb)		/* calculates rounddown(fsb, fs->fs_frag) */ \
        ((fsb) &~ ((fs)->fs_frag - 1))
        */
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

fn main() -> Result<()> {
    let file = std::fs::read("test.img")?;

    for offset in SBLOCKSEARCH {
        if offset + SBSIZE >= file.len() {
            continue;
        }

        let fs: &Fs = unsafe { std::mem::transmute(&file[offset]) };

        if fs.fs_magic != FS_UFS1_MAGIC && fs.fs_magic != FS_UFS2_MAGIC {
            continue;
        }
        if fs.fs_magic == FS_UFS1_MAGIC && offset == SBLOCK_UFS2 {
            continue;
        }
        if fs.fs_magic == FS_UFS2_MAGIC && fs.fs_sblockloc != offset as i64 {
            continue;
        }

        let fs_v2 = fs.fs_magic == FS_UFS2_MAGIC;

        eprintln!("sb={:?}", fs);

        // consistency check
        assert!(fs.fs_ncg >= 1);
        assert!(fs.fs_cpg >= 1);

        if fs.fs_magic == FS_UFS1_MAGIC {
            if fs.fs_ncg as i32 * fs.fs_cpg < fs.fs_ncyl
                || (fs.fs_ncg as i32 - 1) * fs.fs_cpg >= fs.fs_ncyl
            {
                todo!();
            }
        }

        assert!((fs.fs_sbsize as usize) < SBSIZE);
        assert!((fs.fs_bsize as usize).is_power_of_two());
        assert!((fs.fs_bsize as usize) >= MINBSIZE);
        assert!((fs.fs_bsize as usize) <= MAXBSIZE);

        assert!((fs.fs_fsize as usize).is_power_of_two());
        assert!(fs.fs_fsize <= fs.fs_bsize);
        assert!(fs.fs_fsize >= fs.fs_bsize / MAXFRAG as i32);

        let blk_alt = fs.cgsblock(fs.fs_ncg as i32 - 1);
        let offset_alt = fs.fsbtodb(blk_alt) as usize * DEV_BSIZE;

        let fs_alt: &Fs = unsafe { std::mem::transmute(&file[offset_alt]) };

        assert_eq!(fs, fs_alt);

        // pass0

        for c in 0..fs.fs_ncg {
            let blk = fs.cgtod(c as i32);
            let offset = fs.fsbtodb(blk) as usize * DEV_BSIZE;

            let cg: &Cg = unsafe { std::mem::transmute(&file[offset]) };
            assert_eq!(cg.cg_magic, CG_MAGIC);
            eprintln!("cg #{}: {:?}", c, cg);

            let inosused = if fs_v2 {
                cg.cg_initediblk.min(fs.fs_ipg)
            } else {
                fs.fs_ipg
            };

            for inumber in 0..inosused {
                // TODO
                let blk = fs.cgimin(fs.ino_to_cg(inumber as i32));
                let blk_offset = fs.fsbtodb(blk) as usize * DEV_BSIZE;

                let ino_size = if fs_v2 {
                    std::mem::size_of::<Ufs2Dinode>()
                } else {
                    std::mem::size_of::<Ufs1Dinode>()
                } as u32;

                // TODO
                let offset = blk_offset + ((inumber % fs.fs_ipg) * ino_size) as usize;

                let di_ctime = if fs_v2 {
                    let dinode: &Ufs2Dinode = unsafe { std::mem::transmute(&file[offset]) };
                    dinode.di_ctime
                } else {
                    let dinode: &Ufs1Dinode = unsafe { std::mem::transmute(&file[offset]) };
                    dinode.di_ctime as i64
                };
                eprintln!("ino={}, ctime={}", inumber, di_ctime);
            }
            dbg!(inosused);
        }
    }

    println!("Hello, world!");

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
