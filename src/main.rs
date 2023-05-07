use anyhow::Result;

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

impl Cg {
    fn inosused(&self, cgoff: usize, buf: &[u8]) -> &[u8] {
        let offset = cgoff + self.cg_iusedoff as usize;
        let size = self.cg_niblk as usize / 8;
        unsafe {
            let ptr: *const u8 = &buf[offset];
            std::slice::from_raw_parts(ptr, size)
        }
    }

    fn blksfree(&self, cgoff: usize, buf: &[u8]) -> &[u8] {
        let offset = cgoff + self.cg_freeoff as usize;
        let size = self.cg_ndblk as usize / 8;
        unsafe {
            let ptr: *const u8 = &buf[offset];
            std::slice::from_raw_parts(ptr, size)
        }
    }

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
    fn fragnum(&self, fsb: i32) -> i32 {
        fsb & (self.fs_frag - 1)
    }
    fn blknum(&self, fsb: i32) -> i32 {
        fsb & !(self.fs_frag - 1)
    }
}

fn howmany(a: usize, b: usize) -> usize {
    (a + b - 1) / b
}

// extra helpers
impl Fs {
    fn cg<'a>(&'a self, buf: &'a [u8], c: usize) -> &'a Cg {
        assert!(c <= self.fs_ncg as usize);

        let blk = self.cgtod(c as i32);
        let offset = self.fsbtodb(blk) as usize * DEV_BSIZE;

        let cg: &'a Cg = unsafe { std::mem::transmute(&buf[offset]) };
        assert_eq!(cg.cg_magic, CG_MAGIC);

        cg
    }

    fn blk<'a, 'b>(&'a self, buf: &'b [u8], blk: i32, len: usize) -> &'b [u8] {
        assert!(len <= self.fs_bsize as usize, "{}", len);
        let offset = self.fsbtodb(blk) as usize * DEV_BSIZE;
        assert!(offset + len <= buf.len());

        &buf[offset..offset + len]
    }

    fn read_indir(&self, buf: &[u8], ino: &Ufs2Dinode, blk: i64, depth: usize, out: &mut Vec<u8>) {
        if blk == 0 {
            return;
        }

        if depth == 0 {
            let remain = ino.di_size as usize - out.len();
            let read = remain.min(self.fs_bsize as usize);
            if read != self.fs_bsize as usize {
                println!(
                    "read_indir: blk={}/{}/{}, fraglen={}/{}",
                    blk,
                    self.blknum(blk as i32),
                    self.fragnum(blk as i32),
                    read,
                    howmany(read, self.fs_fsize as usize)
                );
            }
            out.extend_from_slice(self.blk(buf, blk as i32, read));
            return;
        }

        let indir = self.blk(buf, blk as i32, self.fs_bsize as usize);
        let blks: &[i32] = unsafe {
            let ptr: *const i32 = std::mem::transmute(indir.as_ptr());
            let size: usize = indir.len() / std::mem::size_of::<i32>();
            std::slice::from_raw_parts(ptr, size)
        };

        for blk_child in blks {
            let blk_child = *blk_child as i64;
            if blk_child == 0 {
                break;
            }
            self.read_indir(buf, ino, blk_child, depth - 1, out);
        }
    }

    fn read(&self, buf: &[u8], ino: &Ufs2Dinode) -> Vec<u8> {
        let mut out = Vec::with_capacity(ino.di_size as usize);

        for i in 0..NDADDR {
            let blk = ino.di_db[i];
            self.read_indir(buf, ino, blk, 0, &mut out);
            if out.len() == ino.di_size as usize {
                break;
            }
        }

        for i in 0..NIADDR {
            let blk = ino.di_ib[i];
            self.read_indir(buf, ino, blk, i + 1, &mut out);
            if out.len() == ino.di_size as usize {
                break;
            }
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

fn fs(buf: &[u8]) -> &Fs {
    for offset in SBLOCKSEARCH {
        if offset + SBSIZE >= buf.len() {
            continue;
        }

        let fs: &Fs = unsafe { std::mem::transmute(&buf[offset]) };

        if fs.fs_magic != FS_UFS1_MAGIC && fs.fs_magic != FS_UFS2_MAGIC {
            continue;
        }
        if fs.fs_magic == FS_UFS1_MAGIC && offset == SBLOCK_UFS2 {
            continue;
        }
        if fs.fs_magic == FS_UFS2_MAGIC && fs.fs_sblockloc != offset as i64 {
            continue;
        }

        return fs;
    }
    todo!();
}

fn main() -> Result<()> {
    let filepath = std::env::args().nth(1).expect("usage: cmd <file>");
    let file = std::fs::read(filepath)?;

    let fs = fs(&file);

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

    if fs != fs_alt {
        eprintln!("superblock mismatch");
        // assert_eq!(fs, fs_alt);
    }

    // pass0
    for c in 0..fs.fs_ncg {
        let blk = fs.cgtod(c as i32);
        let offset = fs.fsbtodb(blk) as usize * DEV_BSIZE;

        let cg = fs.cg(&file, c as usize);

        let inousedmap = cg.inosused(offset, &file);
        let blksfreemap = cg.blksfree(offset, &file);
        eprintln!("cg #{}: {:?}", c, cg,);
        eprintln!("inoused={}/{:?}", inousedmap.len(), inousedmap,);
        eprintln!("blkfree={}/{:?}", blksfreemap.len(), blksfreemap,);

        let inosused = if fs_v2 {
            cg.cg_initediblk.min(fs.fs_ipg)
        } else {
            fs.fs_ipg
        };

        let ino_size = if fs_v2 {
            std::mem::size_of::<Ufs2Dinode>()
        } else {
            std::mem::size_of::<Ufs1Dinode>()
        } as u32;

        dbg!(inosused);
        let ino_start = fs.fs_ipg * c;
        let ino_end = ino_start + inosused;

        for inumber in ino_start..ino_end {
            // TODO
            let blk = fs.ino_to_fsba(inumber as i32);
            let blk_offset = fs.fsbtodb(blk) as usize * DEV_BSIZE;

            // TODO
            let offset = blk_offset + (fs.ino_to_fsbo(inumber as i32) as u32 * ino_size) as usize;

            let ino = if fs_v2 {
                let dinode: &Ufs2Dinode = unsafe { std::mem::transmute(&file[offset]) };
                *dinode
            } else {
                let dinode: &Ufs1Dinode = unsafe { std::mem::transmute(&file[offset]) };
                (*dinode).into()
            };

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
            {
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
                eprintln!("zero-link inode: ino={}", inumber);
            }

            let data = fs.read(&file, &ino);

            eprintln!(
                "ino={}, nlink={}, ctime={}, mode={:0o}, size={}/{}/{}",
                inumber,
                ino.di_nlink,
                ino.di_ctime,
                mode,
                ino.di_size,
                data.len(),
                lndb
            );

            if mode == IFREG {
                if let Ok(s) = std::str::from_utf8(&data) {
                    // eprintln!("content={}", s);
                }
            } else {
                let dirblks = data.len() / DEV_BSIZE;

                for dirblk in 0..dirblks {
                    let mut blkdata = &data[dirblk * DEV_BSIZE..(dirblk + 1) * DEV_BSIZE];

                    while blkdata.len() >= directsiz(0) {
                        let dp: &Direct = unsafe { std::mem::transmute(&blkdata[0]) };
                        if dp.d_ino == 0 {
                            break;
                        }

                        let sz = directsiz(dp.d_namlen);
                        let namebuf: &[u8] = unsafe {
                            let buf: *const u8 =
                                std::mem::transmute(&blkdata[std::mem::size_of::<Direct>()]);
                            std::slice::from_raw_parts(buf, dp.d_namlen as usize)
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

                        eprintln!("{}, {:?}, {}", filename, dp, sz);
                    }
                }
            }
        }
    }

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
