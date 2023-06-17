use argh::FromArgs;
use ffs::*;
use fuser::*;

#[derive(FromArgs)]
/// Reach new heights.
struct Args {
    /// enable read-write with mmap
    #[argh(switch)]
    rw: bool,

    /// an optional nickname for the pilot
    #[argh(option)]
    image: String,
}

fn main() -> anyhow::Result<()> {
    use memmap2::{MmapMut, MmapOptions};
    use std::fs::{File, OpenOptions};

    env_logger::init();

    let args: Args = argh::from_env();

    let (_file, mut map): (File, MmapMut) = unsafe {
        if args.rw {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&args.image)?;
            let map = MmapOptions::new().map_mut(&file)?;
            (file, map)
        } else {
            let file = File::open(&args.image)?;
            let map = MmapOptions::new().map_copy(&file)?;
            (file, map)
        }
    };

    fsck(&map);

    let options = vec![
        MountOption::RW,
        MountOption::AutoUnmount,
        MountOption::AllowRoot,
        MountOption::CUSTOM("nonempty".to_string()),
        MountOption::FSName("ffs".to_string()),
    ];

    let mut ffs = FFS::new(&mut map);
    fuser::mount2(&mut ffs, "mnt", &options)?;

    eprintln!("done");

    ffs.sync();

    Ok(())
}
