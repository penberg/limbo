use cfg_block::cfg_block;

pub type Buffer = Vec<u8>;

cfg_block! {
    #[cfg(target_os = "linux")] {
        mod linux;
        pub use linux::{File, IO};
    }

    #[cfg(target_os = "macos")] {
        mod darwin;
        pub use darwin::{File, IO};
    }
}
