use cfg_block::cfg_block;

cfg_block! {
    #[cfg(target_os = "linux")] {
        mod linux;
        pub use linux::{IO, File};
    }

    #[cfg(target_os = "macos")] {
        mod darwin;
        pub use darwin::{IO, File};
    }
}
