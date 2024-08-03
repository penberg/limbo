//! The storage layer.
//!
//! This module contains the storage layer for Limbo. The storage layer is
//! responsible for managing access to the database and its pages. The main
//! interface to the storage layer is the `Pager` struct, which is
//! responsible for managing the database file and the pages it contains.
//!
//! Pages in a database are stored in one of the following to data structures:
//! `DatabaseStorage` or `Wal`. The `DatabaseStorage` trait is responsible
//! for reading and writing pages to the database file, either local or
//! remote. The `Wal` struct is responsible for managing the write-ahead log
//! for the database, also either local or remote.

pub(crate) mod btree;
pub(crate) mod buffer_pool;
pub(crate) mod database;
pub(crate) mod pager;
pub(crate) mod sqlite3_ondisk;
pub(crate) mod wal;
