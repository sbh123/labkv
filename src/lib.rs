#[macro_use]  extern crate serde_derive;
#[macro_use]
pub mod macros;
pub mod rpc;
pub mod raft;
pub mod pd;
pub mod common;
pub mod mytest;
pub mod kvserver;
// pub mod config;


extern crate uuid;
use uuid::Uuid;
