extern crate labkv;
//#[macro_use] extern crate serde_derive;
extern crate serde;
extern crate serde_json;

use std::env;
use labkv::rpc::*;
//use labkv::raft::*;
use labkv::mytest::*;

fn main() {
    println!("Hello, world!");
    let args: Vec<String> = env::args().collect();
    //test_rpc_server();
    test_raft(&args);
}


// fn test_rpc() {
//         println!("Test start");
//         // let reqreceiver = Arc::new(Mutex::new(reqreceiver));

//         // let client = ClientEnd::new("client1".to_string());
//         let mut server = Server::new("server1".to_string());
//         {
//             let owner = server.add_service(5);
//         }
//         // {
//         //     client.call("127.0.0.1:8082".to_string(), "Append".to_string(),"hello world".to_string());
//         // }
//         // server.listen_thread.join().unwrap();
//         // server.wait_stop();
//         // client.wait_stop();
//         //thread::sleep(Duration::from_secs(100));
//         // cline
// }

