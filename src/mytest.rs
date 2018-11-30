use super::raft::*;
use super::pd::*;

use std::time::Duration;
use std::thread;

pub fn test_raft() {
    let raftserver = RaftServer::new("127.0.0.1:8080".to_string(),8080);
    // raftserver.add_raft_server("server1".to_string(), 
    //                            "127.0.0.1:8080".to_string());
    raftserver.add_raft_server("server2".to_string(), 
                               "127.0.0.1:8082".to_string());
    raftserver.add_raft_server("server3".to_string(), 
                               "127.0.0.1:8084".to_string());
    raftserver.add_raft_server("server4".to_string(), 
                               "127.0.0.1:8086".to_string());
    raftserver.add_raft_server("server5".to_string(), 
                               "127.0.0.1:8088".to_string());
    println!("Add server finished!");
    loop {
        thread::sleep(Duration::from_secs(10));
    }

}

pub fn test_pd() {
    let mut pd = PD::new(8060);
    if let Some(thread) = pd.worker_thread.take() {
        thread.join().unwrap();
    } 
}
