use super::raft::*;
use super::pd::*;

use std::time::Duration;
use std::thread;

pub fn test_raft(args: &[String]) {
    if args.len() < 3 {
        println!("Try use cargo run 5 1");
        return ;
    }

    let mut arg = &args[1];
    let num: u16 = arg.trim().parse().unwrap();

    arg = &args[2];
    let mut port_num: u16 = arg.trim().parse().unwrap();

    
    port_num = 8080 + 2*(port_num - 1);
    let port = format!("127.0.0.1:{}", port_num);
    let raftserver = RaftServer::new(port.to_string(),port_num);
    
    let mut i: u16 = 1;
    let mut port_other;
    let mut server;
    while i <= num{
        let port_num_other: u16 = 8080 + 2*(i-1);
        if port_num_other != port_num{
            port_other = format!("127.0.0.1:{}", port_num_other);
            server = format!("Raft:{}", port_num_other);
            raftserver.add_raft_server(server.to_string(), 
                               port_other.to_string());
        }
        i = i + 1;
    }
    raftserver.show_servers();
    loop {
        thread::sleep(Duration::from_secs(10));
    }
}

// pub fn test_raft() {
//     let raftserver = RaftServer::new("127.0.0.1:8080".to_string(),8080);
//     // raftserver.add_raft_server("server1".to_string(), 
//     //                            "127.0.0.1:8080".to_string());
//     raftserver.add_raft_server("server2".to_string(), 
//                                "127.0.0.1:8082".to_string());
//     raftserver.add_raft_server("server3".to_string(), 
//                                "127.0.0.1:8084".to_string());
//     raftserver.add_raft_server("server4".to_string(), 
//                                "127.0.0.1:8086".to_string());
//     raftserver.add_raft_server("server5".to_string(), 
//                                "127.0.0.1:8088".to_string());
//     println!("Add server finished!");
//     loop {
//         thread::sleep(Duration::from_secs(10));
//     }

// }

pub fn test_pd() {
    let mut pd = PD::new(8060);
    if let Some(thread) = pd.worker_thread.take() {
        thread.join().unwrap();
    } 
}
