use super::raft::*;
use super::pd::*;
use super::rpc::*;
use super::kvserver::*;

use std::time::Duration;
use std::thread;

pub fn test_raft(args: &[String]) {
    if args.len() < 3 {
        println!("Try use cargo run 127.0.0.1 8080");
        return ;
    }



    let mut arg = &args[1];
    //let num: u16 = arg.trim().parse().unwrap();
    let ip: String = arg.to_string();

    arg = &args[2];
    let port_num: u16 = arg.trim().parse().unwrap();

    
    //port_num = 8080 + 2*(port_num - 1);
    //let port = format!("127.0.0.1:{}", port_num);
    let raftserver = RaftServer::new(ip, port_num);
    
    // let mut i: u16 = 1;
    // let mut port_other;
    // let mut server;
    // while i <= num{
    //     let port_num_other: u16 = 8080 + 2*(i-1);
    //     if port_num_other != port_num{
    //         port_other = format!("127.0.0.1:{}", port_num_other);
    //         server = format!("Raft:{}", port_num_other);
    //         raftserver.add_raft_server(server.to_string(), 
    //                            port_other.to_string());
    //     }
    //     i = i + 1;
    // }
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

pub fn test_rpc_server() {
        kv_info!("Test start");
        let mut server = RpcServer::new("server1".to_string(), 8080);
        let mut listens = Vec::new();
        for i in 0..4 {
            let owner = server.add_service(i);
            // let receiver = owner.receiver;
            let listen_thread = thread::spawn(move || {
                loop {
                    let reqmsg = owner.receiver.recv().unwrap();
                    reqmsg.print_req();
                    owner.sender.send(Replymsg {
                        ok: true,
                        reply: "Reply from server".to_string(),
                    }).unwrap();
                }
            });
            listens.push(listen_thread);
        }
        for listen_thread in listens {
            listen_thread.join().unwrap();
        }
}

pub fn test_rpc_client() {
    for _ in 0..10 {
        let arg = "hello world!";
        let args = format!("{0:<0width$}{1}", arg.len(), 
                    arg, width = 10);
        rpc_call("127.0.0.1:8080".to_string(), "Raft.Append".to_string(), args);
        let mut args = String::new();
        for i in 0..4096 {
            for j in 0..8 {
                args += &format!("[{}:{}]", i, j);
            }
        }
        rpc_call("127.0.0.1:8080".to_string(), "Raft.Append".to_string(), args);
    }
}

pub fn test_kv_server(args: &[String]) {
    if args.len() < 3 {
        println!("Try use cargo run 127.0.0.1 8080");
        return ;
    }
    let arg = &args[2];
    //let ip: String = arg.to_string();
    //arg = &args[2];
    let port_num: u16 = arg.trim().parse().unwrap();
    let mut kv_server = KvServer::new(port_num + 1, port_num);
    kv_server.start_kv_server();
}
