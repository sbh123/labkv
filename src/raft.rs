extern crate rand;

use super::serde_derive;

extern crate serde;
extern crate serde_json;

use raft::rand::Rng;

use super::rpc::*;
use std::error::Error;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::io;
use std::fmt;
use std::time::{Instant, Duration};
use std::mem;
use std::clone;

pub enum RaftState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LogCommand{
    op: u8,
    key: String,
    value: String,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct RaftLog{
    term: usize,
    index: usize,
    command: LogCommand,
}
impl Clone for LogCommand {
    fn clone(&self) -> Self {
        LogCommand { 
            op: self.op,
            key: self.key.clone(),
            value: self.value.clone(),
         }
    }

    fn clone_from(&mut self, source: &Self) {
        
    }

}
impl Clone for RaftLog {
    fn clone(&self) -> Self {
        RaftLog { 
            term: self.term,
            index: self.index,
            command: self.command.clone(),
        }
    }
    
    fn clone_from(&mut self, source: &Self) {
    }

}

pub struct RaftServer {
    servers: Arc<Mutex<HashMap<String, String>>>,
    raft: Arc<Mutex<Raft>>,
}

impl RaftServer {
    pub fn new(serverip: String, rpcport: u16) -> RaftServer {
        let servers = Arc::new(Mutex::new(HashMap::new()));
        let client = ClientEnd::new("client".to_string());
        let server = RpcServer::new("raft".to_string(), rpcport);
        let raft = Raft::new(client, server, serverip);
        let raft = Arc::new(Mutex::new(raft));
        Raft::add_timeout(Arc::clone(&raft), Arc::clone(&servers));
        Raft::add_timer(Arc::clone(&raft), Arc::clone(&servers));
        Raft::add_service(Arc::clone(&raft), 0);
        RaftServer { servers, raft }
    }

    pub fn add_raft_server(&self, serverid: String, serverip: String) {
        self.servers.lock().unwrap().insert(serverid, serverip);
    }
}

pub struct Raft {
    client: ClientEnd,
    server: RpcServer,
    state: RaftState,
    current_term: usize,
    last_logterm: usize,
    last_logindex: usize,
    id: String,
    raft_logs: Vec<RaftLog>,
    next_index: HashMap<String, usize>,
    // election_time: Arc<Mutex<usize>>,
    commit_index: usize,
    last_applied: usize,
    servers: HashMap<String, String>,
    leader: (String, String),
    timeout_thread: Option<thread::JoinHandle<()>>,
    timer_thread: Option<thread::JoinHandle<()>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RequestVateArg {
    term: usize,
    candidateid: String,
    last_logindex: usize,
    last_logterm: usize,
}


#[derive(Serialize, Deserialize, Debug)]
pub struct RequestVateReply {
    vote_grante: bool,
    term: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Append_entry_arg {
    term: usize,
    leaderid: String,
    prevLogIndex: usize,
    prevLogTerm: usize,
    entries: Vec<RaftLog>,
    leaderCommit: usize,
}

//
#[derive(Serialize, Deserialize, Debug)]
pub struct Append_entry_reply {
    success: bool,
    term: usize,
    last_index: usize,
}

#[derive(Debug)]
pub enum RaftError {
    Io(io::Error),
    Info(String),
}

impl fmt::Display for RaftError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RaftError::Io(ref err) => err.fmt(f),
            RaftError::Info(ref info) => write!(f, "{}", info),                                                                                                                           
        }
    }
}

impl Error for RaftError {
    fn description(&self) -> &str {
        match *self {
            RaftError::Io(ref err) => err.description(),
            RaftError::Info(ref info) => &info[..],
        }
    }
}

impl RequestVateArg {
    pub fn reqmsg_to_votearg(reqmsg: Reqmsg) -> Result<RequestVateArg, RaftError> {
        if reqmsg.args.len() < 4 {
            return Err(RaftError::Info("Wrong args".to_string()));
        }
        let term: usize = reqmsg.args[0].trim().parse().unwrap();
        let candidateid: String = reqmsg.args[1].clone();
        let last_logindex: usize = reqmsg.args[1].trim().parse().unwrap();
        let last_logterm: usize = reqmsg.args[1].trim().parse().unwrap();
        Ok(RequestVateArg {
            term,
            candidateid,
            last_logindex,
            last_logterm,
        })
    }
}

impl RequestVateReply {
    pub fn replymsg_to_votereply(reply: Replymsg) -> Result<RequestVateReply, RaftError> {
        if reply.reply.len() < 2 {
            return Err(RaftError::Info("Eri reply".to_string()));
        }
        let vote_grante: bool = reply.reply[0].trim().parse().unwrap();
        let term: usize = reply.reply[1].trim().parse().unwrap();
        Ok(RequestVateReply { vote_grante, term })
    }
}

impl Raft {
    pub fn new(client: ClientEnd, server: RpcServer, id: String) -> Raft {
        let raftlog = RaftLog {
            term: 0,
            index: 0,
            command: LogCommand {
                op: 0,
                key: "".to_string(),
                value: "".to_string(),
            },
        };
        Raft {
            client,
            server,
            state: RaftState::Follower,
            current_term: 0,
            last_logterm: 0,
            last_logindex: 0,
            id,
            raft_logs: vec![raftlog],
            next_index: HashMap::new(),
            commit_index: 0,
            last_applied: 0,
            servers: HashMap::new(),
            leader: ("".to_string(), "".to_string()),
            timeout_thread: None,
            timer_thread: None,
        }
    }
    pub fn get_state(&self) -> (usize, &RaftState) {
        let term = self.current_term;
        let state = &self.state;
        (term, state)
    }

    pub fn request_vote(&self, args: RequestVateArg) -> RequestVateReply {
        let vote_grante;
        if args.term >= self.current_term && args.last_logterm >= self.last_logterm &&
            args.last_logindex >= self.last_logindex
        {
            vote_grante = true;
        } else {
            vote_grante = false;
        }
        RequestVateReply {
            vote_grante,
            term: self.current_term,
        }

    }

    pub fn send_request_vote(&self, serverip: String, args: RequestVateArg) -> bool {
        // let args = String::new();
        // let mut reqargs = String::new();
        // reqargs += &args.term.to_arg();
        // reqargs += &args.candidateid.to_arg();
        // reqargs += &args.last_logindex.to_arg();
        // reqargs += &args.last_logterm.to_arg();
        let reqmsg = serde_json::to_string(&args).unwrap();
        println!("remsg is: {}", reqmsg);
        let (ok, reply) = self.client.call(
            serverip,
            "Raft.Vote".to_string(),
            reqmsg.to_arg(),
        );
        if ok == false {
            return false;
        }
        if let Ok(reply) = RequestVateReply::replymsg_to_votereply(reply) {
            if reply.vote_grante == true {
                return true;
            }
        }
        false
    }

    pub fn send_vote(&self, serverip: String) -> bool {
        self.send_request_vote(
            serverip,
            RequestVateArg {
                term: self.current_term,
                candidateid: self.id.clone(),
                last_logindex: self.last_logindex,
                last_logterm: self.last_logterm,
            },
        )
    }

    fn handle_vote(&self, reqmsg: Reqmsg) -> Replymsg {
        println!("args is: {}", reqmsg.args[0]);
        let vote_arg: RequestVateArg = serde_json::from_str(&reqmsg.args[0]).unwrap();
        /* let vote_arg = match RequestVateArg::reqmsg_to_votearg(reqmsg) {
            Ok(arg) => arg,
            Err(err) => {
                return Replymsg {
                    ok: false,
                    reply: vec![err.to_arg()],
                };
            }
        };*/
        let vote_reply = self.request_vote(vote_arg);
        let mut reply = String::new();
        reply += &vote_reply.vote_grante.to_arg();
        reply += &vote_reply.term.to_arg();
        println!("reply is {}", reply);
        Replymsg {
            ok: true,
            reply: vec![reply],
        }
    }

    fn send_hbmsg(&self, serverip: String) {
        let mut hbmsg = String::new();
        hbmsg += &self.leader.0.to_arg();
        hbmsg += &self.leader.1.to_arg();
        hbmsg += &self.current_term.to_arg();
        self.client.call(serverip, "Raft.Hbmsg".to_string(), hbmsg);
    }

    fn send_log(&self, serverip: String, logcount: usize) {
        // let mut logmsg = String::new();
        // let prev_log_index = match self.next_index.get(&serverip) {
        //     Some(index) => *index,
        //     None => {
        //         return;
        //     }
        // };
        // logmsg += &self.current_term.to_arg();
        // logmsg += &self.leader.1.to_arg();
        // logmsg += &prev_log_index.to_arg();
        // logmsg += &self.raft_logs[prev_log_index].term.to_arg();
        // for i in 1..logcount + 1 {
        //     logmsg += &self.raft_logs[prev_log_index + i].term.to_arg();
        //     logmsg += &self.raft_logs[prev_log_index + i].command.to_arg();
        // }
        // self.client.call(
        //     serverip,
        //     "Raft.Logmsg".to_string(),
        //     logmsg,
        // );
    }

    fn handle_hbmsg(&mut self, reqmsg: Reqmsg) -> Replymsg {
        println!("{} recived hbmsg", self.id);
        self.leader = (reqmsg.args[0].to_string(), reqmsg.args[1].to_string());
        self.state = RaftState::Follower;
        self.current_term = reqmsg.args[2].parse().unwrap();
        self.reset_timeout();
        Replymsg {
            ok: true,
            reply: vec!["Reply from hbmsg".to_arg()],
        }
    }

    fn handle_addservers(&mut self, mut reqmsg: Reqmsg) -> Replymsg {
        loop {
            if let Some(serverip) = reqmsg.args.pop() {
                if let Some(servername) = reqmsg.args.pop() {
                    self.add_server(servername, serverip);
                }
            } else {
                break;
            }
        }
        Replymsg {
            ok: true,
            reply: vec!["Reply from addserver".to_arg()],
        }
    }

    fn handle_reqmsg(&mut self, reqmsg: Reqmsg) -> Replymsg {
        println!("Method is {}", reqmsg.methodname);
        if reqmsg.methodname == "Vote".to_string() {
            return self.handle_vote(reqmsg);
        }

        if reqmsg.methodname == "Hbmsg".to_string() {
            return self.handle_hbmsg(reqmsg);
        }

        if reqmsg.methodname == "AddServers".to_string() {
            return self.handle_addservers(reqmsg);
        }

        if reqmsg.methodname == "AppendLog".to_string() {
            let append_reply = self.handle_append_log(reqmsg);
            return Replymsg {
                ok: true,
                reply: vec![serde_json::to_string(&append_reply).unwrap().to_arg()],
            };
        }


        Replymsg {
            ok: false,
            reply: vec![],
        }

    }
    fn add_server(&mut self, servername: String, serverip: String) {
        self.servers.insert(servername, serverip);
    }

    fn add_service(raft: Arc<Mutex<Raft>>, id: usize) {

        let own = raft.lock().unwrap().server.add_service(id);
        let raft = Arc::clone(&raft);
        thread::spawn(move || loop {
            let reqmsg = own.receiver.recv().unwrap();
            println!("at service thread");
            reqmsg.print_req();
            let mut raft = raft.lock().unwrap();
            println!("at raft locked");
            let reply = raft.handle_reqmsg(reqmsg);
            own.sender.send(reply).unwrap();
            println!("finished listen");
        });
    }

    fn handle_append_log(&mut self, reqmsg: Reqmsg) ->Append_entry_reply {
        let mut arg: Append_entry_arg = serde_json::from_str(&reqmsg.args[0]).unwrap();
        let last_index = self.raft_logs.len() - 1;
        let mut success = false;
        // 任期不一致
        self.reset_timeout();
        if self.current_term > arg.term {
            return Append_entry_reply{
                success: false,
                term: self.current_term,
                last_index: self.last_logindex,
            };
        } else {
            self.current_term = arg.term;
            self.state = RaftState::Follower;
        }
        // 心跳包
        println!("call handle append");
        if arg.entries.len() == 0 {
            println!("is a heart beat");
            success = true;
        } else if arg.prevLogIndex == self.raft_logs[last_index].index {
            if arg.prevLogTerm == self.raft_logs[last_index].term {
                self.raft_logs.append(&mut arg.entries);
                self.last_logindex += arg.entries.len();
                success = true;
            } else {
                self.raft_logs.truncate(arg.prevLogIndex);
                self.last_logindex = arg.prevLogIndex - 1;
            }
        } else if arg.prevLogIndex < self.raft_logs[last_index].index {
            if self.raft_logs[arg.prevLogIndex].term == arg.prevLogTerm {
                self.raft_logs.truncate(arg.prevLogIndex + 1);
                self.last_logindex = arg.prevLogIndex;
                self.raft_logs.append(&mut arg.entries);
                self.last_logindex += arg.entries.len();
                success = true;
            } else {
                self.raft_logs.truncate(arg.prevLogIndex);
                self.last_logindex = arg.prevLogIndex - 1;
            }
        } 
        Append_entry_reply{
            success,
            term: self.current_term,
            last_index: self.last_logindex,
        }
    }

    fn send_append_log(&mut self, serverip: String, prev_index: usize, 
                to_commit: usize) ->bool { 
        println!("call send append"); 
        let append_arg = Append_entry_arg {
            term: self.current_term,
            leaderid: self.id.clone(),
            prevLogIndex: self.raft_logs[prev_index].index,
            prevLogTerm: self.raft_logs[prev_index].term,
            entries: self.raft_logs[prev_index + 1..to_commit].to_vec(),
            leaderCommit: self.commit_index,
        };
        let arg = serde_json::to_string(&append_arg).unwrap();
        let (ok, reply) = self.client.call(serverip.clone(), "Raft.AppendLog".to_string(), arg.to_arg());
        if ok == false {
            return false;
        }
        let reply: Append_entry_reply = serde_json::from_str(&reply.reply[0]).unwrap();
        if reply.success == false {
            if reply.term > self.current_term {
                self.current_term = 0;
            }
            self.next_index.insert(serverip, reply.last_index + 1);
            return false;
        }
        true
        
    }
    fn add_timeout(raft: Arc<Mutex<Raft>>, servers: Arc<Mutex<HashMap<String, String>>>) {
        let raft_clone = Arc::clone(&raft);
        let thread = thread::spawn(move || {
            loop {
                {
                let raft = raft.lock().unwrap();
                    match raft.state {
                        RaftState::Leader => {
                            mem::drop(raft);
                            thread::park();
                        }
                        _ => {
                        }
                    }
                }
                let rand_sleep = Duration::from_millis(rand::thread_rng().gen_range(5000, 6000));
                let beginning_park = Instant::now();
                thread::park_timeout(rand_sleep);
                println!("time out");
                let elapsed = beginning_park.elapsed();
                if elapsed >= rand_sleep {
                    let servers = servers.lock().unwrap();
                    println!("Real time out");
                    {
                        let mut raft = raft.lock().unwrap();
                        raft.state = RaftState::Candidate;
                        raft.current_term += 1;
                        println!("raft locked");
                    }
                    println!("Raft change finished");
                    let mut passed = 0;
                    println!("Begin send vote");
                    for (_, serverip) in servers.iter() {
                        let raft = raft.lock().unwrap();
                        match raft.state {
                            RaftState::Candidate => {
                                if raft.send_vote(serverip.to_string()) == true {
                                    passed += 1;
                                } else {
                                    println!("Wait vote for {} failed", serverip);
                                }
                            }
                            _ => {
                                passed = 0;
                                break;
                            }
                        }
                    }
                    println!("{} passed", passed);
                    // 超过半数同意
                    if passed + 1 > servers.len() / 2 {
                        let mut raft = raft.lock().unwrap();
                        raft.state = RaftState::Leader;
                        println!("{} become leader term is {}", raft.id, raft.current_term);
                        let next_index = raft.last_logindex + 1;
                        for (_, serverip) in servers.iter() {
                            raft.next_index.insert(serverip.to_string(), next_index);
                        }
                        raft.timer_start();
                    }
                }
                println!("finished timeout");
            }
        });
        raft_clone.lock().unwrap().timeout_thread = Some(thread);
        println!("Raft add timeout finished");
    }

    // fn add_timer(raft: Arc<Mutex<Raft>>, servers: Arc<Mutex<HashMap<String, String>>>) {
    //     let raft_clone = Arc::clone(&raft);
    //     let thread = thread::spawn(move || loop {
    //         {
    //             let raft = raft.lock().unwrap();
    //             match raft.state {
    //                 RaftState::Leader => {}
    //                 _ => {
    //                     mem::drop(raft);
    //                     thread::park();
    //                 }
    //             }
    //         }
    //         let timer_sleep = Duration::from_millis(100);
    //         thread::park_timeout(timer_sleep);
    //         let servers = servers.lock().unwrap();
    //         for (_, serverip) in servers.iter() {
    //             let raft = raft.lock().unwrap();
    //             match raft.state {
    //                 RaftState::Leader => raft.send_hbmsg(serverip.to_string()),
    //                 _ => {
    //                     break;
    //                 }
    //             }
    //         }
    //     });
    //     raft_clone.lock().unwrap().timer_thread = Some(thread);
    // }
    fn add_timer(raft: Arc<Mutex<Raft>>, servers: Arc<Mutex<HashMap<String, String>>>) {
        let raft_clone = Arc::clone(&raft);
        let thread = thread::spawn(move || loop {
            {
                let raft = raft.lock().unwrap();
                match raft.state {
                    RaftState::Leader => {}
                    _ => {
                        mem::drop(raft);
                        thread::park();
                    }
                }
            }
            let timer_sleep = Duration::from_millis(100);
            thread::park_timeout(timer_sleep);
            println!("Start work");
            let servers = servers.lock().unwrap();
            let mut to_commit = 0; 
            {
                let raft = raft.lock().unwrap();
                to_commit = raft.last_logindex + 1;
            }
            for (_, serverip) in servers.iter() {
                let mut next_index = 0;
                {
                    let mut raft = raft.lock().unwrap();
                    next_index = *raft.next_index.get(&serverip.to_string()).unwrap();
                }
                let mut raft = raft.lock().unwrap();
                match raft.state {
                    RaftState::Leader => {
                            raft.send_append_log(serverip.to_string(), next_index - 1, to_commit);
                    },
                    _ => {
                        break;
                    }
                }
            }
            println!("finished worker");
        });
        raft_clone.lock().unwrap().timer_thread = Some(thread);
    }

    // fn worker(raft: Arc<Mutex<Raft>>, servers: Arc<Mutex<HashMap<String, String>>>) {
    //     let thread = thread::spawn(move || loop {
    //         {
    //             let raft = raft.lock().unwrap();
    //             match raft.state {
    //                 RaftState::Leader => {}
    //                 _ => {
    //                     mem::drop(raft);
    //                     thread::park();
    //                 }
    //             }
    //         }
    //         let timer_sleep = Duration::from_millis(100);
    //         thread::park_timeout(timer_sleep);
    //         let servers = servers.lock().unwrap();
    //         let mut to_commit = 0; 
    //         {
    //             let raft = raft.lock().unwrap();
    //             to_commit = raft.last_logindex + 1;
    //         }
    //         for (_, serverip) in servers.iter() {
    //             let mut next_index = 0;
    //             {
    //                 let mut raft = raft.lock().unwrap();
    //                 next_index = *raft.next_index.get(&serverip.to_string()).unwrap();
    //             }
    //             let mut raft = raft.lock().unwrap();
    //             match raft.state {
    //                 RaftState::Leader => {
    //                         raft.send_append_log(serverip.to_string(), next_index, to_commit);
    //                 },
    //                 _ => {
    //                     break;
    //                 }
    //             }
    //         }
    //     });
    // }


    fn reset_timeout(&self) {
        match self.timeout_thread {
            Some(ref thread) => {
                thread.thread().unpark();
            }
            None => {
                return;
            }
        }
    }

    fn timer_start(&self) {
        match self.timer_thread {
            Some(ref thread) => {
                thread.thread().unpark();
            }
            None => {
                return;
            }
        }
    }
}

// pub struct RaftLog {
//     pub term: usize,
//     command: String,
// }
// pub fn find_raft_log(raft_logs: &Vec<RaftLog>, term: usize) -> (usize, usize) {
//     let mut index = raft_logs.len();
//     let (start, end);
//     if index == 0 {
//         return (0, 0);
//     }
//     index -= 1;
//     loop {
//         if raft_logs[index].term == term {
//             end = index;
//             break;
//         }
//         index -= 1;
//         if index == 0 {
//             return (0, 0);
//         }
//     }
//     loop {
//         if raft_logs[index].term != term {
//             start = index;
//             break;
//         }
//         index -= 1;
//         if index == 0 {
//             start = 0;
//             break;
//         }
//     }
//     (start, end)
// }
