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

impl Clone for RaftState {
    fn clone(&self) -> Self {
        match *self {
            RaftState::Follower => RaftState::Follower,
            RaftState::Candidate => RaftState::Candidate,
            RaftState::Leader => RaftState::Leader,
            
        }
    }

    fn clone_from(&mut self, source: &Self) {
        
    }

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
        let server = RpcServer::new("raft".to_string(), rpcport);
        let raft = Raft::new(server, serverip);
        let raft = Arc::new(Mutex::new(raft));
        let receiver = Raft::timeout_count(Arc::clone(&raft), 1000, 2000);
        Raft::add_timeout(Arc::clone(&raft), Arc::clone(&servers), receiver);
        Raft::add_timer(Arc::clone(&raft), Arc::clone(&servers));
        Raft::add_service(Arc::clone(&raft), 0);
        RaftServer { servers, raft }
    }

    pub fn add_raft_server(&self, serverid: String, serverip: String) {
        self.servers.lock().unwrap().insert(serverid, serverip);
    }

    pub fn put_value(&self, key: String, value: String) -> bool {
        let command = LogCommand {
            op: 1,
            key: key.clone(),
            value: value.clone(),
        };
        let mut raft = self.raft.lock().unwrap();
        let index = raft.last_logindex + 1;
        let term = raft.current_term;
        raft.raft_logs.push(RaftLog {
            term,
            index,
            command,
        });
        raft.last_logindex += 1;
        true
    }

    pub fn get_value(&self, key: String) -> String {
        self.raft.lock().unwrap().data.get(&key).unwrap().to_string()
    }

    pub fn delete_value(&self, key: String) -> bool {
        let value = self.get_value(key.clone());
        if value == "".to_string() {
            return false;
        }
        let command = LogCommand {
            op: 2,
            key: key.clone(),
            value,
        };
        let mut raft = self.raft.lock().unwrap();
        let index = raft.last_logindex;
        let term = raft.current_term;
        raft.raft_logs.push(RaftLog {
            term,
            index,
            command,
        });
        true
    }

}

pub struct Raft {
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
    vote_for: String,
    data: HashMap<String, String>,
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

impl Raft {
    pub fn new(server: RpcServer, id: String) -> Raft {
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
            server,
            state: RaftState::Follower,
            current_term: 0,
            last_logterm: 0,
            last_logindex: 0,
            id,
            raft_logs: vec![raftlog],
            next_index: HashMap::new(),
            commit_index: 1,
            last_applied: 0,
            servers: HashMap::new(),
            leader: ("".to_string(), "".to_string()),
            timeout_thread: None,
            timer_thread: None,
            vote_for: "".to_string(),
            data: HashMap::new(),
        }
    }
    pub fn get_state(&self) -> (usize, &RaftState) {
        let term = self.current_term;
        let state = &self.state;
        (term, state)
    }

    pub fn request_vote(&self, args: RequestVateArg) -> RequestVateReply {
        let vote_grante;
        match self.state {
            RaftState::Leader => {
                vote_grante = false;
                return RequestVateReply {
                    vote_grante,
                    term: self.current_term,
                };
            },
            _ => {},
        }
        self.reset_timeout();
        if args.term < self.current_term {
            vote_grante = false;
        }
        else if args.last_logindex >= self.last_logindex
        {
            // if self.vote_for == "".to_string() || self.vote_for == args.candidateid {
            //     self.vote_for = args.candidateid;
            //     vote_grante = true;
            // } else {
            //     vote_grante = false;
            // }
            vote_grante = true;
        } else {
            vote_grante = false;
        }
        RequestVateReply {
            vote_grante,
            term: self.current_term,
        }
    }

    fn vote_string(&self) ->String {
        let vote = RequestVateArg {
                term: self.current_term,
                candidateid: self.id.clone(),
                last_logindex: self.last_logindex,
                last_logterm: self.last_logterm,
        };
        serde_json::to_string(&vote).unwrap()
    }

    fn handle_vote(&self, reqmsg: Reqmsg) -> Replymsg {
        kv_info!("args is: {}", reqmsg.args);
        let vote_arg: RequestVateArg = serde_json::from_str(&reqmsg.args).unwrap();
        let vote_reply = self.request_vote(vote_arg);
        let reply = serde_json::to_string(&vote_reply).unwrap();
        kv_info!("reply is {}", reply);
        Replymsg {
            ok: true,
            reply,
        }
    }

    fn handle_addservers(&mut self, mut reqmsg: Reqmsg) -> Replymsg {
        Replymsg {
            ok: true,
            reply: "Reply from addserver".to_string(),
        }
    }

    fn handle_reqmsg(&mut self, reqmsg: Reqmsg) -> Replymsg {
        kv_info!("Method is {}", reqmsg.methodname);
        if reqmsg.methodname == "Vote".to_string() {
            return self.handle_vote(reqmsg);
        }

        if reqmsg.methodname == "AppendLog".to_string() {
            let append_reply = self.handle_append_log(reqmsg);
            return Replymsg {
                ok: true,
                reply: serde_json::to_string(&append_reply).unwrap(),
            };
        }

        Replymsg {
            ok: false,
            reply: "Error method".to_string(),
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
            kv_info!("at service thread");
            reqmsg.print_req();
            let mut raft = raft.lock().unwrap();
            kv_info!("at raft locked");
            let reply = raft.handle_reqmsg(reqmsg);
            own.sender.send(reply).unwrap();
            kv_info!("finished listen");
        });
    }

    fn handle_append_log(&mut self, reqmsg: Reqmsg) ->Append_entry_reply {
        let mut arg: Append_entry_arg = serde_json::from_str(&reqmsg.args).unwrap();
        let last_index = self.raft_logs.len() - 1;
        let mut success = false;
        // 任期不一致
        self.state = RaftState::Follower;
        self.reset_timeout();
        let mut last_log_index = self.last_logindex;
        if self.current_term > arg.term {
            return Append_entry_reply{
                success: false,
                term: self.current_term,
                last_index: self.last_logindex,
            };
        } else {
            self.current_term = arg.term;
        }
        // 心跳包
        kv_info!("call handle append");
        if arg.entries.len() == 0 {
            kv_debug!("is a heart beat");
            success = true;
            return Append_entry_reply{
                success,
                term: self.current_term,
                last_index: self.last_logindex,
            };
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

        if success == true{
            for log in arg.entries {
                let op = log.command.op;
                let key = log.command.key;
                let value = log.command.value;
                match op{
                    1 => {
                        self.data.insert(key, value);
                    },
                    2 =>{
                        self.data.insert(key, "".to_string());
                    },
                    _=>{},
                }
            }
            self.commit_index = self.last_logindex;
        }
        Append_entry_reply{
            success,
            term: self.current_term,
            last_index: self.last_logindex,
        }
    }

    fn append_log_to_string(&self, prev_index: usize, 
                to_commit: usize) ->String {
        let append_arg = Append_entry_arg {
            term: self.current_term,
            leaderid: self.id.clone(),
            prevLogIndex: self.raft_logs[prev_index].index,
            prevLogTerm: self.raft_logs[prev_index].term,
            entries: self.raft_logs[prev_index + 1..to_commit].to_vec(),
            leaderCommit: self.commit_index,
        };
        serde_json::to_string(&append_arg).unwrap()
    }

    fn timeout_count(raft: Arc<Mutex<Raft>>, start: u32, end: u32) 
            ->mpsc::Receiver<u32> {
        let (sender, receiver) = mpsc::channel();
        let raft_clone = Arc::clone(&raft);
        let thread = thread::spawn(move || {
        loop {
            let state; {
                let raft = raft.lock().unwrap();
                state = raft.state.clone();
            }
            match state{
                RaftState::Leader => {
                    thread::park();
                },
                 _ => {},
            }
            let rand_sleep = Duration::from_millis(rand::thread_rng().gen_range(1500, 3000));
            let beginning_park = Instant::now();
            thread::park_timeout(rand_sleep);
            let elapsed = beginning_park.elapsed();
            if elapsed >= rand_sleep {
                kv_debug!("Real time out");
                sender.send(1).unwrap();
            }
        }
        });
        raft_clone.lock().unwrap().timeout_thread = Some(thread);
        kv_note!("Raft add timeout counter");
        receiver
    }
    fn add_timeout(raft: Arc<Mutex<Raft>>, servers: Arc<Mutex<HashMap<String, String>>>,
                    receiver: mpsc::Receiver<u32>) {
        let thread = thread::spawn(move || {
            loop {
                receiver.recv().unwrap();
                kv_debug!("Begin send vote");
                let servers = servers.lock().unwrap();
                {
                    let mut raft = raft.lock().unwrap();
                    match raft.state {
                         RaftState::Leader => continue,
                         _ => {},
                    }
                    raft.state = RaftState::Candidate;
                    raft.vote_for = raft.id.clone();
                    raft.current_term += 1;
                }
                let passed = Arc::new(Mutex::new(0));
                let mut threads = vec![];
                for (_, serverip) in servers.iter() {
                    let state;{
                        let raft = raft.lock().unwrap();
                        state = raft.state.clone();
                    }
                    match state {
                        RaftState::Candidate => {
                            let vote; {
                                let raft = raft.lock().unwrap();
                                vote = raft.vote_string();
                            }
                            let passed = Arc::clone(&passed);
                            let serverip = serverip.to_string();
                            threads.push(thread::spawn(move || {  
                            let (ok, reply) = rpc_call(serverip,
                                    "Raft.Vote".to_string(), vote);
                            if ok == false || reply.ok == false { 
                                // continue;
                            } else {
                                let vote_reply: RequestVateReply = 
                                    serde_json::from_str(&reply.reply).unwrap();
                                if vote_reply.vote_grante == true {
                                    *passed.lock().unwrap() += 1;
                                }
                            }
                            }));
                        },
                         _ => {
                            *passed.lock().unwrap() = 0;
                        },
                    }
                }
                for thread in threads {
                    thread.join().unwrap();   
                }
                let passed = *passed.lock().unwrap();                
                kv_debug!("{} passed", passed);
                // 超过半数同意
                if passed + 1 > servers.len() / 2 {
                    let mut raft = raft.lock().unwrap();
                    match raft.state {
                        RaftState::Candidate => {
                            raft.state = RaftState::Leader;
                            kv_debug!("{} become leader term is {}", raft.id, raft.current_term);
                            let next_index = raft.last_logindex + 1;
                            for (_, serverip) in servers.iter() {
                                raft.next_index.insert(serverip.to_string(), next_index);
                            }
                            raft.timer_start();
                        },
                        _ =>{},
                    }
                } else {
                    let mut raft = raft.lock().unwrap();                    
                    raft.vote_for = "".to_string();
                }
                kv_debug!("Finished once vote");
            }
        });
    }

    fn add_timer(raft: Arc<Mutex<Raft>>, servers: Arc<Mutex<HashMap<String, String>>>) {
        let raft_clone = Arc::clone(&raft);
        let thread = thread::spawn(move || loop {
            {
                let mut raft = raft.lock().unwrap();
                match raft.state {
                    RaftState::Leader => {}
                    _ => {
                        mem::drop(raft);
                        thread::park();
                    }
                }
            }
            let timer_sleep = Duration::from_millis(10);
            thread::park_timeout(timer_sleep);
            kv_debug!("Start work");
            let servers = servers.lock().unwrap();
            let to_commit; 
            //let mut passed = 0;
            let passed = Arc::new(Mutex::new(0));
            {
                let raft = raft.lock().unwrap();
                to_commit = raft.last_logindex + 1;
            }
            let mut threads = vec![];
            for (_, serverip) in servers.iter() {
                let next_index;
                let state;
                {
                    let mut raft = raft.lock().unwrap();
                    state = raft.state.clone();
                    next_index = *raft.next_index.get(&serverip.to_string()).unwrap();
                }
                match state {
                    RaftState::Leader => {
                        let arg;{
                            let raft = raft.lock().unwrap();
                            arg = raft.append_log_to_string(next_index - 1, to_commit);
                        }
                        let passed = Arc::clone(&passed);
                        let raft = Arc::clone(&raft);
                        let serverip = serverip.to_string();
                        threads.push(thread::spawn(move || { 
                        let (ok, reply) = rpc_call(serverip.to_string(), 
                                "Raft.AppendLog".to_string(), arg);
                        {
                            if ok == false || reply.ok == false {

                            }else {
                                let reply: Append_entry_reply = serde_json::from_str(&reply.reply).unwrap();
                                if reply.success == false {
                                    let mut raft = raft.lock().unwrap();
                                    if reply.term > raft.current_term {
                                        raft.current_term = reply.term;
                                    }
                                    raft.next_index.insert(serverip.to_string(), reply.last_index + 1);
                                } else {
                                    //passed += 1;
                                    *passed.lock().unwrap() += 1;
                                }
                            }    
                        }}));
                    },
                    _ => {
                        *passed.lock().unwrap() = 0;
                    }
                }
            }
            for thread in threads {
                thread.join().unwrap();   
            }
            let passed = *passed.lock().unwrap(); 
            if passed + 1 > servers.len() / 2 {
                let mut raft = raft.lock().unwrap();
                while raft.commit_index < to_commit{
                    let log = raft.raft_logs[raft.commit_index].clone();
                    let op = log.command.op;
                    let key = log.command.key;
                    let value = log.command.value;
                    match op{
                        1 => {
                            raft.data.insert(key, value);
                        },
                        2 => {
                            raft.data.insert(key, "".to_string());
                        },
                        _ =>{

                        },
                    }
                    raft.commit_index += 1;
                }
            } 
            kv_debug!("finished worker");
        });
        raft_clone.lock().unwrap().timer_thread = Some(thread);
    }

    

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
