extern crate rand;
use raft::rand::Rng;

use super::rpc::*;
use std::error::Error;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::io;
use std::fmt;
use std::time::{Instant, Duration};

pub enum RaftState {
    Follower,
    Candidate,
    Leader,
}

pub struct RaftServer {
    servers: Arc<Mutex<HashMap<String, String>>>,
    raft: Arc<Mutex<Raft>>,
}

impl RaftServer {
    fn new(serverip: String, rpcport: u16) -> RaftServer {
        let servers = Arc::new(Mutex::new(HashMap::new()));
        let client = ClientEnd::new("client".to_string());
        let server = RpcServer::new("raft".to_string(), rpcport);
        let raft = Raft::new(client, server, serverip);
        let raft = Arc::new(Mutex::new(raft));
        Raft::add_timeout(Arc::clone(&raft));
        Raft::add_timer(Arc::clone(&raft));
        Raft::add_service(Arc::clone(&raft), 0);
        RaftServer { servers, raft }
    }

    fn add_raft_server(&self, serverid: String, serverip: String) {
        self.servers.lock().unwrap().insert(serverid, serverip);
    }
}

pub struct Raft {
    client: ClientEnd,
    server: RpcServer,
    state: RaftState,
    current_term: u32,
    last_logterm: u32,
    last_logindex: u32,
    id: String,
    raft_logs: Vec<RaftLog>,
    next_index: HashMap<String, usize>,
    // election_time: Arc<Mutex<u32>>,
    servers: HashMap<String, String>,
    leader: (String, String),
    timeout_thread: Option<thread::JoinHandle<()>>,
    timer_thread: Option<thread::JoinHandle<()>>,
}

pub struct RequestVateArg {
    term: u32,
    candidateid: String,
    last_logindex: u32,
    last_logterm: u32,
}

pub struct RequestVateReply {
    vote_grante: bool,
    term: u32,
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
        let term: u32 = reqmsg.args[0].trim().parse().unwrap();
        let candidateid: String = reqmsg.args[1].clone();
        let last_logindex: u32 = reqmsg.args[1].trim().parse().unwrap();
        let last_logterm: u32 = reqmsg.args[1].trim().parse().unwrap();
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
        let term: u32 = reply.reply[1].trim().parse().unwrap();
        Ok(RequestVateReply { vote_grante, term })
    }
}

impl Raft {
    pub fn new(client: ClientEnd, server: RpcServer, id: String) -> Raft {
        Raft {
            client,
            server,
            state: RaftState::Follower,
            current_term: 0,
            last_logterm: 0,
            last_logindex: 0,
            id,
            raft_logs: vec![],
            next_index: HashMap::new(),
            servers: HashMap::new(),
            leader: ("".to_string(), "".to_string()),
            timeout_thread: None,
            timer_thread: None,
        }
    }
    pub fn get_state(&self) -> (u32, &RaftState) {
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
        let mut reqargs = String::new();
        reqargs += &args.term.to_arg();
        reqargs += &args.candidateid.to_arg();
        reqargs += &args.last_logindex.to_arg();
        reqargs += &args.last_logterm.to_arg();
        let (ok, reply) = self.client.call(serverip, "Vote".to_string(), reqargs);
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
        let vote_arg = match RequestVateArg::reqmsg_to_votearg(reqmsg) {
            Ok(arg) => arg,
            Err(err) => {
                return Replymsg {
                    ok: false,
                    reply: vec![err.to_arg()],
                };
            }
        };
        let vote_reply = self.request_vote(vote_arg);
        let mut reply = String::new();
        reply += &vote_reply.vote_grante.to_arg();
        reply += &vote_reply.term.to_arg();
        Replymsg {
            ok: true,
            reply: vec![reply],
        }
    }

    fn send_hbmsg(&self, serverip: String) {
        let mut hbmsg = String::new();
        hbmsg += &self.leader.0.to_arg();
        hbmsg += &self.leader.1.to_arg();
        self.client.call(
            serverip,
            "Raft.Hbmsg".to_string(),
            hbmsg,
        );
    }

    fn send_log(&self, serverip: String, logcount: usize) {
        let mut logmsg = String::new();
        let prev_log_index = match self.next_index.get(&serverip) {
            Some(index) => *index,
            None => {
                return ;
            }
        };
        logmsg += &self.current_term.to_arg();
        logmsg += &self.leader.1.to_arg();
        logmsg += &prev_log_index.to_arg();
        logmsg += &self.raft_logs[prev_log_index].term.to_arg();
        for i in 0..logcount {
            logmsg += &self.raft_logs[prev_log_index + i].term.to_arg();
            logmsg += &self.raft_logs[prev_log_index + i].command.to_arg();
        }
        self.client.call(
            serverip,
            "Raft.Logmsg".to_string(),
            logmsg,
        );
    }

    fn handle_hbmsg(&mut self, reqmsg: Reqmsg) ->Replymsg {
        self.reset_timeout();
        self.leader = (reqmsg.args[0].to_string(), reqmsg.args[1].to_string());
        Replymsg {
            ok: true,
            reply: vec!["Reply from hbmsg".to_arg()],
        }
    }

    fn handle_addservers(&mut self, mut reqmsg: Reqmsg) ->Replymsg{
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
        if reqmsg.methodname == "Vote".to_string() {
            return self.handle_vote(reqmsg);
        }

        if reqmsg.methodname == "Hbmsg".to_string() {
            return self.handle_hbmsg(reqmsg);
        }

        if reqmsg.methodname == "AddServers".to_string() {
            return self.handle_addservers(reqmsg);
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
        thread::spawn(move || {
            let reqmsg = own.receiver.recv().unwrap();
            let reply = raft.lock().unwrap().handle_reqmsg(reqmsg);
            own.sender.send(reply).unwrap();
        });
    }

    fn add_timeout(raft: Arc<Mutex<Raft>>) {
        let raft_clone = Arc::clone(&raft);
        let thread = thread::spawn(move || {
            loop {
                match raft.lock().unwrap().state {
                    RaftState::Leader => {
                        thread::park();
                    }
                    _ => {}
                }
                let rand_sleep = Duration::from_millis(rand::thread_rng().gen_range(150, 300));
                let beginning_park = Instant::now();
                thread::park_timeout(rand_sleep);
                let elapsed = beginning_park.elapsed();
                if elapsed >= rand_sleep {
                    let servers = &raft.lock().unwrap().servers;
                    raft.lock().unwrap().state = RaftState::Candidate;
                    let mut passed = 0;
                    for (_, serverip) in servers {
                        match raft.lock().unwrap().state {
                            RaftState::Candidate => {
                                if raft.lock().unwrap().send_vote(
                                    serverip.to_string(),
                                ) == true
                                {
                                    passed += 1;
                                }
                            }
                            _ => {
                                passed = 0;
                                break;
                            }
                        }
                    }
                    // 超过半数同意
                    if passed >= servers.len() / 2 {
                        raft.lock().unwrap().state = RaftState::Leader;
                        raft.lock().unwrap().timer_start();
                    }
                }
            }
        });
        raft_clone.lock().unwrap().timeout_thread = Some(thread);
    }

    fn add_timer(raft: Arc<Mutex<Raft>>) {
        let raft_clone = Arc::clone(&raft);
        let thread = thread::spawn(move || loop {
            match raft.lock().unwrap().state {
                RaftState::Leader => {}
                _ => {
                    thread::park();
                }
            }
            let timer_sleep = Duration::from_millis(100);
            thread::park_timeout(timer_sleep);
            let servers = &raft.lock().unwrap().servers;
            for (_, serverip) in servers {
                match raft.lock().unwrap().state {
                    RaftState::Leader => {
                        raft.lock().unwrap().send_hbmsg(
                            serverip.to_string(),
                        )
                    }
                    _ => {
                        break;
                    }
                }
            }
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

pub struct RaftLog {
    pub term: u32,
    command: String,
}
pub fn find_raft_log(raft_logs: &Vec<RaftLog>, term: u32) ->(usize, usize) {
    let mut index = raft_logs.len();
    let (start, end);
    if index == 0 {
        return (0, 0);
    }
    index -= 1;
    loop {
        if raft_logs[index].term == term {
            end = index;
            break;
        }
        index -= 1;
        if index == 0 {
            return (0, 0);
        } 
    }
    loop {
        if raft_logs[index].term != term {
            start = index;
            break;
        }
        index -= 1;
        if index == 0 {
            start = 0;
            break;
        }
    }
    (start, end)
}

pub fn test_raft() {
    let raftserver = RaftServer::new("127.0.0.1:8080".to_string(), 8080);
}
