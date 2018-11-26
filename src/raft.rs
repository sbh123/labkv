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

pub enum Raft_state{
    Follower,
    Candidate,
    Leader,
}


pub fn time_count() ->mpsc::Receiver<bool> {
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        loop {
            let beginning_park = Instant::now();
            let mut rand_sleep = Duration::from_millis(
                    rand::thread_rng().gen_range(150, 300));
            thread::park_timeout(rand_sleep);
            let elapsed = beginning_park.elapsed();
            if elapsed >= rand_sleep {
                sender.send(true).unwrap();
            }     
        }
            
    });
    receiver
}


pub struct Raft {
    client: ClientEnd,
    server: Server,
    mu: i32,
    state: Raft_state,
    currentTerm: u32,
    lastLogTerm: u32,
    lastLogIndex: u32,
    id: String,
    // election_time: Arc<Mutex<u32>>,
    servers: HashMap<String, String>,
    leader: (String, String),
    timeout_listen: Option<thread::JoinHandle<()>>,
}

pub struct RequestVateArg {
    term: u32,
    candidateid: String,
    lastLogIndex: u32,
    lastLogTerm: u32,
}

pub struct RequestVateReply {
    voteGrante: bool,
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
    fn description(&self) ->&str {
        match *self {
            RaftError::Io(ref err) => err.description(),
            RaftError::Info(ref info) => &info[..],
        }
    }
}

impl RequestVateArg {
    pub fn ReqmsgtoVote(reqmsg :Reqmsg) ->Result<RequestVateArg, RaftError> {
        if reqmsg.args.len() < 4 {
            return Err(RaftError::Info("Wrong args".to_string()));
        }
        let term: u32 = reqmsg.args[0].trim().parse().unwrap();
        let candidateid: String = reqmsg.args[1].clone();
        let lastLogIndex: u32 = reqmsg.args[1].trim().parse().unwrap();
        let lastLogTerm: u32 = reqmsg.args[1].trim().parse().unwrap();
        Ok(RequestVateArg {
            term,
            candidateid,
            lastLogIndex,
            lastLogTerm,
        })
    }
}

impl RequestVateReply {
    pub fn ReplymsgToVoteReply(reply: Replymsg) ->Result<RequestVateReply, RaftError>{
        if reply.reply.len() < 2 {
            return Err(RaftError::Info("Eri reply".to_string()));
        }
        let voteGrante: bool = reply.reply[0].trim().parse().unwrap();
        let term: u32 = reply.reply[1].trim().parse().unwrap();
        Ok(RequestVateReply {
            voteGrante,
            term,
        })
    }
}

impl Raft {
    pub fn new(client: ClientEnd, server: Server, id: String, mu: i32) ->Raft {
        Raft {
            client,
            server,
            mu, 
            state: Raft_state::Follower,
            currentTerm: 0,
            lastLogTerm: 0,
            lastLogIndex: 0,
            id,
            servers: HashMap::new(),
            leader: ("".to_string(), "".to_string()),
            timeout_listen: None,
        }
    }
    pub fn get_state(&self) ->(u32, &Raft_state) {
        let term = self.currentTerm;
        let state = &self.state;
        (term, state)
    }

    pub fn request_vote(&self, args: RequestVateArg) ->RequestVateReply {
        let voteGrante;
        if args.term >=  self.currentTerm && args.lastLogTerm >= self.lastLogTerm 
            && args.lastLogIndex >= self.lastLogIndex {
            voteGrante = true;
        } else {
            voteGrante = false;
        }
        RequestVateReply {
            voteGrante,
            term: self.currentTerm,
        }

    } 

    pub fn sendRequestVote(&self, servername: String, args: RequestVateArg) ->bool {
        // let args = String::new(); 
        let mut reqargs = String::new();
        reqargs += &args.term.to_arg();
        reqargs += &args.candidateid.to_arg();
        reqargs += &args.lastLogIndex.to_arg();
        reqargs += &args.lastLogTerm.to_arg();
        let (ok, reply) = self.client.call(servername, "Vote".to_string(), reqargs);
        if ok ==  false {
            return false;
        }
        if let Ok(reply) =  RequestVateReply::ReplymsgToVoteReply(reply){
            if reply.voteGrante == true {
                return true;
            }
        }
        false
    }

    pub fn send_vote(&self, servername: String) ->bool {
        self.sendRequestVote(servername, RequestVateArg {
            term: self.currentTerm,
            candidateid: self.id.clone(),
            lastLogIndex: self.lastLogIndex,
            lastLogTerm: self.lastLogTerm,
        })
    }

    fn handle_vote(&self, reqmsg: Reqmsg) ->Replymsg {
        let vote_arg = match RequestVateArg::ReqmsgtoVote(reqmsg){
            Ok(arg) => arg,
            Err(err) => {
                return Replymsg {
                    ok: false,
                    reply: vec![err.to_arg()],
                };
            },
        };
        let vote_reply =  self.request_vote(vote_arg);
        let mut reply = String::new();
        reply += &vote_reply.voteGrante.to_arg();
        reply += &vote_reply.term.to_arg();
        Replymsg {
            ok: true,
            reply: vec![reply],
        }
    }

    fn hadle_hbmsg(&self, reqmsg: Reqmsg) {


    }

    fn handle_reqmsg(&self, reqmsg: Reqmsg) ->Replymsg{
        if reqmsg.methodname == "Vote".to_string() {
            return self.handle_vote(reqmsg);
        }

        Replymsg {
            ok: false,
            reply: vec![],
        }

    }
    fn add_server(&mut self, servername: String, ip: String) {
        self.servers.insert(servername, ip);
    }
    fn add_service(raft: Arc<Mutex<Raft>>, id: usize) {

        let own = raft.lock().unwrap().server.add_service(0);
        let raft = Arc::clone(&raft);
        thread::spawn(move || {
            let reqmsg = own.receiver.recv().unwrap();
            let reply = raft.lock().unwrap().handle_reqmsg(reqmsg);
            own.sender.send(reply).unwrap();
        });
    }

    fn vote_for_leader(&mut self) ->bool {
        let mut passed  = 0;
        for (_ , servername) in &self.servers {
            if self.send_vote(servername.to_string()) == true {
                passed += 1;
            }
        }
        if passed > self.servers.len() / 2 {
            return true;
        }
        false
    }
    
    fn add_timeout(raft: Arc<Mutex<Raft>>) {
        let receiver = time_count();
        let raft_clone = Arc::clone(&raft);
        let thread = thread::spawn(move || {
            let timeout = receiver.recv().unwrap();
            if timeout == true {
                let servers = &raft_clone.lock().unwrap().servers;
                raft_clone.lock().unwrap().state = Raft_state::Candidate;
                let mut passed  = 0; 
                for (_, servername) in servers {
                    match raft_clone.lock().unwrap().state {
                        Raft_state::Candidate => {
                            if raft_clone.lock().unwrap().send_vote(servername.to_string()) == true {
                                passed += 1;
                            }
                        },
                        _ => {
                            passed = 0;
                            break;
                        }
                    }
                }
                // 超过半数同意
                if passed >= servers.len() / 2 {

                }

            }
        });
        raft.lock().unwrap().timeout_listen = Some(thread);
    }

    fn reset_timeout(&self) {
        match self.timeout_listen {
            Some(ref thread) => {
                thread.thread().unpark();
            },
            None =>{
                return;
            },
        }
    }
}

pub fn test_raft() {
    let client = ClientEnd::new("client".to_string());
    let server = Server::new("raft".to_string(), 8080);
    let raft = Raft::new(client, server, "127.0.0.1:8080".to_string(), 0);
}
