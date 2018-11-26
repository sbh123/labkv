use super::rpc::*;
use std::error::Error;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::io;
use std::fmt;

pub enum Raft_state{
    Follower,
    Candidate,
    Leader,
}

pub struct Raft {
    client: ClientEnd,
    server: Server,
    mu: i32,
    state: Raft_state,
    currentTerm: u32,
    lastLogTerm: u32,
    lastLogIndex: u32,
    servers: HashMap<String, String>,
    leader: (String, String),
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
    pub fn new(client: ClientEnd, server: Server, mu: i32) ->Raft {
        Raft {
            client,
            server,
            mu,
            state: Raft_state::Follower,
            currentTerm: 0,
            lastLogTerm: 0,
            lastLogIndex: 0,
            servers: HashMap::new(),
            leader: ("".to_string(), "".to_string()),
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

    pub fn send_request_vote(&self, servername: String, args: RequestVateArg) ->bool {
        let args = format!("{}\n{}\n{}\n{}", args.term, args.candidateid, 
                args.lastLogIndex, args.lastLogTerm);
        let (ok, reply) = self.client.call(servername, "Raft.Vote".to_string(), args);
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

    fn handle_reqmsg(&self, reqmsg: Reqmsg) ->Replymsg{
        Replymsg {
            ok: false,
            reply: vec![],
        }

    }
    fn add_server(&mut self, servername: String, ip: String) {
        self.servers.insert(servername, ip);
    }
    fn add_service(raft: Arc<Mutex<Raft>>, id: usize) {

        let own = raft.lock().unwrap().server.add_service("Raft".to_string());
        let raft = Arc::clone(&raft);
        thread::spawn(move || {
            let reqmsg = own.receiver.recv().unwrap();
            let reply = raft.lock().unwrap().handle_reqmsg(reqmsg);
            own.sender.send(reply).unwrap();
        });
    }
}
