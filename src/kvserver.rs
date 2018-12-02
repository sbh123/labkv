use super::rpc::*;
use super::common::*;
use super::*;
use super::raft::*;

use std::collections::HashMap;

pub struct KvServer{
    pub raftnote: RaftServer,
    pub clientseqs: HashMap<Uuid, i32>,
    pub rpcserver: RpcServer,
}

impl KvServer {
    pub fn new(rpcport: u16, port: u16) -> KvServer {
        KvServer{
            raftnote: RaftServer::new(String::from("127.0.0.1"),port),
            clientseqs: HashMap::new(),
            rpcserver: RpcServer::new("127.0.0.1".to_string(), rpcport),
        }
    }

    pub fn StartCommand(&mut self,ClientId: Uuid,Seq:& i32) -> String {
        if self.raftnote.is_leader() == false {
            return String::from("WrongLeader");
        } else {
            let mut clientseqs = self.clientseqs.clone();
            let reu = clientseqs.get(&ClientId).clone();
            match reu {
                None => {
                    self.clientseqs.insert(ClientId,Seq+1);
                    return "OK".to_string();
                },
                Some(i) => {
                    if Seq >= i {
                        self.clientseqs.insert(ClientId,Seq+1);
                        return "OK".to_string();
                    } else {
                        return "repeat".to_string();
                    }
                },
            };
        }
    }

    pub fn StartKVServer(&mut self){
        let own = self.rpcserver.add_service(0);
        loop {
            let reqmsg = own.receiver.recv().unwrap();
            if reqmsg.methodname == String::from("Put") {
                let Putargs: PutAppendArgs = serde_json::from_str(&reqmsg.args).unwrap();
                println!("put info : {:#?}",Putargs);
                
                let ready = self.StartCommand(Putargs.id.clone(),&Putargs.seq);
                let mut Putreply = PutAppendReply{
                    wrong_leader:false,
                    err: String::from("OK"),
                };
                
                if ready == "OK".to_string() {
                    let res = self.raftnote.put_value(Putargs.key.clone(),Putargs.value.clone());
                }  else if ready == String::from("WrongLeader") {
                    Putreply.wrong_leader = true;
                } else {}
                let putreply = serde_json::to_string(&Putreply).unwrap();
                let putreply = Replymsg{
                    ok: true,
                    reply:putreply,
                };
                own.sender.send(putreply).unwrap();
            } else if reqmsg.methodname == String::from("Get"){
                let Getargs: GetArgs = serde_json::from_str(&reqmsg.args).unwrap();
                println!("get info : {:#?}",Getargs);

                let ready = self.StartCommand(Getargs.id.clone(),&Getargs.seq);
                
                let mut Getreply = GetReply{
                    wrong_leader: false,
                    err: String::from("OK"),
                    value:String::from(""),
                };

                if ready == "OK".to_string() {
                    let res = self.raftnote.get_value(Getargs.key.clone());
                    Getreply.value=res.clone();
                }  else if ready == String::from("WrongLeader") {
                    Getreply.wrong_leader = true;
                } else {}

                let getreply = serde_json::to_string(&Getreply).unwrap();
                let getreply = Replymsg{
                    ok: true,
                    reply:getreply,
                };
                own.sender.send(getreply).unwrap();
            } else {
                println!("error methodname");
            }
        }
    }
}

