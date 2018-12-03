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

    pub fn start_command(&mut self,id: Uuid,seq:& i32) -> String {
        if self.raftnote.is_leader() == false {
            return String::from("WrongLeader");
        } else {
            let clientseqs = self.clientseqs.clone();
            let reu = clientseqs.get(&id).clone();
            match reu {
                None => {
                    self.clientseqs.insert(id,seq+1);
                    return "OK".to_string();
                },
                Some(i) => {
                    if seq >= i {
                        self.clientseqs.insert(id,seq+1);
                        return "OK".to_string();
                    } else {
                        return "repeat".to_string();
                    }
                },
            };
        }
    }

    pub fn start_kv_server(&mut self){
        let own = self.rpcserver.add_service(0);
        loop {
            let reqmsg = own.receiver.recv().unwrap();
            if reqmsg.methodname == String::from("Put") {
                let put_args: PutAppendArgs = serde_json::from_str(&reqmsg.args).unwrap();
                kv_note!("put info : {:#?}",put_args);
                
                let ready = self.start_command(put_args.id.clone(),&put_args.seq);
                let mut put_reply = PutAppendReply{
                    wrong_leader:false,
                    err: String::from("OK"),
                };
                
                if ready == "OK".to_string() {
                    let _res = self.raftnote.put_value(put_args.key.clone(),put_args.value.clone());
                }  else if ready == String::from("WrongLeader") {
                    put_reply.wrong_leader = true;
                } else {}
                let putreply = serde_json::to_string(&put_reply).unwrap();
                let putreply = Replymsg{
                    ok: true,
                    reply:putreply,
                };
                own.sender.send(putreply).unwrap();
            } else if reqmsg.methodname == String::from("Get"){
                let get_args: GetArgs = serde_json::from_str(&reqmsg.args).unwrap();
                kv_note!("get info : {:#?}",get_args);

                let ready = self.start_command(get_args.id.clone(),&get_args.seq);
                
                let mut get_reply = GetReply{
                    wrong_leader: false,
                    err: String::from("OK"),
                    value:String::from(""),
                };

                if ready == "OK".to_string() {
                    let res = self.raftnote.get_value(get_args.key.clone());
                    get_reply.value=res.clone();
                }  else if ready == String::from("WrongLeader") {
                    get_reply.wrong_leader = true;
                } else {}

                let getreply = serde_json::to_string(&get_reply).unwrap();
                let getreply = Replymsg{
                    ok: true,
                    reply:getreply,
                };
                own.sender.send(getreply).unwrap();
            } else {
                kv_note!("error methodname");
            }
        }
    }
}

