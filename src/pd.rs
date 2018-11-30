use super::serde_derive;

extern crate serde;
extern crate serde_json;

use super::rpc::*;
use std::collections::HashMap;
use std::thread;

#[derive(Serialize, Deserialize, Debug)]
pub struct ServerInfo {
    pub serverid: String,
    pub serverip: String,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct ServerInfos {
    pub leader: Option<ServerInfo>,
    pub servers: HashMap<String, String>,
}
pub struct PD {
    rpcserver: RpcServer,
    pub worker_thread: Option<thread::JoinHandle<()>>,
}

impl ServerInfos {
    fn handle_req(&mut self, reqmsg: Reqmsg) -> Replymsg {
        if reqmsg.methodname == "GetServers".to_string() {
            let replymsg = serde_json::to_string(&self.servers).unwrap();
            return Replymsg {
                ok: true,
                reply: vec![replymsg.to_arg()],
            };
        } else if reqmsg.methodname == "AddServers".to_string() {
            let serverinfo: ServerInfo = serde_json::from_str(&reqmsg.args[0]).unwrap();
            let client = ClientEnd::new("PdClient".to_string());
            for (_, serverip) in self.servers.iter() {
                client.call(serverip.to_string(), "Raft.AddServer".to_string(), reqmsg.args[0].to_arg());
            }
            self.servers.insert(serverinfo.serverid, serverinfo.serverip);
            return Replymsg {
                ok: true,
                reply: vec!["Add server OK".to_arg()],
            };
        } if reqmsg.methodname == "GetLeader".to_string() {
            let replymsg = serde_json::to_string(&self.leader).unwrap();
            return Replymsg {
                ok: true,
                reply: vec![replymsg.to_arg()],
            };
        } else if reqmsg.methodname == "SetLeader".to_string() {
            let serverinfo: ServerInfo = serde_json::from_str(&reqmsg.args[0]).unwrap();
            self.leader = Some(serverinfo);
            return Replymsg {
                ok: true,
                reply: vec!["Set leader OK".to_arg()],
            };
        }
        Replymsg {
            ok: true,
            reply: vec!["Error method for PD".to_arg()],
        }
    } 
}

impl PD {
    pub fn new(rpcport: u16) ->PD {
        let rpcserver = RpcServer::new("Pd".to_string(), rpcport);
        PD {
            rpcserver,
            worker_thread: None,
        }
    }
    pub fn run(&mut self) {
        let own = self.rpcserver.add_service(0);
        let mut serverinfos = ServerInfos {
            leader: None,
            servers: HashMap::new(),
        };
        let thread = thread::spawn(move || loop {
            let reqmsg = own.receiver.recv().unwrap();
            let replymsg = serverinfos.handle_req(reqmsg);
            own.sender.send(replymsg).unwrap();
        });
        self.worker_thread = Some(thread);
    }
}