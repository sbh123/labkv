//use super::serde_derive;

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
        kv_note!("PD req: {:?}", reqmsg);
        if reqmsg.methodname == "GetServers".to_string() {
            let replymsg = serde_json::to_string(&self.servers).unwrap();
            return Replymsg {
                ok: true,
                reply: replymsg,
            };
        } else if reqmsg.methodname == "AddServers".to_string() {
            let serverinfo: ServerInfo = serde_json::from_str(&reqmsg.args).unwrap();
            for (_, serverip) in self.servers.iter() {
                rpc_call(serverip.to_string(), "Raft.AddServer".to_string(), reqmsg.args.clone());
            }
            self.servers.insert(serverinfo.serverid, serverinfo.serverip);
            return Replymsg {
                ok: true,
                reply: "Add server OK".to_string(),
            };
        } if reqmsg.methodname == "GetLeader".to_string() {
            let replymsg = serde_json::to_string(&self.leader).unwrap();
            return Replymsg {
                ok: true,
                reply: replymsg,
            };
        } else if reqmsg.methodname == "SetLeader".to_string() {
            let serverinfo: ServerInfo = serde_json::from_str(&reqmsg.args).unwrap();
            self.leader = Some(serverinfo);
            return Replymsg {
                ok: true,
                reply: "Set leader OK".to_string(),
            };
        }
        Replymsg {
            ok: true,
            reply: "Error method for PD".to_string(),
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

pub fn test_pd() {
    let mut pd = PD::new(8060);
    pd.run();
}