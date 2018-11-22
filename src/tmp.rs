use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::sync::Mutex;
use std::sync::Arc;
use std::thread;
use std::collections::HashMap;
use std::rc::Rc;
use std::cell::RefCell;
use std::io::prelude::*;

#[derive(Debug)]
pub struct reqMsg {
    endname: String,
    // svcMetch: String,
    servicename: String,
    methodname: String,
    args: Vec<String>,
}

#[derive(Debug)]
pub  struct replyMsg {
    ok: bool,
    reply: String,
}

impl reqMsg {
    pub fn print_req(&self) {
        println!("req: {:?}", self);
    }

    pub fn deal_req(&self) -> replyMsg {
        if self.methodname == "Append".to_string() {
            println!("Append:");
            return replyMsg {
                ok: true,
                reply: "Apped finished".to_string(),
            };
        }
        replyMsg {
            ok: true,
            reply: "".to_string(),
        }
    }
    pub fn string_to_req(text: String, spilt: u8) ->reqMsg {
        let mut args: Vec<String> = Vec::new();
        let mut next  = 0;
        let mut pre  = 0;
        for b in text.bytes() {
            next += 1;
            if  b == spilt {
                args.push(text[pre..next].to_string());
                pre = next;
            }
        }
        reqMsg {
            endname: "client".to_string(),
            servicename: "service".to_string(),
            methodname: text[pre..].to_string(),
            args: args
        }
    }
}

impl replyMsg {
    pub fn print_reply(&self){
        println!("reply: {:?}", self);
    }

    pub fn string_to_reply(text: String, spilt: u8) ->replyMsg {
        let mut args: Vec<String> = Vec::new();
        let mut next  = 0;
        let mut pre  = 0;
        for b in text.bytes() {
            next += 1;
            if  b == spilt {
                pre = next;
            } else if b == 0 {
                break;
            } 
        }
        replyMsg {
            ok: text[pre..next -1].to_string().parse().unwrap(),
            reply: text[..pre-1].to_string(),
        }
    }
}

pub struct ClientEnd {
    endname: String,
    sender: mpsc::Sender<reqMsg>,
    receiver: mpsc::Receiver<replyMsg>,
    pub listen_thread: thread::JoinHandle<()>
}

pub struct NetWork {
    mux_lock: Mutex<i32>,
    reliable: bool,
    ends: HashMap<String, Arc<Mutex<ClientEnd>>>,
    enabled: HashMap<String, bool>,
    servers: HashMap<String, Arc<Mutex<Server>>>,
    connects: HashMap<String, String>,
    sender: mpsc::Sender<replyMsg>,
//    receiver: mpsc::Receiver<reqMsg>,
    pub listen_thread: thread::JoinHandle<()>
}

pub struct Server {
    servername: String,
    mux_lock: Mutex<i32>,
    services: HashMap<String, Arc<Mutex<Service>>>,
    listen_thread: thread::JoinHandle<()>
}

pub struct Service {
    name: String,

}

fn handle_reply(mut stream: TcpStream) {
    let mut buffer = [0; 512];
    stream.read(&mut buffer).unwrap();
    // println!("Reply: {}", String::from_utf8_lossy(&buffer[..]));
    let reply = replyMsg::string_to_reply(String::from_utf8_lossy(&buffer[..]).to_string(), 10);
    reply.print_reply();
}

fn handle_req(mut stream: TcpStream) {
    let mut buffer = [0; 512];
    stream.read(&mut buffer).unwrap();
    let req = reqMsg::string_to_req(String::from_utf8_lossy(&buffer[..]).to_string(), 10);
    // println!("Req: {}", String::from_utf8_lossy(&buffer[..]));
    req.print_req();
    let reply = req.deal_req();
    let replymsg = format!("{}\n{}", reply.reply, reply.ok);
    stream.write(replymsg.as_bytes()).unwrap();
    stream.flush().unwrap();
}


impl ClientEnd {
    pub fn new(endname: String, sender: mpsc::Sender<reqMsg>, 
               receiver: mpsc::Receiver<replyMsg>) -> ClientEnd {
        let listen_thread = thread::spawn(move ||{
            let listener = TcpListener::bind("127.0.0.1:8081").unwrap();
            for stream in listener.incoming() {
                let stream = stream.unwrap();
                handle_reply(stream);
            }
        });
        ClientEnd {
            endname,
            sender,
            receiver,
            listen_thread,
        }
    }

    pub fn call(&self, servicename: String, methodname: String, args: String) ->bool {
        println!("Note: Send a req!");
        let mut stream = TcpStream::connect(servicename).unwrap();
        let reqmsg = format!("{}\n{}", args, methodname);
        stream.write(reqmsg.as_bytes()).unwrap();
        stream.flush().unwrap();
        handle_reply(stream);
        true
    }

    pub fn wait_stop(&self) {
        // self.listen_thread.join().unwrap();
    }
}

impl NetWork {
    pub fn new(sender: mpsc::Sender<replyMsg>,receiver: Arc<Mutex<mpsc::Receiver<reqMsg>>>)
                ->NetWork {
        let thread = thread::spawn(move || {   });
        let network = NetWork {
            mux_lock: Mutex::new(0),
            reliable: true,
            ends: HashMap::new(),
            enabled: HashMap::new(),
            servers: HashMap::new(),
            connects: HashMap::new(),
            sender,
            //receiver,
            listen_thread: thread,
        };
        return network;
    }

    pub fn reliable(&mut self, yes: bool) {
        let _ = self.mux_lock.lock().unwrap();
        self.reliable = yes;
    }

    pub fn add_server(&mut self, servername: String, rs: Arc<Mutex<Server>>) {
        let _ = self.mux_lock.lock().unwrap();
        self.servers.insert(servername, rs);
    }

    pub fn delete_server(&mut self, servername: String) {
        let _ = self.mux_lock.lock().unwrap();
        self.servers.remove(&servername);
    }

    pub fn add_client(&mut self, endname: String, client: Arc<Mutex<ClientEnd>>) {
        let _ = self.mux_lock.lock().unwrap();
        self.ends.insert(endname.clone(), client);
        self.enabled.insert(endname.clone(), false);
    }

    pub fn delete_client(&mut self, endname: String) {
        let _ = self.mux_lock.lock().unwrap();
        self.ends.remove(&endname);
    }

    pub fn connect(&mut self, endname: String, servername: String) {
        let _ = self.mux_lock.lock().unwrap();
        self.connects.insert(endname, servername);

    }

    pub fn enable(&mut self, endname: String, enable: bool) {
        let _ = self.mux_lock.lock().unwrap();
        self.enabled.insert(endname, enable);
    }

    pub fn is_server_dead(&self, endname: String, servername: String) -> bool {
        let _ = self.mux_lock.lock().unwrap();
        if self.enabled[&endname] == false {
            return true;
        }
        false
    }

    pub fn listen_client(&self) {

    }

    pub fn send(&self, reply: replyMsg) {
        self.sender.send(reply).unwrap();
    }
    pub fn processreq(&self, req: reqMsg) ->replyMsg {
        let servername = self.connects.get(&req.endname);
        let enable = self.enabled.get(&req.endname);

        for (key, value) in &self.connects {
            println!("connect: {}:{}", key, value);
        }
        for (key, value) in &self.enabled {
            println!("enable: {}:{}", key, value);
        }       
        match servername {
            Some(sname) => {
                match enable {
                    Some(able) =>{
                        if *able == true {
                            match self.servers.get(sname) {
                                Some(server) =>{
                                    let server = server.lock().unwrap();
                                    return server.dispatch(req);
                                },
                                None => (),
                            }
                        }
                    },
                    None => (), 
                }
            },
            None => (),
        }
        replyMsg {
            ok: false,
            reply: "".to_string(),
        }
    }
}

impl Server {
    pub fn new(servername: String) ->Server {
        let listen_thread = thread::spawn(move ||{
            let listener = TcpListener::bind("127.0.0.1:8083").unwrap();
            for stream in listener.incoming() {
                let stream = stream.unwrap();
                handle_req(stream);
            }
        });

        Server {
            servername,
            mux_lock: Mutex::new(0),
            services: HashMap::new(),
            listen_thread,
        }
    }

    pub fn add_service(&mut self, name: String, service: Arc<Mutex<Service>>) {
        let _ = self.mux_lock.lock().unwrap();
        self.services.insert(name, service);
    }

    pub fn wait_stop(&self) {
        // let listen_thread = self.listen_thread;
        // listen_thread.join().unwrap();
    }

    pub fn dispatch(&self, req: reqMsg) ->replyMsg {
        let _ = self.mux_lock.lock().unwrap();
        println!("Note: Call server dispatch!");
        let service = self.services.get(&req.servicename);
        match service {
            Some(val) => {
                let service = val.lock().unwrap();
                return  service.dispatch(req.methodname.clone(), req);
            },
            None => {
                return replyMsg {
                    ok: false,
                    reply: "".to_string(),
                }
            },
        }
    }
}

impl Service {
    pub fn new(name: String) -> Service {
        Service{
            name,
        }
    }
    pub fn dispatch(&self, methodname: String, req: reqMsg) ->replyMsg {
        replyMsg {
            ok: true,
            reply: "from sevices".to_string(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    fn test_tcp_connect() {
        println!("Test start");
        let (reqsender, reqreceiver) = mpsc::channel();
        let (repsender, repreceiver) = mpsc::channel();

        let mut net_work = NetWork::new(repsender, reqreceiver);
        let client = Rc::new(RefCell::new(ClientEnd::new("clinet1".to_string(),
                                 reqsender, repreceiver)));
        let server = Rc::new(RefCell::new(Server::new("server1".to_string())));
        let service =Rc::new(RefCell::new(Service::new("service1".to_string())));
        
        net_work.add_server("server1".to_string(), Rc::clone(&server));
        net_work.add_client("clinet1".to_string(), Rc::clone(&client));
        net_work.connect("clinet1".to_string(), "server1".to_string());
        server.borrow_mut().add_service("service1".to_string(), Rc::clone(&service));
        client.borrow().call("service1".to_string(), "Append".to_string(), "".to_string());
    }
}
