use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::sync::Mutex;
use std::sync::Arc;
use std::thread;
use std::io::prelude::*;
use std::fmt;
// use super::config;

#[derive(Debug)]
pub struct Reqmsg {
    endname: String,
    servername: String,
    pub methodname: String,
    pub args: Vec<String>,
}

#[derive(Debug)]
pub struct Replymsg {
    pub ok: bool,
    pub reply: Vec<String>,
}

pub struct MsgChannel {
    pub sender: mpsc::Sender<Reqmsg>,
    pub receiver: mpsc::Receiver<Replymsg>,
}

pub struct OwnChannel {
    pub sender: mpsc::Sender<Replymsg>,
    pub receiver: mpsc::Receiver<Reqmsg>,
}

pub trait String_to_arg {
    fn to_arg(&self) ->String;
}

impl<T> String_to_arg for T  
where T: fmt::Display{
    fn to_arg(&self) ->String {
        let arg = format!("{}", self);
        format!("{0:<0width$}{1}", arg.len(), arg, width = 10)
    }
}

impl Reqmsg {
    pub fn print_req(&self) {
        println!("req: {:?}", self);
    }

    pub fn deal_req(&self) -> Replymsg {
        if self.methodname == "Append".to_string() {
            println!("Append:");
            return Replymsg {
                ok: true,
                reply: vec!["Apped finished".to_arg()],
            };
        }
        Replymsg {
            ok: true,
            reply: vec![],
        }
    }
    pub fn string_to_req(text: String, spilt: u8) ->Reqmsg {
        let max_len = text.len();
        let mut dealt = 0;
        let mut args: Vec<String> = Vec::new();
        while dealt < max_len {
            let len: usize = text[dealt..dealt + 10].trim().parse().unwrap();
            println!("{}", text[dealt..dealt + 10].to_string());
            dealt+= 10;
            args.push(text[dealt..dealt + len].to_string());
            dealt += len;
        }
        let methodname = args.pop().unwrap();
        Reqmsg {
            endname: "client".to_string(),
            servername: "service".to_string(),
            methodname,
            args: args
        }
    }
}

impl Replymsg {
    pub fn print_reply(&self){
        println!("reply: {:?}", self);
    }

    pub fn string_to_reply(text: String) ->Replymsg {
        let max_len = text.len();
        let mut dealt = 0;
        let mut reply: Vec<String> = Vec::new();
        while dealt < max_len {
            let len: usize = text[dealt..dealt + 10].trim().parse().unwrap();
            dealt+= 10;
            reply.push(text[dealt..dealt + len].to_string());
            dealt += len;
        }
        let ok = reply.pop().unwrap().parse().unwrap();
        Replymsg {
            ok,
            reply: reply,
        }
    }
}

pub struct ClientEnd {
    endname: String,
    // sender: mpsc::Sender<Reqmsg>,
    // receiver: mpsc::Receiver<Replymsg>,
    // pub listen_thread: thread::JoinHandle<()>
}

pub struct Server {
    servername: String,
    services: Arc<Mutex<ServicePool>>,
    pub listen_thread: thread::JoinHandle<()>
}

fn handle_reply(mut stream: TcpStream) ->Replymsg {
    let mut buffer = [0; 512];
    stream.read(&mut buffer).unwrap();
    let reply = Replymsg::string_to_reply(String::from_utf8_lossy(&buffer[..]).to_string());
    reply.print_reply();
    reply
}


impl ClientEnd {
    pub fn new(endname: String) -> ClientEnd {
        ClientEnd {
            endname,
        }
    }

    pub fn call(&self, servername: String, methodname: String, args: String) ->(bool, Replymsg) {
        println!("Note: Send a req!");
        let mut stream = match TcpStream::connect(servername){
            Ok(stream) => stream,
            Err(_) =>{
                return (false, Replymsg{
                    ok: false,
                    reply: vec!["Connect failed".to_string()],
                });
            },
        };
        let reqmsg = format!("{}{}", args, methodname.to_arg());
        stream.write(reqmsg.as_bytes()).unwrap();
        stream.flush().unwrap();
        let reply = handle_reply(stream);
        (true, reply)
    }
}

impl Server {
    pub fn new(servername: String, port: u32) ->Server{
    //    , sender: mpsc::Sender<Reqmsg>, receiver: mpsc::Receiver<Replymsg>) ->Server {
        let service_pool = Arc::new(Mutex::new(ServicePool::new(4)));
        let services = Arc::clone(&service_pool);
        // let sender = Arc::new(Mutex::new(sender));
        // let receiver = Arc::new(Mutex::new(receiver));
        let listen_thread = thread::spawn(move ||{
            let address = format!("127.0.0.1:{}", port);
            let listener = TcpListener::bind(address).unwrap();
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                // let 'static handle_req(stream, Arc::clone(&sender), Arc::clone(&receiver));
                let mut buffer = [0; 4096];
                let mut reqmsg = String::new();
                let mut size: usize = 4096;
                while size == 4096 {
                    size = stream.read(&mut buffer).unwrap();
                    reqmsg += &String::from_utf8_lossy(&buffer[..size]);
                }
                println!("{}", reqmsg);
                let reqmsg = Reqmsg::string_to_req(reqmsg, 10);
                let services = services.lock().unwrap();
                services.execute(Job {
                    reqmsg,
                    stream,
                });
            }
        });

        Server {
            servername,
            services: service_pool,
            listen_thread,
        }
    }

    pub fn add_service(&mut self, id: usize) ->OwnChannel {
        let (reqsender, reqreceiver) = mpsc::channel();
        let (replysender, replyreceiver) = mpsc::channel(); 
        self.services.lock().unwrap().add_service(id, 
                MsgChannel{sender: reqsender, receiver: replyreceiver});
        OwnChannel {
            sender: replysender,
            receiver: reqreceiver,
        }
    }
}

enum Message {
    NewJob(Job),
    Terminate,
}

pub struct ServicePool {
    services: Vec<Service>,
    sender: mpsc::Sender<Message>,
    receiver: Arc<Mutex<mpsc::Receiver<Message>>>,
}

trait FnBox {
    fn call_box(self: Box<Self>);
}

impl<F: FnOnce()> FnBox for F {
    fn call_box(self: Box<F>) {
        (*self)()
    }
}

// type Job = Box<FnBox + Send + 'static>;
struct Job {
    reqmsg: Reqmsg,
    stream: TcpStream,
}

impl ServicePool {
    fn new(size: usize) -> ServicePool {
        assert!(size > 0);

        let (sender, receiver) = mpsc::channel();

        let receiver = Arc::new(Mutex::new(receiver));
        let services = Vec::new();
        ServicePool {
            services,
            sender,
            receiver,
        }
    }

    fn add_service(&mut self, id: usize, msgchannel: MsgChannel) {
        self.services.push(Service::new(id, Arc::clone(&self.receiver), msgchannel));
    }

    fn execute(&self, job: Job)
    {
        // let job = Box::new(job);

        self.sender.send(Message::NewJob(job)).unwrap();
    }
}

impl Drop for ServicePool {
    fn drop(&mut self) {
        println!("Sending terminate message to all services.");

        for _ in &mut self.services {
            self.sender.send(Message::Terminate).unwrap();
        }

        println!("Shutting down all services.");

        for service in &mut self.services {
            println!("Shutting down service {}", service.id);

            if let Some(thread) = service.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

struct Service {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Service {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>, msgchannel: MsgChannel) ->
        Service {

        let thread = thread::spawn(move ||{
            loop {
                let message = receiver.lock().unwrap().recv().unwrap();

                match message {
                    Message::NewJob(mut job) => {
                        println!("Service {} got a job; executing.", id);
                        msgchannel.sender.send(job.reqmsg).unwrap();
                        let reply = msgchannel.receiver.recv().unwrap();
                        let replymsg = format!("{}{}", reply.reply[0], reply.ok);
                        job.stream.write(replymsg.as_bytes()).unwrap();
                        job.stream.flush().unwrap();
                    },
                    Message::Terminate => {
                        println!("Service {} was told to terminate.", id);

                        break;
                    },
                }
            }
        });

        Service {
            id,
            thread: Some(thread),
        }
    }
}

pub fn test_rpc_server() {
        println!("Test start");
        let mut server = Server::new("server1".to_string(), 8080);
        let mut listens = Vec::new();
        for i in 0..4 {
            let owner = server.add_service(i);
            // let receiver = owner.receiver;
            let listen_thread = thread::spawn(move || {
                loop {
                    let reqmsg = owner.receiver.recv().unwrap();
                    reqmsg.print_req();
                    owner.sender.send(Replymsg {
                        ok: true,
                        reply: vec!["Reply from server".to_arg()],
                    }).unwrap();
                }
            });
            listens.push(listen_thread);
        }
        for listen_thread in listens {
            listen_thread.join().unwrap();
        }
}

pub fn test_rpc_client() {
    let client = ClientEnd::new("Client".to_string());
    for i in 0..10 {
        let arg = "hello world!";
        let args = format!("{0:<0width$}{1}", arg.len(), 
                    arg, width = 10);
        client.call("127.0.0.1:8080".to_string(), "Append".to_string(), args);
    }
}


#[cfg(test)]
mod test {
    use super::*;
}
