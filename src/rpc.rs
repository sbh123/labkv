use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::sync::Mutex;
use std::sync::Arc;
use std::thread;
use std::io::prelude::*;
use std::collections::HashMap;
// use super::config;

#[derive(Debug)]
pub struct Reqmsg {
    endname: String,
    servicename: String,
    methodname: String,
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

impl Reqmsg {
    pub fn print_req(&self) {
        println!("req: {:?}", self);
    }

    pub fn deal_req(&self) -> Replymsg {
        if self.methodname == "Append".to_string() {
            println!("Append:");
            return Replymsg {
                ok: true,
                reply: vec!["Apped finished".to_string()],
            };
        }
        Replymsg {
            ok: true,
            reply: vec![],
        }
    }
    pub fn string_to_req(text: String, spilt: u8) ->Reqmsg {
        let mut args: Vec<String> = Vec::new();
        let mut next  = 0;
        let mut pre  = 0;
        let mut servicename = String::new();
        let mut methodname = String::new();

        for b in text.bytes() {
            next += 1;
            if  b == spilt {
                args.push(text[pre..next-1].to_string());
                pre = next;
            } else if b == 0{
                next -= 1;
                break;
            }
        }
        let mut i = 0;
        for  b in text[pre..next].bytes(){
            i += 1;
            if b == 46 {  // b = '.'
                servicename = text[pre..pre + i-1].to_string();
                methodname = text[pre + i..next].to_string();
                break;
            }

        }
        Reqmsg {
            endname: "client".to_string(),
            servicename,
            methodname,
            args: args
        }
    }
}

impl Replymsg {
    pub fn print_reply(&self){
        println!("reply: {:?}", self);
    }

    pub fn string_to_reply(text: String, spilt: u8) ->Replymsg {
        let mut reply: Vec<String> = Vec::new();
        let mut next  = 0;
        let mut pre  = 0;
        for b in text.bytes() {
            next += 1;
            if  b == spilt {
                reply.push(text[pre..next -1].to_string());
                pre = next;
            } else if b == 0 {
                break;
            } 
        }
        Replymsg {
            ok: text[pre..next -1].to_string().parse().unwrap(),
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
    let reply = Replymsg::string_to_reply(String::from_utf8_lossy(&buffer[..]).to_string(), 10);
    reply.print_reply();
    reply
}

// fn handle_req(mut stream: TcpStream, sender: Arc<Mutex<mpsc::Sender<Reqmsg>>>, 
//     receiver: Arc<Mutex<mpsc::Receiver<Replymsg>>>) {
//     let mut buffer = [0; 512];
//     stream.read(&mut buffer).unwrap();
//     let req = Reqmsg::string_to_req(String::from_utf8_lossy(&buffer[..]).to_string(), 10);
//     req.print_req();
//     let sender = sender.lock().unwrap();
//     sender.send(req).unwrap();
//     let receiver = receiver.lock().unwrap();
//     let reply = receiver.recv().unwrap();
    
//     // let reply = req.deal_req();
//     let replymsg = format!("{}\n{}", reply.reply, reply.ok);
//     stream.write(replymsg.as_bytes()).unwrap();
//     stream.flush().unwrap();
// }

// fn get_req(mut stream: TcpStream) -> Reqmsg {
//     let mut buffer = [0; 512];
//     stream.read(&mut buffer).unwrap();
//     Reqmsg::string_to_req(String::from_utf8_lossy(&buffer[..]).to_string(), 10)  
// }

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
        let reqmsg = format!("{}\n{}", args, methodname);
        stream.write(reqmsg.as_bytes()).unwrap();
        stream.flush().unwrap();
        let reply = handle_reply(stream);
        (true, reply)
    }
}

impl Server {
    pub fn new(servername: String) ->Server{
    //    , sender: mpsc::Sender<Reqmsg>, receiver: mpsc::Receiver<Replymsg>) ->Server {
        let service_pool = Arc::new(Mutex::new(ServicePool::new(4)));
        let services = Arc::clone(&service_pool);
        // let sender = Arc::new(Mutex::new(sender));
        // let receiver = Arc::new(Mutex::new(receiver));
        let listen_thread = thread::spawn(move ||{
            let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                // let 'static handle_req(stream, Arc::clone(&sender), Arc::clone(&receiver));
                let mut buffer = [0; 512];
                stream.read(&mut buffer).unwrap();
                let reqmsg = Reqmsg::string_to_req(String::from_utf8_lossy(&buffer[..]).to_string(), 10);
                let services = services.lock().unwrap();
                services.execute(reqmsg.servicename.clone(), Job {
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

    pub fn add_service(&mut self, name: String) ->OwnChannel {
        let (reqsender, reqreceiver) = mpsc::channel();
        let (replysender, replyreceiver) = mpsc::channel(); 
        self.services.lock().unwrap().add_service(name, 
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
    senders: HashMap<String, mpsc::Sender<Message>>,
    // sender: mpsc::Sender<Message>,
    // receiver: Arc<Mutex<mpsc::Receiver<Message>>>,
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

        // let receiver = Arc::new(Mutex::new(receiver));
        let services = Vec::new();

        // let mut services = Vec::with_capacity(size);

        // for id in 0..size {
        //     services.push(Service::new(id, Arc::clone(&receiver)));
        // }

        ServicePool {
            services,
            senders: HashMap::new(),
        }
    }

    fn add_service(&mut self, name: String, msgchannel: MsgChannel) {
        let (sender, receiver) = mpsc::channel();
        self.senders.insert(name.clone(), sender);
        self.services.push(Service::new(name.clone(), Arc::new(Mutex::new(receiver)), msgchannel));
    }

    fn execute(&self, servicename:String, mut job: Job)
    {
        // let job = Box::new(job);
        let sender = self.senders.get(&servicename);
        let sender = match sender{
            Some(val) => val,
            None => {
                let replymsg = format!("{}\n{}", "No service", false);
                job.stream.write(replymsg.as_bytes()).unwrap();
                job.stream.flush().unwrap();
                return ;
            }
        };
        sender.send(Message::NewJob(job)).unwrap();
    }
}

impl Drop for ServicePool {
    fn drop(&mut self) {
        println!("Sending terminate message to all services.");

        for (key, sender) in &self.senders {
            sender.send(Message::Terminate).unwrap();
        }

        println!("Shutting down all services.");

        for service in &mut self.services {
            println!("Shutting down service {}", service.name);

            if let Some(thread) = service.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

struct Service {
    name: String,
    thread: Option<thread::JoinHandle<()>>,
}

impl Service {
    fn new(name: String, receiver: Arc<Mutex<mpsc::Receiver<Message>>>, msgchannel: MsgChannel) ->
        Service {

        let thread = thread::spawn(move ||{
            loop {
                let message = receiver.lock().unwrap().recv().unwrap();

                match message {
                    Message::NewJob(mut job) => {
                        // println!("Service {} got a job; executing.", name);
                        msgchannel.sender.send(job.reqmsg).unwrap();
                        let reply = msgchannel.receiver.recv().unwrap();
                        let replymsg = format!("{}\n{}", reply.reply[0], reply.ok);
                        job.stream.write(replymsg.as_bytes()).unwrap();
                        job.stream.flush().unwrap();

        // job.call_box();
                    },
                    Message::Terminate => {
                        // println!("Service {} was told to terminate.", name);
                        break;
                    },
                }
            }
        });

        Service {
            name,
            thread: Some(thread),
        }
    }
}

pub fn test_rpc() {
        println!("Test start");
        let mut server = Server::new("server1".to_string());
        let mut listens = Vec::new();
        let owner = server.add_service("Main".to_string());
            // let receiver = owner.receiver;
            let listen_thread = thread::spawn(move || {
                loop {
                    let reqmsg = owner.receiver.recv().unwrap();
                    reqmsg.print_req();
                    owner.sender.send(Replymsg {
                        ok: true,
                        reply: vec!["Reply from server".to_string()],
                    }).unwrap();
                }
            });
            listens.push(listen_thread);
        for listen_thread in listens {
            listen_thread.join().unwrap();
        }
        
        // {
        //     client.call("127.0.0.1:8082".to_string(), "Append".to_string(),"hello world".to_string());
        // }
        // server.listen_thread.join().unwrap();
        // server.wait_stop();
        // client.wait_stop();
        //thread::sleep(Duration::from_secs(100));
        // cline
}

pub fn test_reqmsg() {
    let reqmsg = format!("{}\n{}.{}", 78, "Raft", "Vote");
    let reqmsg = Reqmsg::string_to_req(reqmsg, 10);
    reqmsg.print_req();

}

#[cfg(test)]
mod test {
    use super::*;
}
