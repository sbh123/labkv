use std::net::{TcpListener, TcpStream, SocketAddr};
use std::sync::mpsc;
use std::sync::Mutex;
use std::sync::Arc;
use std::thread;
// use std::time::Duration;
use std::time::{Duration};
use std::io::prelude::*;
use std::fmt;
use std::cmp;
// use super::config;

#[derive(Debug)]
pub struct Reqmsg {
    servername: String,
    pub methodname: String,
    pub args: String,
}

#[derive(Debug)]
pub struct Replymsg {
    pub ok: bool,
    pub reply: String,
}

pub struct MsgChannel {
    pub sender: mpsc::Sender<Reqmsg>,
    pub receiver: mpsc::Receiver<Replymsg>,
}

pub struct OwnChannel {
    pub sender: mpsc::Sender<Replymsg>,
    pub receiver: mpsc::Receiver<Reqmsg>,
}

trait StringToArg {
    fn to_arg(&self) ->String;
}

impl<T> StringToArg for T  
where T: fmt::Display{
    fn to_arg(&self) ->String {
        let arg = format!("{}", self);
        format!("{0:<0width$}{1}", arg.len(), arg, width = 10)
    }
}

impl Reqmsg {
    pub fn print_req(&self) {
        kv_debug!("req: {:?}", self);
    }

    pub fn string_to_req(text: String) ->Reqmsg {
        let mut dealt = 0;
        if text.len() < 10 {
            kv_note!("Recived a Error Req");
            return Reqmsg {
                servername: "None".to_string(),
                methodname: "None".to_string(),
                args: "None".to_string(),
            };
        }
        let len: usize = text[dealt..dealt + 10].trim().parse().unwrap();
        dealt += 10;
        let methodname = text[dealt..dealt + len].to_string();
        dealt += len;
        let methodname: Vec<&str> = methodname.split_terminator('.').collect();
        let servername = methodname[0].to_string();
        let methodname = methodname[1].to_string();
        let len: usize = text[dealt..dealt + 10].trim().parse().unwrap();
        dealt += 10;
        let args = text[dealt..dealt + len].to_string();
        Reqmsg {
            servername,
            methodname,
            args,
        }
    }

    pub fn to_string(&self) ->String {
        let methodname = format!("{}.{}", self.servername, self.methodname);
        format!("{}{}", methodname.to_arg(), self.args.to_arg())
    }
}

impl Replymsg {
    pub fn print_reply(&self){
        kv_info!("reply: {:?}", self);
    }

    pub fn string_to_reply(text: String) ->Replymsg {
        let mut dealt = 0;
        if text.len() < 10 {
            kv_note!("Recived a Error reply");
            return Replymsg {
                ok: false,
                reply: "Reply error".to_string(),
            };
        }
        let len: usize = text[dealt..dealt + 10].trim().parse().unwrap();
        dealt += 10;
        let ok: bool = text[dealt..dealt + len].to_string().parse().unwrap();
        dealt += len;
        let len: usize = text[dealt..dealt + 10].trim().parse().unwrap();
        dealt += 10;
        let reply = text[dealt..dealt + len].to_string();
        Replymsg {
            ok,
            reply,
        }
    }

    pub fn to_string(&self) ->String {
        format!("{}{}", self.ok.to_arg(), self.reply.to_arg())
    }
}

pub struct RpcServer {
    servername: String,
    services: Arc<Mutex<ServicePool>>,
    pub listen_thread: thread::JoinHandle<()>
}

fn handle_reply(mut stream: TcpStream) ->Replymsg {
    let mut len = [0; 10];
    let size = match stream.read(&mut len) {
        Ok(size) => size,
        Err(_) => {
            return Replymsg {
                ok: false,
                reply: "Wrong reply".to_string(),};
            },
    };
    if size < 10 {
        kv_note!("Received a wrong reply");
        return Replymsg {
            ok: false,
            reply: "Wrong reply".to_string(),
        };
    }
    let len: usize = String::from_utf8_lossy(&len[..size]).parse().unwrap();
    kv_debug!("Need to read {} bytes", len);
    let mut buffer = [0; 4096];
    let mut replymsg = String::new();
    let mut read = 0;
    while read < len {
        let size = match stream.read(&mut buffer) {
            Ok(size) => size,
            Err(_) => {
                return Replymsg {
                    ok: false,
                    reply: "Wrong reply".to_string(),};
             },
        };
        kv_debug!("Read {}", String::from_utf8_lossy(&buffer[..size]));
        kv_debug!("Read {} bytes", size);
        replymsg += &String::from_utf8_lossy(&buffer[..size]);
        read += size;
    }
    kv_info!("reply is: {}", replymsg);
    let reply = Replymsg::string_to_reply(replymsg);
    reply.print_reply();
    reply
}

pub fn rpc_call(serverip: String, methodname: String, args: String) ->(bool, Replymsg) {
        let timeout = Duration::from_millis(100);
        let socket: SocketAddr = serverip.parse().unwrap();
        let mut stream = match TcpStream::connect_timeout(&socket, timeout){
            Ok(stream) => stream,
            Err(_) =>{
                return (false, Replymsg{
                    ok: false,
                    reply: "Connect failed".to_string(),
                });
            },
        };
        let reqmsg = format!("{}{}", methodname.to_arg(), args.to_arg());
        let reqmsg = reqmsg.to_arg();
        kv_info!("Call req msg is {}", reqmsg);
        let mut written = 0;
        while written < reqmsg.len() {
            let deadline = cmp::min(reqmsg.len(), written + 4096);
            let size = match stream.write(reqmsg[written..deadline].as_bytes()) {
                Ok(size) => size,
                Err(_) =>{
                    return (false, Replymsg{
                        ok: false,
                        reply: "Connect failed".to_string(),
                    });
                },
            };
            written += size;
            kv_debug!("Write at one time for {} bytes", size);
        }
        match stream.flush(){
            Ok(_) => {},
            Err(err) => {
                kv_note!("{}", err);
            },
        };
        thread::sleep(Duration::from_millis(100));
        let reply = handle_reply(stream);
        (true, reply)
}

impl RpcServer {
    pub fn new(servername: String, port: u16) -> RpcServer{
        let service_pool = Arc::new(Mutex::new(ServicePool::new(4)));
        let services = Arc::clone(&service_pool);
        let listen_thread = thread::spawn(move ||{
            let address = format!("127.0.0.1:{}", port);
            println!("bind {} finished", address);
            let listener = TcpListener::bind(address).unwrap();
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut len = [0; 10];
                let size = match stream.read(&mut len) {
                    Ok(size) => size,
                    Err(_) => {
                        continue;
                    },
                };
                if size < 10 {
                    kv_note!("Received a wrong reqmsg");
                    continue;
                }
                let len: usize = String::from_utf8_lossy(&len[..size]).parse().unwrap();
                kv_debug!("Need to read {} bytes", len);
                let mut buffer = [0; 4096];
                let mut reqmsg = String::new();
                let mut read = 0;
                while read < len {
                    let size = match stream.read(&mut buffer) {
                        Ok(size) => size,
                        Err(err) => {
                            kv_note!("{}", err);
                            break;
                        },
                    };
                    kv_debug!("Read {} bytes", size);
                    reqmsg += &String::from_utf8_lossy(&buffer[..size]);
                    read += size;
                }
                if read < len {
                    continue;
                }
                kv_debug!("Reqmsg is {} bytes", reqmsg.len());
                kv_info!("recived reqmsg is: {}", reqmsg);
                let reqmsg = Reqmsg::string_to_req(reqmsg);
                let services = services.lock().unwrap();
                services.execute(Job {
                    reqmsg,
                    stream,
                });
            }
        });

        RpcServer {
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
        self.sender.send(Message::NewJob(job)).unwrap();
    }
}

impl Drop for ServicePool {
    fn drop(&mut self) {
        kv_info!("Sending terminate message to all services.");

        for _ in &mut self.services {
            self.sender.send(Message::Terminate).unwrap();
        }

        kv_info!("Shutting down all services.");

        for service in &mut self.services {
            kv_info!("Shutting down service {}", service.id);

            if let Some(thread) = service.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

struct Service {
    // id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Service {
    fn new(_id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>, msgchannel: MsgChannel) ->
        Service {

        let thread = thread::spawn(move ||{
            loop {
                let message = receiver.lock().unwrap().recv().unwrap();

                match message {
                    Message::NewJob(mut job) => {
                        kv_info!("Service {} get a job", id);
                        job.reqmsg.print_req();
                        msgchannel.sender.send(job.reqmsg).unwrap();
                        let reply = msgchannel.receiver.recv().unwrap();
                        let replymsg = format!("{}{}", reply.ok.to_arg(), 
                                    reply.reply.to_arg()).to_arg();
                        let mut written = 0;
                        kv_debug!("Write reply is {}", replymsg);
                        while written < replymsg.len() {
                            let deadline = cmp::min(replymsg.len(), written + 4096);
                            let size = match job.stream.write(replymsg[written..deadline].as_bytes()) {
                                Ok(size) => size,
                                Err(err) => {
                                    kv_note!("{}", err);
                                    break;
                                },
                            };
                            written += size;
                            kv_debug!("Write at one time for {} bytes", size);
                        }




                        match job.stream.flush(){
                            Ok(_) => {},
                            Err(err) => {
                                kv_note!("{}", err);
                            },
                        };
                    },
                    Message::Terminate => {
                        kv_info!("Service {} was told to terminate.", id);

                        break;
                    },
                }
            }
        });

        Service {
            // id,
            thread: Some(thread),
        }
    }
}




#[cfg(test)]
mod test {
    //use super::*;
}
