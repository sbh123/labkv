use super::*;

#[derive(Serialize, Deserialize, Debug)]
pub struct PutAppendArgs {
    pub key: String,
    pub value: String,
    pub op: String,
    pub id: Uuid,
    pub seq: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PutAppendReply{
    pub wrong_leader:bool,
    pub err: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetArgs {
    pub key: String,
    pub id: Uuid,
    pub seq: i32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetReply{
    pub wrong_leader: bool,
    pub err: String,
    pub value:String,
}