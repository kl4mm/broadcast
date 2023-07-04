use std::{
    collections::HashMap,
    io::{self, Write},
};

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Message {
    src: String,
    dest: String,
    body: Body,
}

#[derive(Deserialize, Serialize)]
pub struct Body {
    #[serde(rename = "type")]
    msg_type: Type,
    #[serde(rename = "msg_id")]
    id: Option<u32>,
    in_reply_to: Option<u32>,

    #[serde(flatten)]
    fields: HashMap<String, serde_json::Value>,
}

#[derive(PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    Init,
    InitOk,
    Broadcast,
    BroadcastOk,
    Read,
    ReadOk,
    Topology,
    TopologyOk,
}

pub fn init() -> io::Result<(String, Vec<String>)> {
    let stdin = io::stdin();

    let mut init = String::new();
    stdin.read_line(&mut init)?;

    let mut init: Message = serde_json::from_str(&init)?;
    assert!(init.body.msg_type == Type::Init);

    let id = init.body.fields["node_id"]
        .as_str()
        .expect("Expected node_id")
        .to_owned();
    let node_ids = init.body.fields["node_ids"]
        .as_array()
        .expect("Expected node_ids")
        .to_vec()
        .iter()
        .map(|v| v.as_str().unwrap().to_owned())
        .collect::<Vec<String>>();

    init.body.msg_type = Type::InitOk;

    reply(id.to_owned(), init)?;

    Ok((id, node_ids))
}

pub fn reply(src: String, mut msg: Message) -> io::Result<()> {
    msg.dest = msg.src;
    msg.body.in_reply_to = msg.body.id;
    msg.src = src;

    let mut msg = serde_json::to_string(&msg).unwrap();
    msg.push('\n');

    let mut stdout = io::stdout();
    stdout.write_all(msg.as_bytes())?;
    stdout.flush()?;

    Ok(())
}

#[derive(Default)]
pub struct Node {
    id: String,
    node_ids: Vec<String>,
    messages: Vec<u64>,
    topology: HashMap<String, Vec<String>>,
}

impl Node {
    pub fn new(id: String, node_ids: Vec<String>) -> Self {
        Self {
            id,
            node_ids,
            ..Default::default()
        }
    }

    pub fn handle_msg(&mut self, mut msg: Message) -> io::Result<()> {
        match msg.body.msg_type {
            Type::Broadcast => {
                msg.body.msg_type = Type::BroadcastOk;
                let value = msg.body.fields["message"]
                    .as_u64()
                    .expect("Expected number value in broadcast message");
                self.messages.push(value);

                msg.body.fields.clear();
                reply(self.id.clone(), msg)?;
            }
            Type::Read => {
                msg.body.msg_type = Type::ReadOk;
                msg.body
                    .fields
                    .insert("messages".into(), self.messages.clone().into());

                reply(self.id.clone(), msg)?;
            }
            Type::Topology => {
                msg.body.msg_type = Type::TopologyOk;
                msg.body.fields.clear();

                reply(self.id.clone(), msg)?;
            }

            Type::Init | Type::InitOk | Type::BroadcastOk | Type::ReadOk | Type::TopologyOk => {
                unreachable!("Unexpected message type")
            }
        }

        Ok(())
    }

    pub fn run(&mut self) -> io::Result<()> {
        let stdin = io::stdin();
        let mut reader = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();

        while let Some(msg) = reader.next() {
            match msg {
                Ok(m) => self.handle_msg(m)?,
                Err(e) => panic!("{e}"),
            }
        }

        Ok(())
    }
}
