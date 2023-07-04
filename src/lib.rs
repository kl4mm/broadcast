use std::{
    collections::{HashMap, HashSet},
    io::{self, Write},
};

use rand::Rng;
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
        .collect();

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

pub fn send(msg: Message) -> io::Result<()> {
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
    messages: HashSet<u64>,
    topology: HashMap<String, Vec<String>>,

    // Eager probabalistic broadcast:
    k: u8,
    round: u8,
}

impl Node {
    pub fn new(id: String, node_ids: Vec<String>, k: u8) -> Self {
        Self {
            id,
            node_ids,
            k,
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
                self.messages.insert(value);

                msg.body.fields.clear();
                reply(self.id.clone(), msg)?;

                self.gossip(value)?;
            }
            Type::Read => {
                msg.body.msg_type = Type::ReadOk;
                msg.body.fields.insert(
                    "messages".into(),
                    self.messages
                        .clone()
                        .into_iter()
                        .collect::<Vec<u64>>()
                        .into(),
                );

                reply(self.id.clone(), msg)?;
            }
            Type::Topology => {
                msg.body.msg_type = Type::TopologyOk;
                msg.body.fields.clear();

                reply(self.id.clone(), msg)?;
            }

            Type::BroadcastOk => {}

            Type::Init | Type::InitOk | Type::ReadOk | Type::TopologyOk => {
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

    // Eager probabalistic broadcast:
    fn pick_targets(&self, k: u8) -> Vec<String> {
        let mut targets = Vec::new();

        for _ in 0..k {
            let n = self.node_ids.len();

            let mut rng = rand::thread_rng();
            let i: usize = rng.gen_range(0..10) % n;

            let mut candidate = self.node_ids[i % n].clone();
            if candidate == self.id {
                candidate = self.node_ids[(1 + i) % n].clone();
            }

            if !targets.contains(&candidate) {
                targets.push(candidate);
            }
        }

        targets
    }

    fn gossip(&self, value: u64) -> io::Result<()> {
        for id in self.pick_targets(self.k) {
            let message = Message {
                src: self.id.clone(),
                dest: id,
                body: Body {
                    msg_type: Type::Broadcast,
                    id: None,
                    in_reply_to: None,
                    fields: [
                        (String::from("message"), value.into()),
                        // (String::from("round"), r.into()),
                    ]
                    .into(),
                },
            };

            send(message)?;
        }

        Ok(())
    }
}
