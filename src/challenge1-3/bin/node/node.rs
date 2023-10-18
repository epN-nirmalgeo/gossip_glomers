use std::collections::HashSet;

use serde::{Serialize, Deserialize};

use crate::node::message::{Message, MessageType};


#[derive(Serialize, Deserialize, Debug)]
pub struct Node {
    id: String,
    message_ids: Vec<i64>,
    neighboring_node_ids: Vec<String>,
    last_gossip_time: String,
    message_visibility: HashSet<i64>,
    request_replication: bool,
}

impl Node {
    pub fn task_loop() {
        let mut node = Node {
            id: String::new(),
            message_ids: vec![],
            neighboring_node_ids: vec![],
            last_gossip_time: String::new(),
            message_visibility: HashSet::new(),
            request_replication: false,
        };

        loop {
            node.process_messages();
        }
    }

    pub fn process_messages(&mut self) {
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
        let request: Message = serde_json::from_str(&line).unwrap();

        let request_type = Message::get_message_type(&request);

        let response = match request_type {
            MessageType::Init =>  {
                let (response, node_id) = Message::parse_init_message(&request);
                self.id = node_id;
                response
            }

            MessageType::Echo => {
                Message::parse_echo_message(&request)
            }
            
            MessageType::Generate => {
                Message::parse_generate_message(&request)
            }
        
            MessageType::Broadcast => {
                let (response, message_id) = Message::parse_broadcast_message(&request);
                self.message_ids.push(message_id);
                response
            }

            MessageType::Read => {
                Message::parse_read_message(&request, &self.message_ids)
            }

            MessageType::Topology => {
                let (resp, node_ids) = Message::parse_topology_message(&request, &self.id);
                self.neighboring_node_ids = node_ids;
                resp
            }

            // more or less like chain replication
            MessageType::Gossip => {
                let messages = Message::parse_gossip_message(&request);

                for message_id in messages {
                    // message already seen, not required to gossip
                    if self.message_visibility.contains(&message_id) {
                        continue;
                    }

                    self.message_visibility.insert(message_id);
                    self.message_ids.push(message_id);
                    for node_id in &self.neighboring_node_ids {
                        // for now pass all the messages from one node to other ( we can optimize )
                        let resp = Message::generate_gossip_request(&self.id, node_id, &self.message_ids, &request);
                        let gossip_str = serde_json::to_string(&resp).unwrap();
                        println!("{gossip_str}");
                    }
                }

                return;
            }

            MessageType::RequestReplication => {
                Message::parse_request_replication_message(&request, &self.message_ids, &self.id)
            }

            MessageType::InitOk | MessageType::BroadcastOk | 
                MessageType::EchoOk | MessageType::GenerateOk | MessageType::ReadOk | MessageType::TopologyOk => {
                panic!("Oops don't expect TopologyOk as request");
            }
        };
    
        let response_str = serde_json::to_string(&response).unwrap();
        println!("{response_str}");

        match request_type {
            // Gossip only the recent message to the target node if it was a broadcast
            MessageType::Broadcast => {
                for node_id in &self.neighboring_node_ids {
                    let n = self.message_ids.len();
                    let resp = Message::generate_gossip_request(&self.id, node_id, &vec![self.message_ids[n-1]], &request);
                    let gossip_str = serde_json::to_string(&resp).unwrap();
                    println!("{gossip_str}");
                }
            }

            // If topology is populated and if initial replication is not requested ( for recovery cases,) .. request it!
            MessageType::Topology => {
                if !self.request_replication && !self.neighboring_node_ids.is_empty() {
                    for node in &self.neighboring_node_ids {
                        let resp = Message::request_replication_request(&self.id, node);
                        let request_replication = serde_json::to_string(&resp).unwrap();
                        println!("{request_replication}");
                    }
                }
            }
            _ => {
                // nothing to be done here
            }
        }



    }
}