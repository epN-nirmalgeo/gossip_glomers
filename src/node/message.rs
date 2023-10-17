use std::collections::HashMap;

use serde::{Serialize, Deserialize};
use serde_json::{Value, json};

use std::time::{SystemTime, UNIX_EPOCH};
use rand::Rng;

#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    src: String,
    dest: String,
    body: MessageBody,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MessageBody {

    #[serde(rename = "type")]
    typ: MessageType,

    #[serde(flatten)]
    message_params: HashMap<String, Value>,
}
#[derive(Serialize, Deserialize, Debug)]
pub enum MessageType {
    #[serde(rename = "init")]
    Init,
    #[serde(rename = "init_ok")]
    InitOk,
    #[serde(rename = "echo")]
    Echo,
    #[serde(rename = "echo_ok")]
    EchoOk,
    #[serde(rename = "generate")]
    Generate,
    #[serde(rename = "generate_ok")]
    GenerateOk,
    #[serde(rename = "broadcast")]
    Broadcast,
    #[serde(rename = "broadcast_ok")]
    BroadcastOk,
    #[serde(rename = "read")]
    Read,
    #[serde(rename = "read_ok")]
    ReadOk,
    #[serde(rename = "topology")]
    Topology,
    #[serde(rename = "topology_ok")]
    TopologyOk,
    #[serde(rename = "gossip")]
    Gossip,
}


impl Message {

    pub fn get_message_type(request: &Message) -> &MessageType {
        &request.body.typ
    }

    fn generate_response(request: &Message, response_type: MessageType) -> Message {
        let mut params = HashMap::new();
        let mut response = Message {
            src: request.dest.to_owned(),
            dest: request.src.to_owned(),
            body: MessageBody { typ: response_type, message_params: HashMap::new()}
        };

        params.insert("in_reply_to".to_owned(), request.body.message_params.get("msg_id").unwrap().clone().to_owned());
        // for now insert a dummy entry as msg_id, maybe snowflake algorithm here?
        params.insert("msg_id".to_owned(), json!(1));
        response.body.message_params = params;
        
        response
    }

    pub fn parse_init_message(request: &Message) -> (Message, String) {
        let mut node_id = String::new();
        if let Some(val) = request.body.message_params.get("node_id") {
            // required to remove quotes like "n0" to n0  
            node_id = val.as_str().unwrap().to_owned();
        }

        (Self::generate_response(request, MessageType::InitOk), node_id)
    }

    pub fn parse_echo_message(request: &Message) -> Message {
        let mut resp = Self::generate_response(request, MessageType::EchoOk);
        if let Some(val) = request.body.message_params.get("echo") {
            resp.body.message_params.insert("echo".to_owned(), json!(val));
        }

        resp
    }

    pub fn parse_generate_message(request: &Message) -> Message {
        let mut resp = Self::generate_response(request, MessageType::GenerateOk);
        let time_since = SystemTime::now()
        .duration_since(UNIX_EPOCH).expect("That's odd").as_millis();
        let random = rand::thread_rng().gen_range(100000..200000);
        let random_number = format!("{time_since}-{random}");
        resp.body.message_params.insert("id".to_owned(), json!(random_number));
        resp
    }

    pub fn parse_broadcast_message(request: &Message) -> (Message, i64) {
        let mut message_id: i64 = 0;
        let resp = Self::generate_response(request, MessageType::BroadcastOk);
        if let Some(val) = request.body.message_params.get("message") {
            message_id = val.as_i64().unwrap();
        }
        (resp, message_id)
    }

    pub fn parse_read_message(request: &Message, message_ids: &Vec<i64>) -> Message {
        let mut resp = Self::generate_response(request, MessageType::ReadOk);
        resp.body.message_params.insert("messages".to_owned(), json!(message_ids));
        resp
    }

    pub fn parse_topology_message(request: &Message, current_node_id: &String) -> (Message, Vec<String>) {
        let mut all_nodes: Vec<String> = vec![];
        if current_node_id.is_empty() {
            panic!("Expected current node id to be populated by now");
        }

        if let Some(val) = request.body.message_params.get("topology") {
            eprintln!("Val {:?} current_node_id {current_node_id}", val);
            eprintln!("{:?}", val[current_node_id.as_str()]);
            if let Some(nodes) = val.get(current_node_id.as_str()).unwrap().as_array() {
                eprintln!("nodes {:?}", nodes);
                for node in nodes {
                    all_nodes.push(node.as_str().unwrap().to_owned());
                }
            }
        }
        
        (Self::generate_response(request, MessageType::TopologyOk), all_nodes)
    }

    pub fn parse_gossip_message(request: &Message) -> i64 {
        let mut message_id: i64 = 0;
        if let Some(val) = request.body.message_params.get("message") {
            message_id = val.as_i64().unwrap();
        }
        message_id 
    }

    pub fn generate_gossip_request(current_node_id: &String, target_node_id: &String, message_id: i64, request: &Message) -> Message {
        let mut resp = Message { 
            src: current_node_id.to_owned(), 
            dest: target_node_id.to_owned(), 
            body: MessageBody { 
                typ: MessageType::Gossip, 
                message_params: HashMap::new(),
            } 
        };

        let mut params = HashMap::new();
        params.insert("message".to_owned(), json!(message_id));
        params.insert("in_reply_to".to_owned(), request.body.message_params.get("msg_id").unwrap().clone().to_owned());
        // for now insert a dummy entry as msg_id, maybe snowflake algorithm here?
        params.insert("msg_id".to_owned(), json!(1));
        resp.body.message_params = params;

        eprintln!("Gossip message {:?}", resp);
        resp

    }
}