
#[derive(Clone, Serialize. Deserialize, Debug)]
struct Message {
    #[serde(rename = "type")]
    typ: MessageType,
    
}


#[derive(Clone, Serialize, Deserialize, Debug)]
enum MessageType {
    #[serde(rename = "add")]
    Add,
    #[serde(rename = "add_ok")]
    AddOk,
    #[serde(rename = "read")]
    Read,
    #[serde(rename = "read_ok")]
    ReadOk,
}