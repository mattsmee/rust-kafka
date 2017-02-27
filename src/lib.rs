#[macro_use]
extern crate error_chain;
extern crate rdkafka;
extern crate futures;

mod errors;

use errors::*;
use futures::stream::Stream;
use rdkafka::client::{Context};
use rdkafka::consumer::{Consumer as _Consumer, ConsumerContext, Rebalance, CommitMode};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::config::{ClientConfig, TopicConfig};


struct ConsumerLoggingContext;


pub struct Consumer {
  conn : StreamConsumer<ConsumerLoggingContext>,
}


#[derive(Debug)]
pub struct Message {
  pub payload : Option<String>,
  raw : rdkafka::message::Message,
}


#[derive(Default)]
pub struct ConsumerBuilder {
  pub brokers   : String,
  pub topic     : String,
  pub group_id  : String,
}


type LoggingConsumer = StreamConsumer<ConsumerLoggingContext>;


fn get_consumer(group:String, topic:String, brokers:String) -> Result<StreamConsumer<ConsumerLoggingContext>> {

  let context = ConsumerLoggingContext{};

  let mut consumer = ClientConfig::new()
    .set("group.id", group.as_str())
    .set("bootstrap.servers", brokers.as_str())
    .set("enable.partition.eof", "true")
    .set("session.timeout.ms", "6000")
    .set("enable.auto.commit", "false")
    .set_default_topic_config(TopicConfig::new()
        .set("auto.offset.reset", "smallest")
        .finalize())
    .create_with_context::<_, LoggingConsumer>(context)?;

  consumer.subscribe(&vec![topic.as_str()])?;

  Ok(consumer)
}


impl Context for ConsumerLoggingContext {}


impl ConsumerContext for ConsumerLoggingContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        println!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        println!("Post rebalance {:?}", rebalance);
    }
}


impl Consumer {

  fn new(brokers: String, topic: String, group_id: String) -> Consumer {
    let kafka_con = match get_consumer(group_id, topic, brokers) {
      Ok(con) => con,
      Err(_) => panic!("Failed to create Kafka consumer."),
    };
    println!("kafka_con {:?}", kafka_con.assignment());
    Consumer { conn : kafka_con}
  }

  pub fn start_streaming<F>(&mut self, handler: &F)
    where F: Fn(&Self, Message) {

    let stream = self.conn.start().wait();

    for message in stream {
      match message {
        Ok(Ok(m)) => {
          handler(&self, Message::from(m));
        },
        Ok(Err(_)) => {
          // Stream EOF
        },
        Err(_) => {
          // Error while reading from kafka stream
        },
      }
    };
  }

  pub fn stop_stream(&mut self) {
    self.conn.stop();
  }

  #[allow(unused_must_use)]
  pub fn commit(&self, message: Message) {
    self.conn.commit_message(&message.raw, CommitMode::Async);
  }

}


impl Message {

  fn from(message: rdkafka::message::Message) -> Message {
    let payload = match message.payload_view::<str>() {
      Some(Ok(s)) => Some(s.to_string()),
      Some(Err(e)) => {
        println!("Error while deserializing message payload: {:?}", e);
        None
      },
      None => None,
    };
    Message {
      payload : payload,
      raw     : message,
    }
  }

}


impl ConsumerBuilder {

  pub fn new() -> ConsumerBuilder {
    ConsumerBuilder { ..Default::default() }
  }

  pub fn set_brokers(mut self, brokers: String) -> ConsumerBuilder {
    self.brokers = brokers;
    self
  }

  pub fn set_topic(mut self, topic: String) -> ConsumerBuilder {
    self.topic = topic;
    self
  }

  pub fn set_group(mut self, group_id: String) -> ConsumerBuilder {
    self.group_id = group_id;
    self
  }

  pub fn finalize(self) -> Consumer {
    Consumer::new(self.brokers, self.topic, self.group_id)
  }

}
