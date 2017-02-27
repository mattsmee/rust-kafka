
extern crate rdkafka;

error_chain! {
  foreign_links {
    KafkaError(rdkafka::error::KafkaError);
  }
}
