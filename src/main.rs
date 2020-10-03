use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};

struct Kafka {
    url: &'static str,
    timeout: Duration,
}

impl Kafka {
    fn list_topics(&self) {
        // creating the consumer
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", self.url)
            .create()
            .expect("Consumer creation failed");

        // read metadata.
        let metadata = consumer.fetch_metadata(None, self.timeout)
            .expect("Failed to fetch metadata");

        // extrac topics.
        let topics = metadata.topics();

        println!("  Topics count: {}", topics.len());
        // let's loop over the topics and print them out.
        for topic in topics {
            println!("\t{}", topic.name());
        }
    }
}

fn main() {
    let kafka_url = "localhost:9092";
    let timeout: Duration = Duration::new(5, 0);

    let kafka = Kafka { url: kafka_url, timeout: timeout };
    // try and list topics.
    kafka.list_topics();
}
