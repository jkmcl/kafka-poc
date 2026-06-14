package jkml;

import java.util.ArrayList;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

class MessageCollector {

	private final ArrayList<ConsumerRecord<String, String>> messages = new ArrayList<>();

	int count() {
		return messages.size();
	}

	void add(ConsumerRecords<String, String> messages) {
		synchronized (this.messages) {
			this.messages.ensureCapacity(this.messages.size() + messages.count());
			for (var m : messages) {
				this.messages.add(m);
			}
		}
	}

	void export(Consumer<ConsumerRecord<String, String>> consumer) {
		synchronized (this.messages) {
			for (var m : messages) {
				consumer.accept(m);
			}
		}
	}

}
