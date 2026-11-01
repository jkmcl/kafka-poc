package jkml;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

class MessageCollector<K, V> implements java.util.function.Consumer<ConsumerRecords<K, V>> {

	private final Lock instanceLock = new ReentrantLock();

	private final ArrayList<ConsumerRecord<K, V>> messages; // NOPMD: Use ArrayList for optimization

	MessageCollector(ArrayList<ConsumerRecord<K, V>> messages) { // NOPMD: Use ArrayList for optimization
		this.messages = messages;
	}

	@Override
	public void accept(ConsumerRecords<K, V> messages) {
		instanceLock.lock();
		try {
			this.messages.ensureCapacity(this.messages.size() + messages.count());
			for (var m : messages) {
				this.messages.add(m);
			}
		} finally {
			instanceLock.unlock();
		}
	}

}
