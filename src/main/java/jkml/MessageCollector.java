package jkml;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

class MessageCollector {

	private final ArrayList<ConsumerRecord<String, String>> messages = new ArrayList<>();

	private final ReadWriteLock lock = new ReentrantReadWriteLock();

	int count() {
		return messages.size();
	}

	void add(ConsumerRecords<String, String> messages) {
		lock.writeLock().lock();
		try {
			this.messages.ensureCapacity(this.messages.size() + messages.count());
			for (var m : messages) {
				this.messages.add(m);
			}
		} finally {
			lock.writeLock().unlock();
		}

	}

	void export(Consumer<ConsumerRecord<String, String>> consumer) {
		lock.readLock().lock();
		try {
			for (var m : messages) {
				consumer.accept(m);
			}
		} finally {
			lock.readLock().unlock();
		}
	}

}
