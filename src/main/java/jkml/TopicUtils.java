package jkml;

import java.util.ArrayList;
import java.util.Comparator;

import org.apache.kafka.common.TopicPartition;

public class TopicUtils {

	private TopicUtils() {
	}

	public static String join(CharSequence delimiter, Iterable<TopicPartition> partitions) {
		var list = new ArrayList<TopicPartition>();
		partitions.forEach(list::add);
		list.sort(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));

		var strings = new ArrayList<String>(list.size());
		list.forEach(p -> strings.add(p.toString()));
		return String.join(delimiter, strings);
	}

}
