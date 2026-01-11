package jkml;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class TopicUtils {

	private TopicUtils() {
	}

	public static List<TopicPartition> convertPartitions(List<PartitionInfo> partitions) {
		var list = new ArrayList<TopicPartition>(partitions.size());
		partitions.forEach(p -> list.add(new TopicPartition(p.topic(), p.partition())));
		return list;
	}

	public static Map<TopicPartition, Long> createTimestamps(List<TopicPartition> partitions, Instant timestamp) {
		var map = new HashMap<TopicPartition, Long>();
		var ts = timestamp.toEpochMilli();
		partitions.forEach(p -> map.put(p, ts));
		return map;
	}

	public static Map<TopicPartition, OffsetAndMetadata> convertOffsets(Map<TopicPartition, Long> offsets) {
		var map = new HashMap<TopicPartition, OffsetAndMetadata>();
		offsets.forEach((p, o) -> map.put(p, new OffsetAndMetadata(o)));
		return map;
	}

}
