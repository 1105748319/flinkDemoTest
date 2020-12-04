package com.flink;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class ObjSerializationSchema implements KafkaSerializationSchema<Tuple2<String, Float>> {

	private String topic;
	private ObjectMapper mapper;

	public ObjSerializationSchema(String topic) {
		super();
		this.topic = topic;
	}

	@Override
	public ProducerRecord<byte[], byte[]> serialize(
		Tuple2<String, Float> stringIntegerTuple2,
		@Nullable Long timestamp) {
		byte[] b = null;
		if (mapper == null) {
			mapper = new ObjectMapper();
		}
		try {
			b = mapper.writeValueAsBytes(stringIntegerTuple2);
		} catch (JsonProcessingException e) {
			// 注意，在生产环境这是个非常危险的操作，
			// 过多的错误打印会严重影响系统性能，请根据生产环境情况做调整
			e.printStackTrace();
			System.out.println("================"+e);
		}
		return new ProducerRecord<byte[], byte[]>(topic, b);
	}
}
