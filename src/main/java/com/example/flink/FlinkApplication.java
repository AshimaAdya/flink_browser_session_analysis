package com.example.flink;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkApplication {
	public static final String INPUT_TOPIC = "transactions";

	public static void main(String[] args) throws Exception
		// Create a Flink execution environment

	{
		final KafkaSource<String> source = buildSource();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Set up Redpanda as a data source
		DataStream<String> transactionStream = env
				.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSource");

		 //Process the transaction data stream
        DataStream<FraudResult> fraudDetectionResults = transactionStream.addSink(KafkaSink);

        // Output the fraud detection results to a sink
        fraudDetectionResults.print().name("FraudDetectionSink");

		// Execute the Flink job
		env.execute("Fraud Detection Job");
	}

	private static KafkaSource<String> buildSource() {
		//  final String offsetResetStrategy = kafkaConfigInfo.getKafkaSourceProps().getProperty("offset.reset.strategy");
		return KafkaSource.<String>builder()
				.setBootstrapServers("localhost:19092")
				.setTopics(INPUT_TOPIC)
				.setStartingOffsets(OffsetsInitializer.earliest())
				// .setGroupId(GROUP_ID)
				// .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.valueOf(offsetResetStrategy)))
				//   .setProperties(kafkaConfigInfo.getKafkaSourceProps())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();
	}

}
