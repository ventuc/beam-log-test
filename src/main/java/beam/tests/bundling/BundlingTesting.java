package beam.tests.bundling;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nullable;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.time.StopWatch;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BundlingTesting {
	
	protected static final Logger logger = LoggerFactory.getLogger(BundlingTesting.class);
	
	protected final Pipeline pipeline;
	
	protected final String kafkaBootstrapServers;
	protected final String inputTopicName;

	protected final int splits;
	protected final int splitsInterval;
	
	public BundlingTesting(String kafkaBootstrapServers, String inputTopicName, int splits, int splitsInterval, @Nullable PipelineOptions options) {
		this.kafkaBootstrapServers = kafkaBootstrapServers;
		this.inputTopicName = inputTopicName;
		
		this.splits = splits;
		this.splitsInterval = splitsInterval;
		
		if (options == null) {
			// Create Beam pipeline options
			logger.info("Creating pipeline options...");
			options = PipelineOptionsFactory.create();
			logger.debug("Pipeline options created\n{}", options);
		}
		
		// Create the pipeline
		logger.info("Creating the pipeline...");
		this.pipeline = Pipeline.create(options);
		logger.debug("Empty pipeline created:\n{}", pipeline);
	}
	
	public BundlingTesting(String kafkaBootstrapServers, String inputTopicName, Pipeline pipeline, int splits, int splitsInterval) {
		this.kafkaBootstrapServers = kafkaBootstrapServers;
		this.inputTopicName = inputTopicName;
		this.pipeline = pipeline;
		
		this.splits = splits;
		this.splitsInterval = splitsInterval;
	}
	
	public PipelineResult run() {
		Map<String, Object> kafkaConsumerConfig = new HashMap<>();
		kafkaConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		// Create a pipeline and apply the transform
		PCollection<KV<String, String>> output = this.pipeline
			// Read from Kafka
			.apply(KafkaIO.<String, String>read()
				.withBootstrapServers(this.kafkaBootstrapServers)
				.withKeyDeserializer(StringDeserializer.class)
				.withValueDeserializer(StringDeserializer.class)
				.withTopic(this.inputTopicName)
//				.withMaxReadTime(Duration.standardMinutes(1))
				.withConsumerConfigUpdates(kafkaConsumerConfig)
				.withTimestampPolicyFactory(new KafkaReaderPolicyFactory())
				.withoutMetadata()
			)
			.apply("print input", ParDo.of(new Printer("input")))
			// Assign timestamp to now
//			.apply(WithTimestamps.of(i -> Instant.now()))
			// Window
//			.apply("window", Window.into(Sessions.withGapDuration(Duration.standardSeconds(5))))
			.apply("window", Window.into(Sessions.withGapDuration(Duration.millis(5))))
			// Split
			.apply("split", ParDo.of(new Splitter(this.splits, this.splitsInterval)))
			// Combine
			.apply("combine", Combine.perKey(new Combiner()))
			// Print
			.apply("print output", ParDo.of(new Printer("combined", true)));
		
		logger.info("Running the pipeline");
		return this.pipeline.run();
	}

	public static void main(String[] args) {
		logger.info("Create a " + BundlingTesting.class.getName() + " instance");
		PipelineOptions options = PipelineOptionsFactory.create();
		options.setRunner(SparkRunner.class);
		BundlingTesting logTesting = new BundlingTesting(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]), options);
		
		logger.info("Execute");
		StopWatch watch = StopWatch.createStarted();
		logTesting.run().waitUntilFinish();
		watch.stop();
		logger.info("Execution time: {}", watch);
	}

	public static class Splitter extends DoFn<KV<String, String>, KV<String, String>> {
		
		private static final long serialVersionUID = 4267952862935753653L;
		
		protected static final Logger logger = LoggerFactory.getLogger(Splitter.class);
		
		protected final int splits;
		protected final int interval;
		
		public Splitter(int splits, int interval) {
			this.splits = splits;
			this.interval = interval;
		}

		@ProcessElement
		public void process(@Element KV<String, String> inputElement, OutputReceiver<KV<String, String>> outputReceiver, ProcessContext ctx) throws InterruptedException {
			logger.info("[TH: {}] Splitting element [ts: {}]: KV<{}, {}>", Thread.currentThread().getId(), ctx.timestamp(), inputElement.getKey(), inputElement.getValue());
			for (int i = 0; i < splits; i++) {
				if (this.interval > 0) {
					Thread.sleep(this.interval);
				}
				outputReceiver.output(KV.of(inputElement.getKey(), inputElement.getValue() + "." + i));
			}
		}
		
	}
	
	public static class Combiner implements SerializableFunction<Iterable<String>, String> {
		
		private static final long serialVersionUID = 3892715029407709478L;
		
		protected static final Logger logger = LoggerFactory.getLogger(Combiner.class);
		
		@Override
		public String apply(Iterable<String> input) {
			StringBuilder sb = new StringBuilder();
			String first = null;
			String last = null;
			int i = 0;
			for (String s : input) {
				sb.append(s);
				if (i == 0) {
					first = s;
				}
				last = s;
				i++;
			}
			logger.info("[TH: {}] Combined segment [now: {}]: <{},{}>", Thread.currentThread().getId(), Instant.now(), cutValue(first), cutValue(last));
			return sb.toString();
		}
		
	}
	
	public static class Printer extends DoFn<KV<String, String>, KV<String, String>> {
		
		private static final long serialVersionUID = -3985657108722803817L;
		
		protected static final Logger logger = LoggerFactory.getLogger(Printer.class);
		
		protected final String prefix;
		
		protected final boolean cutValue;
		
		public Printer(String prefix) {
			this(prefix, false);
		}
		
		public Printer(String prefix, boolean cutValue) {
			this.prefix = prefix;
			this.cutValue = cutValue;
		}

		@ProcessElement
		public void process(@Element KV<String, String> inputElement, OutputReceiver<KV<String, String>> outputReceiver, ProcessContext ctx) {
			String value = inputElement.getValue();
			if (this.cutValue) {
				value = cutValue(value);
			}
			logger.info("{} [ts: {}, now: {}]: KV<{}, {}>", prefix, ctx.timestamp(), Instant.now(), inputElement.getKey(), value);
			outputReceiver.output(inputElement);
		}
		
	}
	
	public static class KafkaReaderPolicyFactory implements TimestampPolicyFactory<String, String> {
		
		private static final long serialVersionUID = 6002098168625414287L;

		protected static final Logger logger = LoggerFactory.getLogger(KafkaReaderPolicyFactory.class);
		
		@Override
		public TimestampPolicy<String, String> createTimestampPolicy(TopicPartition tp, Optional<Instant> previousWatermark) {
//			return new LastWatermarkPolicy(previousWatermark);
			return new Policy();
		}
		
		public static class Policy extends TimestampPolicy<String, String> {
			
			@Override
			public Instant getTimestampForRecord(PartitionContext ctx, KafkaRecord<String, String> record) {
				Instant now = Instant.now();
				logger.info("[TH: {}] Timestamp: {}", Thread.currentThread().getId(), now);
				return now;
			}
			
			@Override
			public Instant getWatermark(PartitionContext ctx) {
				Instant now = Instant.now();
				logger.info("[TH: {}] Watermark: {}", Thread.currentThread().getId(), now);
				return now;
			}
			
		}
		
		public static class LastWatermarkPolicy extends TimestampPolicy<String, String> {
			
			private Instant currentWatermark;
			
			public LastWatermarkPolicy(Optional<Instant> previousWatermark) {
				this.currentWatermark = previousWatermark.orElse(Instant.now());
			}
			
			@Override
			public Instant getTimestampForRecord(PartitionContext ctx, KafkaRecord<String, String> record) {
				this.currentWatermark = Instant.now();
				logger.info("[TH: {}] Timestamp: {}", Thread.currentThread().getId(), this.currentWatermark);
				return this.currentWatermark;
			}
			
			@Override
			public Instant getWatermark(PartitionContext ctx) {
				logger.info("[TH: {}] Watermark: {}", Thread.currentThread().getId(), this.currentWatermark);
				return this.currentWatermark;
			}
			
		}
		
	}
	
	public static String cutValue(String value) {
		return value.substring(0, Math.min(value.length(), 10)) + " ... " + value.substring(Math.max(value.length() - 10, 0));
	}
	
}
