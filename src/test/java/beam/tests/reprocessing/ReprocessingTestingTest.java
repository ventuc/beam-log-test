package beam.tests.reprocessing;

import java.io.IOException;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.time.StopWatch;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.TestStream.Builder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReprocessingTestingTest {
	
	protected static final Logger logger = LoggerFactory.getLogger(ReprocessingTestingTest.class);
	
	@Rule
	public final transient TestPipeline p = TestPipeline.create();
	
	@Test
	public void test() throws IOException {
		Builder<KV<String, String>> streamBuilder = TestStream.create(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
		Instant now = Instant.now();
		Instant ingestionTime = now.minus(Duration.standardDays(1));
//		streamBuilder = streamBuilder.advanceWatermarkTo(now);
		for (int i = 1; i <= 4; i++) {
			String key = (i % 2 == 0)?"pari":"dispari";
			String value = Integer.toString(i);
			streamBuilder = streamBuilder.addElements(TimestampedValue.of(KV.of(key, value), now));
//			streamBuilder = streamBuilder.addElements(TimestampedValue.of(KV.of(key, value), now.plus(Duration.millis(((i - 1) / 2)))));
//			streamBuilder = streamBuilder.addElements(TimestampedValue.of(KV.of(key, value), ingestionTime.plus(Duration.standardMinutes(10 * ((i - 1) / 2)))));
			if (i % 1 == 0) {
				streamBuilder = streamBuilder.advanceProcessingTime(Duration.millis(1));
			}
		}
		streamBuilder = streamBuilder.advanceWatermarkTo(now);
//		streamBuilder = streamBuilder.advanceWatermarkTo(ingestionTime.plus(Duration.standardHours(1)));
//		streamBuilder = streamBuilder.advanceWatermarkTo(now);
		streamBuilder = streamBuilder.advanceProcessingTime(Duration.millis(1));
		final TestStream<KV<String, String>> stream = streamBuilder.advanceWatermarkToInfinity();
		
		logger.info("Start");
		
		PCollection<KV<String, String>> out = this.p
			.apply(stream)
			.apply(ParDo.of(new Splitter()))
//			.apply(WithTimestamps.of(s -> ingestionTime))
			.apply(ParDo.of(new Worker()))
			.apply(
				Window.<KV<String, String>>into(Sessions.withGapDuration(Duration.millis(1)))
					.triggering(AfterWatermark.pastEndOfWindow().withEarlyFirings(
						AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(1))
					))
//				Window.<KV<String, String>>into(FixedWindows.of(Duration.standardSeconds(10)))
//					.withAllowedLateness(Duration.standardMinutes(10))
					.withAllowedLateness(Duration.ZERO)
//					.triggering(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(5)))
					.accumulatingFiredPanes()
//					.discardingFiredPanes()
			)
			.apply(GroupByKey.create())
			.apply(ParDo.of(new Aggregator()));
		
//		PAssert.that(out).inWindow(new IntervalWindow(ingestionTime, Duration.standardSeconds(10))).containsInAnyOrder(Lists.newArrayList(KV.of("dispari", "(1.0)-(1.1)-(1.2)-(1.3)-(1.4)-")));
		
		StopWatch watch = new StopWatch();
		watch.start();
		this.p.run();
		watch.stop();
		logger.info("Runtime: {}", watch);
	}
	
	public static class Splitter extends DoFn<KV<String, String>, KV<String, String>> {
		
		private static final long serialVersionUID = 3829929689471598927L;
		
		protected static final Logger logger = LoggerFactory.getLogger(Splitter.class);

		@ProcessElement
		public void process(@Element KV<String, String> inputElement, OutputReceiver<KV<String, String>> outputReceiver, ProcessContext ctx) {
			logger.info("[TH: {}] Splitting element [ts: {}]: KV<{}, {}>", Thread.currentThread().getId(), ctx.timestamp(), inputElement.getKey(), inputElement.getValue());
			for (int i = 0; i < 10; i++) {
//				outputReceiver.outputWithTimestamp(KV.of(inputElement.getKey(), inputElement.getValue() + "." + i), ctx.timestamp().plus(Duration.standardSeconds(i * 2)));
				outputReceiver.output(KV.of(inputElement.getKey(), inputElement.getValue() + "." + i));
			}
		}
		
	}
	
	public static class Worker extends DoFn<KV<String, String>, KV<String, String>> {
		
		private static final long serialVersionUID = 3829929689471598927L;
		
		protected static final Logger logger = LoggerFactory.getLogger(Worker.class);
		
		@ProcessElement
		public void process(@Element KV<String, String> inputElement, OutputReceiver<KV<String, String>> outputReceiver, ProcessContext ctx) {
			logger.info("[TH: {}] Processing element [ts: {}]: KV<{}, {}>", Thread.currentThread().getId(), ctx.timestamp(), inputElement.getKey(), inputElement.getValue());
			outputReceiver.output(KV.of(inputElement.getKey(), "(" + inputElement.getValue() + ")"));
		}
		
	}
	
	public static class Aggregator extends DoFn<KV<String, Iterable<String>>, KV<String, String>> {
		
		private static final long serialVersionUID = 3829929689471598927L;
		
		protected static final Logger logger = LoggerFactory.getLogger(Aggregator.class);

		@ProcessElement
		public void process(@Element KV<String, Iterable<String>> inputElement, OutputReceiver<KV<String, String>> outputReceiver, ProcessContext ctx) {
			StringBuilder sb = new StringBuilder();
			for (String s : inputElement.getValue()) {
				sb.append(s + "-");
			}
			String combined = sb.toString();
			logger.info("Combined [{}]: {}", inputElement.getKey(), combined);
			outputReceiver.output(KV.of(inputElement.getKey(), combined));
		}
		
	}

}
