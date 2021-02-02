package beam.tests.log;
import java.net.UnknownHostException;

import javax.annotation.Nullable;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.time.StopWatch;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
//import org.apache.beam.sdk.extensions.jackson.*;
//import org.apache.beam.sdk.extensions.jackson.ParseJsons.ParseJsonsWithFailures;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
//import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//mvn compile exec:java -Dexec.mainClass=BeamTest1 -Dexec.args="--runner=SparkRunner" -Pspark-runner

public class LogTesting {
	
	protected static final Logger logger = LoggerFactory.getLogger(LogTesting.class);
	
	protected final Pipeline pipeline;

	public LogTesting(@Nullable PipelineOptions options) {
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
	
	public LogTesting(Pipeline pipeline) {
		this.pipeline = pipeline;
	}
	
	public void run() {
		logger.info("Creating an input PCollection of strings...");
		PCollection<String> input = this.pipeline.apply(Create.of("a", "b", null, "c", "").withCoder(NullableCoder.of(StringUtf8Coder.of())));
		
		logger.info("Create a transform which adds \"-test\" to all input strings");
		input.apply(ParDo.of(new StringTransformer()));
		
		logger.info("Running the pipeline");
		StopWatch watch = new StopWatch();
		watch.start();
		this.pipeline.run();
		watch.stop();
		logger.info("Pipeline executed in {}", watch.toString());
	}

	public static void main(String[] args) throws UnknownHostException {
		logger.info("Create a " + LogTesting.class.getName() + " instance");
		PipelineOptions options = PipelineOptionsFactory.create();
		options.setRunner(SparkRunner.class);
		LogTesting logTesting = new LogTesting(options);
		
		logger.info("Execute");
		logTesting.run();
	}
	
	protected static class StringTransformer extends DoFn<String, String> {
		
		private static final long serialVersionUID = -1902683679840001173L;
		
		protected static final Logger logger = LoggerFactory.getLogger(StringTransformer.class);

		@ProcessElement
		public void process(@Element String element, OutputReceiver<String> outputReceiver) {
			logger.info("Adding \"test\" to string \"" + element + "\"");
			if (element == null) {
				logger.error("Skipping null string");
			}
			else {
				if (element.isEmpty()) {
					logger.warn("Input string is empty!");
				}
				
				String out = element.concat("-test");
				outputReceiver.output(out);
				logger.debug("Output new string \"" + out + "\"");
			}
		}
		
	}
	
}
