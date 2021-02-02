package beam.tests.log;

import java.io.IOException;

import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;

public class LogTestingTest {
	
	@Rule
	public final transient TestPipeline p = TestPipeline.create();
	
	@Test
	public void test() throws IOException {
		new LogTesting(this.p).run();
	}

}
