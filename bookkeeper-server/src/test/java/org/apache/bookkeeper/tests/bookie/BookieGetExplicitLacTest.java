package org.apache.bookkeeper.tests.bookie;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.tests.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.netty.buffer.ByteBuf;

@RunWith(Parameterized.class)
public class BookieGetExplicitLacTest {

	// Bookie instance
	private Bookie bookie;

	// Test parameters
	private long ledgerId;
	
	// Rule to make temporary folders
	@Rule public final TemporaryFolder testDir = new TemporaryFolder();

	// Test environment
	private File journalDir;
	private File ledgerDir;
	private ServerConfiguration conf;
	private ByteBuf entry;
	private CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
	private WriteCallback callback = (rc, lid, eid, addr, c) -> writeFuture.complete(rc);

	public BookieGetExplicitLacTest(long ledgerId) {
		this.ledgerId = ledgerId;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			// Minimal test suite
			{ 1L },
			{ 0L },
			{ -1L },
		});
	}

	// Setup the test environment
	@Before
	public void setUp() throws IOException, InterruptedException, BookieException {
		journalDir = testDir.newFolder("journal");
		ledgerDir = testDir.newFolder("ledger");
		conf = TestUtil.getConfiguration(journalDir, ledgerDir);
		
		bookie = new BookieImpl(conf);
		bookie.start();
		
		entry = TestUtil.generateEntry(ledgerId, 1L);
	}

	// Cleanup the test environment
	@After
	public void cleanUp() {
		bookie.shutdown();
	}

	@Test
	public void getExplicitLacTest() throws IOException, InterruptedException, BookieException {

		// Get entry data
		byte[] dst = new byte[entry.capacity() - 16];
		entry.getBytes(16, dst);

		// Add the entry to the bookie
		bookie.setExplicitLac(entry, callback, null, new byte[0]);

		// Retrieve the entry from the bookie
		ByteBuf actual = bookie.getExplicitLac(ledgerId);
		byte[] actualDst = new byte[actual.capacity() - 16];
		actual.getBytes(16, actualDst);

		// Convert data into string
		String expectedData = new String(dst, StandardCharsets.UTF_8);
		String actualData = new String(actualDst, StandardCharsets.UTF_8);

		// Assert that the added entry is the same as the retrieved entry from the bookie
		assertEquals(expectedData, actualData);
	}
}
