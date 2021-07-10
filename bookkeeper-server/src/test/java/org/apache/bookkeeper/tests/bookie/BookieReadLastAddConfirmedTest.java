	package org.apache.bookkeeper.tests.bookie;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
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
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.netty.buffer.ByteBuf;

@RunWith(Parameterized.class)
public class BookieReadLastAddConfirmedTest {

	// Bookie instance
	private Bookie bookie;

	// Test parameters
	private long ledgerId;
	private Class<? extends Exception> expectedException;

	// Rule to make temporary folders
	@Rule public final TemporaryFolder testDir = new TemporaryFolder();

	// Rule to manage exceptions
	@Rule public ExpectedException exceptionRule = ExpectedException.none();

	// Test environment
	private File journalDir;
	private File ledgerDir;
	private ServerConfiguration conf;
	private ByteBuf entry;
	private CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
	private WriteCallback callback = (rc, lid, eid, addr, c) -> writeFuture.complete(rc);

	public BookieReadLastAddConfirmedTest(long ledgerId, Class<? extends Exception> expectedException) {
		this.ledgerId = ledgerId;
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			// Minimal test suite
			{ 1L, null },
			{ 0L, null },
			{ -1L, IllegalArgumentException.class },
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
		
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
		}
	}

	// Cleanup the test environment
	@After
	public void cleanUp() {
		bookie.shutdown();
	}

	@Test
	public void readLastAddConfirmedTest() throws IOException, BookieException, InterruptedException {

		// Add the entry to the bookie
		bookie.addEntry(entry, false, callback, null, new byte[0]);

		// Retrieve last long added from the bookie
		long actual = bookie.readLastAddConfirmed(ledgerId);

		// Assert that the last long added is the same as the retrieved long from the bookie
		assertEquals(entry.getLong(16), actual);
	}
}
