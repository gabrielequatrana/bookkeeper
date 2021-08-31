package org.apache.bookkeeper.tests.bookie;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
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
public class BookieReadEntryTest {

	// Bookie instance
	private Bookie bookie;

	// Test parameters
	private long ledgerId;
	private long entryId;
	private Class<? extends Exception> expectedException;

	// Rule to make temporary folders
	@Rule public final TemporaryFolder testDir = new TemporaryFolder();

	// Rule to manage exceptions
	@Rule public ExpectedException exceptionRule = ExpectedException.none();

	// Test environment
	private File journalDir;
	private File ledgerDir;
	private ServerConfiguration conf;
	private ByteBuf expectedEntry;
	private static WriteCallback callback = new WriteCallback() {
		@Override
		public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
			// empty
		}
	};
	
	public BookieReadEntryTest(long ledgerId, long entryId, Class<? extends Exception> expectedException) {
		this.ledgerId = ledgerId;
		this.entryId = entryId;
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			
			// Minimal test suite
			{ 1L, 1L, null },
			{ 1L, 2L, Bookie.NoEntryException.class },
			{ 2L, 1L, Bookie.NoLedgerException.class },
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
		
		ByteBuf entry = TestUtil.validEntry();
		bookie.addEntry(entry, false, callback, null, new byte[0]);
		
		expectedEntry = entry;
		if (ledgerId != 1 || entryId != 1) {
			expectedEntry = null;
		}
		
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
	public void readEntryTest() throws IOException {

		// Retrieve the entry from the bookie
		ByteBuf actualEntry = bookie.readEntry(ledgerId, entryId);

		// Assert that the added entry is the same as the retrieved entry from the bookie
		assertEquals(expectedEntry, actualEntry);
	}
}