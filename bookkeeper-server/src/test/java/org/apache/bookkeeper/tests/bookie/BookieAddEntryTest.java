package org.apache.bookkeeper.tests.bookie;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieException.LedgerFencedException;
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
public class BookieAddEntryTest {

	// Bookie instance
	private Bookie bookie;

	// Test parameters
	private ByteBuf entry;
	private boolean ackBeforeSync;
	private WriteCallback cb;
	private Object ctx;
	private byte[] masterKey;
	private Class<? extends Exception> expectedException;

	// Rule to make temporary folders
	@Rule public final TemporaryFolder testDir = new TemporaryFolder();

	// Rule to manage exceptions
	@Rule public ExpectedException exceptionRule = ExpectedException.none();

	// Test environment
	private boolean fenced;
	private File journalDir;
	private File ledgerDir;
	private ServerConfiguration conf;
	private static Object validCtx = new Object();
	private static byte[] validMasterKey = {0, 1, 2, 3, 4};
	private static byte[] emptyMasterKey = {};
	private static WriteCallback validCb = new WriteCallback() {
		@Override
		public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
			// empty
		}
	};
	
	public BookieAddEntryTest(ByteBuf entry, boolean ackBeforeSync, WriteCallback cb, Object ctx, byte[] masterKey, boolean improvement, Class<? extends Exception> expectedException) {
		this.entry = entry;
		this.ackBeforeSync = ackBeforeSync;
		this.cb = cb;
		this.ctx = ctx;
		this.masterKey = masterKey;
		this.fenced = improvement;
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			
			// Minimal test suite 
			{ TestUtil.validEntry(), false, null, null, validMasterKey, false, null },
			{ null, true, validCb, null, emptyMasterKey, false, NullPointerException.class },
			{ TestUtil.invalidEntry(), false, null, validCtx, validMasterKey, false, IllegalArgumentException.class },
			
			// Added after the improvement of the test suite
			{ TestUtil.validEntry(), true, validCb, validCtx, validMasterKey, true, LedgerFencedException.class },
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
	public void addEntryTest() throws IOException, BookieException, InterruptedException {
		
		if (fenced) {
			
			// Fence the ledger 1
			bookie.fenceLedger(1L, masterKey);
		}
		
		// Get free space before adding a new entry
		long usableSpace = ledgerDir.getUsableSpace();
		
		// Add the entry to the bookie
		bookie.addEntry(entry, ackBeforeSync, cb, ctx, masterKey);

		// Sleep for 1 second
		Thread.sleep(1000);
	
		// Assert that the free space is less than before adding the entry
		assertTrue(ledgerDir.getUsableSpace() < usableSpace);
	}
}
