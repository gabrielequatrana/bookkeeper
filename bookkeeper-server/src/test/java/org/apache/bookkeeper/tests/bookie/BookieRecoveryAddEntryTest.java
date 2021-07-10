package org.apache.bookkeeper.tests.bookie;

import static org.junit.Assert.assertTrue;

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
public class BookieRecoveryAddEntryTest {

	// Bookie instance
	private Bookie bookie;

	// Test parameters
	private ByteBuf entry;
	private WriteCallback cb;
	private Object ctx;
	private byte[] masterKey;
	private Class<? extends Exception> expectedException;

	// Rule to make temporary folders
	@Rule public final TemporaryFolder testDir = new TemporaryFolder();

	// Rule to manage exceptions
	@Rule public ExpectedException exceptionRule = ExpectedException.none();

	// Test environment
	private File journalDir;
	private File ledgerDir;
	private ServerConfiguration conf;
	private static CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
	private static WriteCallback callback = (rc, lid, eid, addr, c) -> writeFuture.complete(rc);
	
	public BookieRecoveryAddEntryTest(ByteBuf entry, WriteCallback cb, Object ctx, byte[] masterKey, Class<? extends Exception> expectedException) {
		this.entry = entry;
		this.cb = cb;
		this.ctx = ctx;
		this.masterKey = masterKey;
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			
			// Minimal test suite
			{ TestUtil.validEntry(), null, null, new byte[1], null },
			{ TestUtil.validEntry(), callback, "ledger-test", new byte[0], null },
			{ null, callback, null, new byte[0], NullPointerException.class },
			{ TestUtil.invalidEntry(), null, "ledger-test", new byte[1], IllegalArgumentException.class },

			// Added after the improvement of the test suite
			//{ TestUtil.invalidEntry(), callback, "ledger-test", new byte[1], IllegalArgumentException.class },
			//{ TestUtil.validEntry(), callback, null, new byte[0], null },
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
	public void recoveryAddEntryTest() throws IOException, BookieException, InterruptedException {
		
		// Get free space before adding a new entry
		long usableSpace = ledgerDir.getUsableSpace();

		// Get ledger ID from the entry
		long ledgerId = entry.getLong(0);

		// Fence the ledger before adding the entry to test the method recoveryAddEntry(..)
		bookie.fenceLedger(ledgerId, masterKey);
		
		// Add the entry to the bookie 
		bookie.recoveryAddEntry(entry, cb, ctx, masterKey);

		// Sleep for 1 second
		Thread.sleep(1000);

		// Assert that the free space is less than before adding the entry
		assertTrue(ledgerDir.getUsableSpace() < usableSpace);
	}
}
