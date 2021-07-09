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
	private File journalDir;
	private File ledgerDir;
	private ServerConfiguration conf;
	private static CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
	private static WriteCallback callback = (rc, lid, eid, addr, c) -> writeFuture.complete(rc);

	public BookieAddEntryTest(ByteBuf entry, boolean ackBeforeSync, WriteCallback cb, Object ctx, byte[] masterKey, Class<? extends Exception> expectedException) {
		this.entry = entry;
		this.ackBeforeSync = ackBeforeSync;
		this.cb = cb;
		this.ctx = ctx;
		this.masterKey = masterKey;
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			
			// Minimal test suite
			{ TestUtil.validEntry(), false, null, null, new byte[0], null },
			{ TestUtil.validEntry(), false, callback, "ledger-test", new byte[0], null },
			{ null, false, callback, null, new byte[0], NullPointerException.class},
			{ TestUtil.invalidEntry(), false, null, new String(), new byte[0], IllegalArgumentException.class },
			{ TestUtil.invalidEntry(), true, callback, "ledger-test", new byte[1], IllegalArgumentException.class },
			{ TestUtil.validEntry(), true, callback, new String(), null, Bookie.NoLedgerException.class },
			
			// Added after the improvement of the test suite
			{ TestUtil.validEntry(), false, callback, null, new byte[1], null },
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
