package org.apache.bookkeeper.tests.bookie.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.conf.ServerConfiguration;
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
public class BookieSetExcplicitLacTest {

	// Bookie instance
	private Bookie bookie;

	// Test parameters
	private ByteBuf entry;
	private Object ctx;
	private byte[] masterKey;
	private Class<? extends Exception> expectedException;

	// Rule to make temporary folders
	@Rule
	public final TemporaryFolder testDir = new TemporaryFolder();

	// Rule to manage exceptions
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	// Test environment
	private File journalDir;
	private File ledgerDir;
	private ServerConfiguration conf;

	public BookieSetExcplicitLacTest(ByteBuf entry, Object ctx, byte[] masterKey, Class<? extends Exception> expectedException) {
		this.entry = entry;
		this.ctx = ctx;
		this.masterKey = masterKey;
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			// Minimal test suite
			{ TestUtil.generateEntry(1L, 1L), null, new byte[0], null },
			{ TestUtil.generateEntry(0L, 1L), "ledger-test", new byte[0], null },
			{ null, null, new byte[0], NullPointerException.class},
			{ TestUtil.generateEntry(-1L, -1L), new String(), new byte[0], IllegalArgumentException.class },
			{ TestUtil.generateEntry(2L, -1L), "ledger-test", new byte[1], IndexOutOfBoundsException.class },
			{ TestUtil.generateEntry(1L, 2L), new String(), null, NullPointerException.class } 
		});
	}

	@Before
	public void setUp() throws IOException, InterruptedException, BookieException {
		journalDir = testDir.newFolder("journal");
		ledgerDir = testDir.newFolder("ledger");

		float usage = 1.0f - ((float) ledgerDir.getUsableSpace()) / ledgerDir.getTotalSpace();

		conf = TestUtil.getConfiguration();
		conf.setJournalDirsName(new String[] { journalDir.getAbsolutePath() });
		conf.setLedgerDirNames(new String[] { ledgerDir.getAbsolutePath() });
		conf.setMetadataServiceUri(null);
		conf.setDiskUsageThreshold(usage / 2);
		conf.setDiskUsageWarnThreshold(usage / 3);
		conf.setMinUsableSizeForEntryLogCreation(Long.MIN_VALUE);
		conf.setLedgerStorageClass(InterleavedLedgerStorage.class.getName());

		bookie = new BookieImpl(conf);
		bookie.start();
	}

	@After
	public void cleanUp() {
		if (bookie != null) {
			bookie.shutdown();
		}
	}

	@Test
	public void setExcplicitLacTest() throws IOException, BookieException, InterruptedException {
		System.out.println("\n**************** TEST ****************\n");
		
		long usableSpace = ledgerDir.getUsableSpace();

		byte[] dst = new byte[10];
		if (entry != null) {
			entry.getBytes(16, dst);
		}

		System.out.println("------------- SET -------------");

		if (expectedException == NullPointerException.class) {
			exceptionRule.expect(expectedException);
			System.out.println("Exception raised: " + expectedException.getName());
		}

		System.out.println("Entry Data: " + new String(dst, StandardCharsets.UTF_8));
		System.out.println("CTX: " + ctx);
		System.out.println("Master Key: " + masterKey.length + "\n");

		CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
		bookie.setExplicitLac(entry, (rc, lid, eid, addr, c) -> writeFuture.complete(rc), ctx, masterKey);

		Thread.sleep(1000);

		System.out.println("\n------------ RESULT ------------");
		System.out.println("Usable Space before: " + usableSpace);
		System.out.println("Usable Space after: " + ledgerDir.getUsableSpace());

		assertTrue(ledgerDir.getUsableSpace() < usableSpace);
		
		System.out.println("\n**************************************\n");
	}
}
