package org.apache.bookkeeper.tests.bookie.test;

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
public class BookieGetExplicitLacTest {

	// Bookie instance
	private Bookie bookie;

	// Test parameters
	private long ledgerId;
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

	public BookieGetExplicitLacTest(long ledgerId, Class<? extends Exception> expectedException) {
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
	public void getExplicitLacTest() throws IOException, InterruptedException, BookieException {
		System.out.println("\n**************** TEST ****************\n");

		ByteBuf entry = TestUtil.generateEntry(ledgerId, 1L);
		byte[] dst = new byte[10];
		entry.getBytes(16, dst);

		System.out.println("\n------------- SET -------------");

		if (expectedException == NullPointerException.class) {
			exceptionRule.expect(expectedException);
			System.out.println("Exception raised: " + expectedException.getName());
		}

		System.out.println("Ledger ID: " + ledgerId);
		System.out.println("Entry ID: " + 1L);
		System.out.println("Entry Data: " + new String(dst, StandardCharsets.UTF_8) + "\n");

		CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
		bookie.setExplicitLac(entry, (rc, lid, eid, addr, c) -> writeFuture.complete(rc), null, new byte[0]);

		ByteBuf actual = bookie.getExplicitLac(ledgerId);
		byte[] actualDst = new byte[10];
		actual.getBytes(16, actualDst);

		System.out.println("\n------------- GET -------------");
		System.out.println("Read Entry Data: " + new String(actualDst, StandardCharsets.UTF_8));

		String expectedData = new String(dst, StandardCharsets.UTF_8);
		String actualData = new String(actualDst, StandardCharsets.UTF_8);

		System.out.println("\n------------ RESULT ------------");
		System.out.println("Expected Data: " + expectedData);
		System.out.println("Actual Data: " + actualData + "\n");

		assertEquals(expectedData, actualData);
		
		System.out.println("\n**************************************\n");
	}
}
