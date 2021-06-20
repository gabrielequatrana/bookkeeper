package org.apache.bookkeeper.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.Bookie.NoLedgerException;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.conf.ServerConfiguration;
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
import io.netty.buffer.Unpooled;

@RunWith(Parameterized.class)
public class BookieTest {
	
	// Bookie instance
	private Bookie bookie;
	
	// Test parameters
	private long ledgerId;
	private long entryId;
	private boolean ackBeforeSync;
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
	
	public BookieTest(long ledgerId, long entryId, boolean ackBeforeSync, Object ctx, byte[] masterKey, Class<? extends Exception> expectedException) {
		this.ledgerId = ledgerId;
		this.entryId = entryId;
		this.ackBeforeSync = ackBeforeSync;
		this.ctx = ctx;
		this.masterKey = masterKey;
		this.expectedException = expectedException;
	}
	
	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			
			// Minimal test suite
			{ 1L, 1L, false, null, new byte[0], null },
			{ 0L, 0L, false, null, new byte[0], null },
			{ 0L, 1L, false , "ledger-test", new byte[0], null},
			{ -1L, -1L, false, new String(), new byte[0], IllegalArgumentException.class },
			{ 2L, -1L, true, "ledger-test", new byte[1], IndexOutOfBoundsException.class},
			{ 1L, 2L, true, new String(), null, NullPointerException.class}
		});
	}
	
	@Before
	public void setUp() throws IOException, InterruptedException, BookieException {
		journalDir = testDir.newFolder("journal");
		ledgerDir = testDir.newFolder("ledger");
		
		float usage = 1.0f - ((float) ledgerDir.getUsableSpace()) / ledgerDir.getTotalSpace();
		
		conf = TestConfiguration.getConfiguration();
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
	public void addEntryTest() throws IOException, BookieException, InterruptedException {
		long usableSpace = ledgerDir.getUsableSpace();
		ByteBuf entry = generateEntry(ledgerId, entryId);
		byte[] data = ("ledger-" + ledgerId + "-" + entryId).getBytes();
		byte[] dst = new byte[data.length];
		entry.getBytes(16, dst);
		
		System.out.println("\n------------- ADD -------------");
		
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
			System.out.println("Exception raised: " + expectedException.getName());
		}

		System.out.println("Fence: false");
		System.out.println("Ledger ID: " + ledgerId);
		System.out.println("Entry ID: " + entryId);
		System.out.println("Entry Data: " + new String(dst, StandardCharsets.UTF_8));
		System.out.println("Ack Before Sync: " + ackBeforeSync);
		System.out.println("CTX: " + ctx);
		System.out.println("Master Key: " + masterKey.length + "\n");
		
		CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
		bookie.addEntry(entry, ackBeforeSync, (rc, ledgerId, entryId, addr, ctx) -> writeFuture.complete(rc), ctx, masterKey);
		
		Thread.sleep(1000);
		
		System.out.println("\n------------ RESULT ------------");
		System.out.println("Usable Space before: " + usableSpace);
		System.out.println("Usable Space after: " + ledgerDir.getUsableSpace() + "\n");
		
		assertTrue(ledgerDir.getUsableSpace() < usableSpace);
	}
	
	@Test
	public void readEntryTest() throws IOException, BookieException, InterruptedException {
		byte[] data = ("ledger-" + ledgerId + "-" + entryId).getBytes();
		
		ByteBuf entry = generateEntry(ledgerId, entryId);	
		byte[] dst = new byte[data.length];
		entry.getBytes(16, dst);
		
		System.out.println("\n------------- ADD -------------");
		
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
			System.out.println("Exception raised: " + expectedException.getName());
		}
		
		System.out.println("Fence: false");
		System.out.println("Ledger ID: " + ledgerId);
		System.out.println("Entry ID: " + entryId);
		System.out.println("Entry Data: " + new String(dst, StandardCharsets.UTF_8));
		System.out.println("Ack Before Sync: " + ackBeforeSync);
		System.out.println("CTX: " + ctx);
		System.out.println("Master Key: " + masterKey.length + "\n");
		
		CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
		bookie.addEntry(entry, ackBeforeSync, (rc, ledgerId, entryId, addr, ctx) -> writeFuture.complete(rc), ctx, masterKey);
		
		ByteBuf actual = bookie.readEntry(ledgerId, entryId);
		byte[] actualDst = new byte[data.length];
		actual.getBytes(16, actualDst);
		
		System.out.println("\n------------- READ -------------");
		System.out.println("Read Entry Data: " + new String(actualDst, StandardCharsets.UTF_8));
		
		String expectedData = new String(dst, StandardCharsets.UTF_8);
		String actualData = new String(actualDst, StandardCharsets.UTF_8);
		
		System.out.println("\n------------ RESULT ------------");
		System.out.println("Expected Data: " + expectedData);
		System.out.println("Actual Data: " + actualData + "\n");
		
		assertEquals(expectedData, actualData);
	}
	
	@Test
	public void fenceAndRecoveryAddEntryTest() throws IOException, BookieException, InterruptedException {
		byte[] data = ("ledger-" + ledgerId + "-" + entryId).getBytes();
		long usableSpace = ledgerDir.getUsableSpace();
		
		ByteBuf entry = generateEntry(ledgerId, entryId);	
		byte[] dst = new byte[data.length];
		entry.getBytes(16, dst);
		
		System.out.println("\n------------- ADD -------------");
		
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
			System.out.println("Exception raised: " + expectedException.getName());
		}
		
		System.out.println("Fence: true");
		System.out.println("Ledger ID: " + ledgerId);
		System.out.println("Entry ID: " + entryId);
		System.out.println("Entry Data: " + new String(dst, StandardCharsets.UTF_8));
		System.out.println("Ack Before Sync: " + ackBeforeSync);
		System.out.println("CTX: " + ctx);
		System.out.println("Master Key: " + masterKey.length + "\n");
		
		CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
		bookie.fenceLedger(ledgerId, masterKey);
		bookie.recoveryAddEntry(entry, (rc, ledgerId, entryId, addr, ctx) -> writeFuture.complete(rc), ctx, masterKey);
	
		Thread.sleep(1000);
		
		System.out.println("\n------------ RESULT ------------");
		System.out.println("Usable Space before: " + usableSpace);
		System.out.println("Usable Space after: " + ledgerDir.getUsableSpace() + "\n");
		
		assertTrue(ledgerDir.getUsableSpace() < usableSpace);
	}
	
	@Test
	public void setExcplicitLacTest() throws IOException, BookieException, InterruptedException {
		byte[] data = ("ledger-" + ledgerId + "-" + entryId).getBytes();
		long usableSpace = ledgerDir.getUsableSpace();
		
		ByteBuf entry = generateEntry(ledgerId, entryId);	
		byte[] dst = new byte[data.length];
		entry.getBytes(16, dst);
		
		System.out.println("\n------------- SET -------------");
		
		if (expectedException == NullPointerException.class) {
			exceptionRule.expect(expectedException);
			System.out.println("Exception raised: " + expectedException.getName());
		}
		
		System.out.println("Fence: false");
		System.out.println("Ledger ID: " + ledgerId);
		System.out.println("Entry ID: " + entryId);
		System.out.println("Entry Data: " + new String(dst, StandardCharsets.UTF_8));
		System.out.println("Ack Before Sync: " + ackBeforeSync);
		System.out.println("CTX: " + ctx);
		System.out.println("Master Key: " + masterKey.length + "\n");
		
		CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
		bookie.setExplicitLac(entry, (rc, ledgerId, entryId, addr, ctx) -> writeFuture.complete(rc), ctx, masterKey);
		
		Thread.sleep(1000);

		System.out.println("\n------------ RESULT ------------");
		System.out.println("Usable Space before: " + usableSpace);
		System.out.println("Usable Space after: " + ledgerDir.getUsableSpace() + "\n");
		
		assertTrue(ledgerDir.getUsableSpace() < usableSpace);
	}
	
	@Test
	public void getExplicitLacTest() throws IOException, InterruptedException, BookieException {
		byte[] data = ("ledger-" + ledgerId + "-" + entryId).getBytes();
		
		ByteBuf entry = generateEntry(ledgerId, entryId);	
		byte[] dst = new byte[data.length];
		entry.getBytes(16, dst);
		
		System.out.println("\n------------- SET -------------");
		
		if (expectedException == NullPointerException.class) {
			exceptionRule.expect(expectedException);
			System.out.println("Exception raised: " + expectedException.getName());
		}
		
		System.out.println("Fence: false");
		System.out.println("Ledger ID: " + ledgerId);
		System.out.println("Entry ID: " + entryId);
		System.out.println("Entry Data: " + new String(dst, StandardCharsets.UTF_8));
		System.out.println("Ack Before Sync: " + ackBeforeSync);
		System.out.println("CTX: " + ctx);
		System.out.println("Master Key: " + masterKey.length + "\n");
		
		CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
		bookie.setExplicitLac(entry, (rc, ledgerId, entryId, addr, ctx) -> writeFuture.complete(rc), ctx, masterKey);
		
		ByteBuf actual = bookie.getExplicitLac(ledgerId);
		byte[] actualDst = new byte[data.length];
		actual.getBytes(16, actualDst);
		
		System.out.println("\n------------- GET -------------");
		System.out.println("Read Entry Data: " + new String(actualDst, StandardCharsets.UTF_8));
		
		String expectedData = new String(dst, StandardCharsets.UTF_8);
		String actualData = new String(actualDst, StandardCharsets.UTF_8);
		
		System.out.println("\n------------ RESULT ------------");
		System.out.println("Expected Data: " + expectedData);
		System.out.println("Actual Data: " + actualData + "\n");
		
		assertEquals(expectedData, actualData);
	}
	
	@Test
	public void readLAstAddConfirmedTest() throws NoLedgerException, IOException, BookieException, InterruptedException {
		byte[] data = ("ledger-" + ledgerId + "-" + entryId).getBytes();
		
		ByteBuf entry = generateEntry(ledgerId, entryId);	
		byte[] dst = new byte[data.length];
		entry.getBytes(16, dst);
		
		System.out.println("\n------------- ADD -------------");
		
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
			System.out.println("Exception raised: " + expectedException.getName());
		}
		
		System.out.println("Fence: false");
		System.out.println("Ledger ID: " + ledgerId);
		System.out.println("Entry ID: " + entryId);
		System.out.println("Entry Data: " + new String(dst, StandardCharsets.UTF_8));
		System.out.println("Ack Before Sync: " + ackBeforeSync);
		System.out.println("CTX: " + ctx);
		System.out.println("Master Key: " + masterKey.length + "\n");
		
		CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
		bookie.addEntry(entry, ackBeforeSync, (rc, ledgerId, entryId, addr, ctx) -> writeFuture.complete(rc), ctx, masterKey);
		
		long actual = bookie.readLastAddConfirmed(ledgerId);
		
		System.out.println("\n------------- READ -------------");
		System.out.println("Last Long Added: " + actual);
		
		System.out.println("\n------------ RESULT ------------");
		System.out.println("Expected: " + entry.getLong(15));
		System.out.println("Actual: " + actual + "\n");
		
		assertEquals(entry.getLong(16), actual);	
	}
	
	@Test
	public void getListOfEntriesOfLedgerTest() throws NoLedgerException, IOException, BookieException, InterruptedException {
		byte[] data = ("ledger-" + ledgerId + "-" + entryId).getBytes();
		
		ByteBuf buf1 = generateEntry(ledgerId, entryId+1L);
		ByteBuf buf2 = generateEntry(ledgerId, entryId+2L);
		ByteBuf buf3 = generateEntry(ledgerId, entryId+3L);
		
		ByteBuf entry = generateEntry(ledgerId, entryId);	
		byte[] dst = new byte[data.length];
		entry.getBytes(16, dst);
		
		System.out.println("\n------------- ADD -------------");
		
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
			System.out.println("Exception raised: " + expectedException.getName());
		}
		
		System.out.println("Fence: false");
		System.out.println("Ledger ID: " + ledgerId);
		System.out.println("Entry ID: " + entryId);
		System.out.println("Entry Data: " + new String(dst, StandardCharsets.UTF_8));
		System.out.println("Ack Before Sync: " + ackBeforeSync);
		System.out.println("CTX: " + ctx);
		System.out.println("Master Key: " + masterKey.length + "\n");
		
		CompletableFuture<Integer> writeFuture = new CompletableFuture<>();
		bookie.addEntry(buf1, ackBeforeSync, (rc, ledgerId, entryId, addr, ctx) -> writeFuture.complete(rc), ctx, masterKey);
		bookie.addEntry(buf2, ackBeforeSync, (rc, ledgerId, entryId, addr, ctx) -> writeFuture.complete(rc), ctx, masterKey);
		bookie.addEntry(buf3, ackBeforeSync, (rc, ledgerId, entryId, addr, ctx) -> writeFuture.complete(rc), ctx, masterKey);
		bookie.addEntry(entry, ackBeforeSync, (rc, ledgerId, entryId, addr, ctx) -> writeFuture.complete(rc), ctx, masterKey);
		
		Iterator<Long> it = bookie.getListOfEntriesOfLedger(ledgerId);
		long index = 0L;
		while (it.hasNext()) {
			index = it.next();
		}
		
		System.out.println("\n------------- READ -------------");
		System.out.println("Last EntryId Added: " + index);
		
		long actual = index - 3L;
		
		System.out.println("\n------------ RESULT ------------");
		System.out.println("Expected: " + entryId);
		System.out.println("Actual: " + actual + "\n");
		
		assertEquals(entryId, actual);
	}
	
	private ByteBuf generateEntry(long ledgerId, long entryId) {
		byte[] data = ("ledger-" + ledgerId + "-" + entryId).getBytes();
		ByteBuf buf = Unpooled.buffer(8 + 8 + data.length);
		buf.writeLong(ledgerId);
		buf.writeLong(entryId);
		buf.writeBytes(data);
		return buf;
	}
}
