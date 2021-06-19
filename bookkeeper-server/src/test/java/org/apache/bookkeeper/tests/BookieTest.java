package org.apache.bookkeeper.tests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

@RunWith(Parameterized.class)
public class BookieTest {
	
	private Bookie bookie;
	private ServerConfiguration conf;
	
	private static final int ENTRY_SIZE = 1024;
	
	private ByteBuf entry;
	private boolean ackBeforeSync;
	private WriteCallback cb;
	private Object ctx;
	private byte[] masterKey;
	
	public BookieTest(ByteBuf entry, boolean ackBeforeSync, WriteCallback cb, Object ctx, byte[] masterKey) {
		this.entry = entry;
		this.ackBeforeSync = ackBeforeSync;
		this.cb = cb;
		this.ctx = ctx;
		this.masterKey = masterKey;
	}
	
	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			{ Unpooled.wrappedBuffer(new byte[ENTRY_SIZE]), false, null, null, new byte[512] },
			{ Unpooled.wrappedBuffer(new byte[ENTRY_SIZE]), true, null, null, new byte[ENTRY_SIZE]},
		});
	}
	
	@Before
	public void setUp() throws IOException, InterruptedException, BookieException {
		conf = TestConfiguration.getConfiguration();
		bookie = new BookieImpl(conf);
	}
	
	@After
	public void cleanUp() {
		bookie.shutdown();
		conf.clear();
	}
	
	@Test
	public void testAddAndRead() throws IOException, BookieException, InterruptedException {
		System.out.println("----------- ADD -----------");
		System.out.println("Entry: " + entry);
		System.out.println("Ack Before Sync: " + ackBeforeSync);
		System.out.println("Write Callback: " + cb);
		System.out.println("CTX: " + ctx);
		System.out.println("Master Key: " + masterKey);
		
		bookie.addEntry(entry, ackBeforeSync, cb, ctx, masterKey);
		
		long entryId = entry.readerIndex();
		long ledgerId = entry.getLong((int)entryId);
		
		System.out.println("\n----------- READ -----------");
		System.out.println("LedgerId: " + ledgerId);
		System.out.println("EntryId: " + entryId);
		
		ByteBuf actual = bookie.readEntry(ledgerId, entryId);
		
		System.out.println("\n---------- RESULT ----------");
		System.out.println("Expected: " + entry);
		System.out.println("Actual: " + actual);
		
		assertEquals(entry, actual);
	}
	
	@Test
	public void testRecoveryAddEntry() throws IOException, BookieException, InterruptedException {
		System.out.println("----------- ADD -----------");
		System.out.println("Entry: " + entry);
		System.out.println("Ack Before Sync: " + ackBeforeSync);
		System.out.println("Write Callback: " + cb);
		System.out.println("CTX: " + ctx);
		System.out.println("Master Key: " + masterKey);
		
		long entryId = entry.readerIndex();
		long ledgerId = entry.getLong((int)entryId);
		
		bookie.fenceLedger(ledgerId, masterKey);	
		bookie.recoveryAddEntry(entry, cb, ctx, masterKey);
		
		System.out.println("\n----------- READ -----------");
		System.out.println("LedgerId: " + ledgerId);
		System.out.println("EntryId: " + entryId);
		
		ByteBuf actual = bookie.readEntry(ledgerId, entryId);
		
		System.out.println("\n---------- RESULT ----------");
		System.out.println("Expected: " + entry);
		System.out.println("Actual: " + actual);
		
		assertEquals(entry, actual);
	}
}
