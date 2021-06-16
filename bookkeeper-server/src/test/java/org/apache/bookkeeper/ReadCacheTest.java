package org.apache.bookkeeper.tests;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.bookie.storage.ldb.ReadCache;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

@RunWith(Parameterized.class)
public class ReadCacheTest {

	// ReadCache instance
	private ReadCache readCache;

	// Cache configs
	private static final int ENTRY_SIZE = 1024;
	private static final int MAX_ENTRIES = 10;
	private static final int CACHE_SIZE = ENTRY_SIZE * MAX_ENTRIES;

	// Test parameters
	private long ledgerId;
	private long entryId;
	private ByteBuf entry;
	private Class<? extends Exception> expectedException;

	public ReadCacheTest(long ledgerId, long entryId, ByteBuf entry, Class<? extends Exception> expectedException) {
		this.ledgerId = ledgerId;
		this.entryId = entryId;
		this.entry = entry;
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getTestParameters() {
		return Arrays.asList(new Object[][] { 
			{ -1, 1, Unpooled.wrappedBuffer(new byte[ENTRY_SIZE]), IllegalArgumentException.class },		// Valid entry
			{ 0, -1, Unpooled.wrappedBuffer(new byte[ENTRY_SIZE]), null },									// Valid entry
			{ 1, 0, null, NullPointerException.class },														// NULL entry
			{ 0, 1, Unpooled.wrappedBuffer(new byte[CACHE_SIZE + 1]), IndexOutOfBoundsException.class } 	// Invalid entry
		});
	}

	@Rule
	public ExpectedException rule = ExpectedException.none();

	@Before
	public void setUp() {
		readCache = new ReadCache(UnpooledByteBufAllocator.DEFAULT, CACHE_SIZE);
	}

	@After
	public void cleanUp() {
		System.out.println("---------------------\n");
		readCache.close();
	}

	@Test
	public void testPut() {
		System.out.println("-------- PUT --------");
		System.out.println("LedgerID: " + ledgerId);
		System.out.println("EntryID: " + entryId);
		System.out.println("Entry: " + entry);
		
		if (expectedException != null) {
			rule.expect(expectedException);
			System.out.println("Exception raised: " + expectedException.getName());
		}
		
		readCache.put(ledgerId, entryId, entry);
		assertEquals(1, readCache.count());
		
	}

	@Test
	public void testGet() {
		System.out.println("-------- GET --------");
		System.out.println("LedgerID: " + ledgerId);
		System.out.println("EntryID: " + entryId);
		System.out.println("ExpectedEntry: " + entry);
		
		if (expectedException != null) {
			rule.expect(expectedException);
			System.out.println("Exception raised: " + expectedException.getName());
		}

		System.out.println("ActualEntry: " + readCache.get(ledgerId, entryId));
		
		readCache.put(ledgerId, entryId, entry);
		assertEquals(entry, readCache.get(ledgerId, entryId));
	}
}
