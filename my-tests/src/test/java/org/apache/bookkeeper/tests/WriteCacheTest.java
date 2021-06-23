package org.apache.bookkeeper.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.bookie.storage.ldb.WriteCache;
import org.apache.bookkeeper.tests.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

@RunWith(Parameterized.class)
public class WriteCacheTest {
	
	// WriteCache instance
	private WriteCache writeCache;

	// Cache configs
	private static final int ENTRY_SIZE = 1024;
	private static final int MAX_ENTRIES = 10;
	private static final int CACHE_SIZE = ENTRY_SIZE * MAX_ENTRIES;
	
	// Test parameters
	private long ledgerId;
	private long entryId;
	private ByteBuf entry;
	private Class<? extends Exception> expectedException;
	
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();
	
	public WriteCacheTest(long ledgerId, long entryId, ByteBuf entry, Class<? extends Exception> expectedException) {
		this.ledgerId = ledgerId;
		this.entryId = entryId;
		this.entry = entry;
		this.expectedException = expectedException;
	}
	
	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			
			// Minimal test suite
			{ 0L, 1L, TestUtil.generateEntry(1L, 1L), null },
			{ -1L, 0L, TestUtil.generateEntry(-1L, 0L), IllegalArgumentException.class },
			{ 0L, -1L, TestUtil.generateEntry(0L, -1L), IllegalArgumentException.class },
			{ 1L, 2L, Unpooled.wrappedBuffer(new byte[CACHE_SIZE + 1]), IndexOutOfBoundsException.class },
			{ 0L, 1L, null, NullPointerException.class }
		});
	}
	
	@Before
	public void setUp() {
		System.out.println("\n***************** TEST *****************");
		writeCache = new WriteCache(ByteBufAllocator.DEFAULT, CACHE_SIZE);
	}
	
	@After
	public void cleanUp() {
		writeCache.clear();
		writeCache.close();
	}
	
	@Test
	public void putTest() {
		
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
			System.out.println("Exception raised: " + expectedException.getName());
		}
		
		long countBefore = writeCache.count();
		byte[] data = ("ledger-" + ledgerId + "-" + entryId).getBytes();	
		byte[] dst = new byte[data.length];
		entry.getBytes(16, dst);
		
		System.out.println("------------- PUT -------------");
		System.out.println("Ledger ID: " + ledgerId);
		System.out.println("Entry ID: " + entryId);
		System.out.println("Entry Data: " + new String(dst, StandardCharsets.UTF_8));
		
		boolean result = writeCache.put(ledgerId, entryId, entry);
		if (!result) {
			throw new IndexOutOfBoundsException();
		}
		
		long countAfter = writeCache.count();
		
		System.out.println("\n------------ RESULT ------------");
		System.out.println("Count before: " + countBefore);
		System.out.println("Count after: " + countAfter);
		
		assertTrue(countAfter > countBefore);
	}
	
	@Test
	public void getTest() {
		
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
			System.out.println("Exception raised: " + expectedException.getName());
		}
		
		byte[] data = ("ledger-" + ledgerId + "-" + entryId).getBytes();	
		byte[] dst = new byte[data.length];
		entry.getBytes(16, dst);
		
		System.out.println("------------- PUT -------------");
		System.out.println("Ledger ID: " + ledgerId);
		System.out.println("Entry ID: " + entryId);
		System.out.println("Entry Data: " + new String(dst, StandardCharsets.UTF_8));
		
		boolean result = writeCache.put(ledgerId, entryId, entry);
		if (!result) {
			throw new IndexOutOfBoundsException();
		}
		
		System.out.println("\n------------ GET ------------");
		
		ByteBuf bufGet = writeCache.get(ledgerId, entryId);
		byte[] dstGet = new byte[data.length];
		bufGet.getBytes(16, dstGet);
		
		String expected = new String(dst, StandardCharsets.UTF_8);
		String actual = new String(dstGet, StandardCharsets.UTF_8);
		
		System.out.println("Data get: " + actual);
		
		System.out.println("\n----------- RESULT -----------");
		System.out.println("Expected: " + expected);
		System.out.println("Actual: " + actual);
		
		assertEquals(expected, actual);
	}
}
