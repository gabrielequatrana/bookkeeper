package org.apache.bookkeeper.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.bookie.storage.ldb.WriteCache;
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

@RunWith(Parameterized.class)
public class WriteCacheTest {

	private static final int ENTRY_SIZE = 1024;
	private static final int MAX_ENTRIES = 10;
	private static final int CACHE_SIZE = ENTRY_SIZE * MAX_ENTRIES;
	
	private WriteCache writeCache;
	
	private long ledgerId;
	private long entryId;
	private ByteBuf entry;
	private Class<? extends Exception> expectedException;
	
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();
	
	public WriteCacheTest(long ledgerId, long entryId, Class<? extends Exception> expectedException) {
		this.ledgerId = ledgerId;
		this.entryId = entryId;
		this.entry = TestUtil.generateEntry(ledgerId, entryId);
		this.expectedException = expectedException;
	}
	
	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			{ 1L, 1L, null }, 
			{ 0L, 0L, null },
			{ -1L, 0L, IllegalArgumentException.class },
			{ 0L, -1L, IllegalArgumentException.class }
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
		long countBefore = writeCache.count();
		byte[] data = ("ledger-" + ledgerId + "-" + entryId).getBytes();	
		byte[] dst = new byte[data.length];
		entry.getBytes(16, dst);
		
		System.out.println("------------- PUT -------------");
		
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
			System.out.println("Exception raised: " + expectedException.getName());
		}
		
		System.out.println("Ledger ID: " + ledgerId);
		System.out.println("Entry ID: " + entryId);
		System.out.println("Entry Data: " + new String(dst, StandardCharsets.UTF_8));
		
		writeCache.put(ledgerId, entryId, entry);
		
		long countAfter = writeCache.count();
		
		System.out.println("\n------------ RESULT ------------");
		System.out.println("Count before: " + countBefore);
		System.out.println("Count after: " + countAfter);
		
		assertTrue(countAfter > countBefore);
	}
	
	@Test
	public void getTest() {
		byte[] data = ("ledger-" + ledgerId + "-" + entryId).getBytes();	
		byte[] dst = new byte[data.length];
		entry.getBytes(16, dst);
		
		System.out.println("------------- PUT -------------");
		
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
			System.out.println("Exception raised: " + expectedException.getName());
		}
		
		System.out.println("Ledger ID: " + ledgerId);
		System.out.println("Entry ID: " + entryId);
		System.out.println("Entry Data: " + new String(dst, StandardCharsets.UTF_8));
		
		writeCache.put(ledgerId, entryId, entry);
		
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
