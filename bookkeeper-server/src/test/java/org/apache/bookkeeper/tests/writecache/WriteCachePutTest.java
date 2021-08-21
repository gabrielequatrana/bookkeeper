package org.apache.bookkeeper.tests.writecache;

import static org.junit.Assert.assertEquals;

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

@RunWith(Parameterized.class)
public class WriteCachePutTest {
	
	// WriteCache instance
	private WriteCache writeCache;

	// Cache configuration
	private static final int ENTRY_SIZE = 1024;
	private static final int MAX_ENTRIES = 10;
	private static final int CACHE_SIZE = ENTRY_SIZE * MAX_ENTRIES;
	private static final int SEGMENT_SIZE = 4096;

	// Test parameters
	private long ledgerId;
	private long entryId;
	private ByteBuf entry;
	private Class<? extends Exception> expectedException;
	
	// Test environment
	private enum Type {TYPE1, TYPE2, TYPE3}
	private Type type;

	// Rule to manage expected exception
	@Rule public ExpectedException exceptionRule = ExpectedException.none();

	public WriteCachePutTest(long ledgerId, long entryId, ByteBuf entry, Type type, Class<? extends Exception> expectedException) {
		this.ledgerId = ledgerId;
		this.entryId = entryId;
		this.entry = entry;
		this.type = type;
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			
			// Minimal test suite
			{ 1L, 1L, null, Type.TYPE1, NullPointerException.class },
			{ -1L, 0L, TestUtil.generateEntry(CACHE_SIZE + 1), Type.TYPE1, IndexOutOfBoundsException.class },
			{ 0L, -1L, TestUtil.generateEntry(ENTRY_SIZE), Type.TYPE1, IllegalArgumentException.class },
			
			// Added after the improvement of the test suite (No coverage improvement)
			{ 0L, 0L, TestUtil.generateEntry(ENTRY_SIZE), Type.TYPE1, null },
			{ -1L, 0L, TestUtil.generateEntry(ENTRY_SIZE), Type.TYPE1, IllegalArgumentException.class },
			
			//Added after the improvement of the test suite (Coverage improvement)
			{ 1L, 1L, TestUtil.generateEntry(ENTRY_SIZE), Type.TYPE2, null },		// Branch line 167
			{ 1L, 1L, TestUtil.generateEntry(SEGMENT_SIZE/2+1), Type.TYPE3, null },	// Branch line 150
			
			// Added after mutation testing
			{ 1L, 1L, TestUtil.generateEntry(SEGMENT_SIZE), Type.TYPE1, null },
			{ 1L, 1L, TestUtil.generateEntry(0), Type.TYPE1, null },
		});
	}

	// Setup the test environment
	@Before
	public void setUp() {
		writeCache = new WriteCache(ByteBufAllocator.DEFAULT, CACHE_SIZE, SEGMENT_SIZE);
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
		}
	}
	
	// Cleanup the test environment
	@After
	public void cleanUp() {
		writeCache.clear();
		writeCache.close();
	}
	
	@Test
	public void putTest() {
		
		long numEntries;
		
		if (type == Type.TYPE1) {

			// Add the entry to the cache
			boolean result = writeCache.put(ledgerId, entryId, entry);
			if (!result) {
				// Entry was not added to the cache
				throw new IndexOutOfBoundsException();
			}
	
			numEntries = 1;
		}
		
		else if (type == Type.TYPE2) {
			writeCache.put(ledgerId, entryId + 1L, entry);
			writeCache.put(ledgerId, entryId, entry);
			
			numEntries = 2;
		}
		
		else {
			writeCache.put(ledgerId, entryId, entry);
			writeCache.put(ledgerId, entryId+1, entry);
			
			numEntries = 2;
		}
		
		// Assert that the cache has one entry
		assertEquals(numEntries, writeCache.count());
					
		// Assert that the size of the entry is equal to the size of the cache
		assertEquals(numEntries*entry.capacity(), writeCache.size());
	}
}
