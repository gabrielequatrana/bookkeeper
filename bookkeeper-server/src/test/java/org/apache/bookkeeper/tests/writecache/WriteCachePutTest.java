package org.apache.bookkeeper.tests.writecache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

	// Test parameters
	private long ledgerId;
	private long entryId;
	private ByteBuf entry;
	private Class<? extends Exception> expectedException;

	// Rule to manage expected exception
	@Rule public ExpectedException exceptionRule = ExpectedException.none();

	public WriteCachePutTest(long ledgerId, long entryId, ByteBuf entry, Class<? extends Exception> expectedException) {
		this.ledgerId = ledgerId;
		this.entryId = entryId;
		this.entry = entry;
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			
			// Minimal test suite
			{ 1L, 1L, null, NullPointerException.class },
			{ -1L, 0L, TestUtil.generateEntry(CACHE_SIZE + 1), IndexOutOfBoundsException.class },
			{ 0L, -1L, TestUtil.generateEntry(ENTRY_SIZE), IllegalArgumentException.class },
			
			// Added after the improvement of the test suite
			{ 0L, 0L, TestUtil.generateEntry(ENTRY_SIZE), null },
			{ -1L, 0L, TestUtil.generateEntry(ENTRY_SIZE), IllegalArgumentException.class },
		
			// Added after mutation testing
			//{ 3L, 5L, TestUtil.generateEntry(CACHE_SIZE), null },
			//{ 1L, 2L, TestUtil.generateEntry(0), null },
		});
	}

	// Setup the test environment
	@Before
	public void setUp() {
		writeCache = new WriteCache(ByteBufAllocator.DEFAULT, CACHE_SIZE);
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

		// Get free space before adding a new entry
		long spaceBefore = writeCache.count();

		// Add the entry to the cache
		boolean result = writeCache.put(ledgerId, entryId, entry);
		if (!result) {
			// Entry was not added to the cache
			throw new IndexOutOfBoundsException();
		}

		// Get free space after adding the entry
		long spaceAfter = writeCache.count();

		// Assert that the free space is less than before adding the entry
		assertTrue(spaceBefore < spaceAfter);
		
		// Assert that the size of the entry is equal to the size of the cache
		assertEquals(entry.capacity(), writeCache.size());
	}
}
