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
public class WriteCacheGetLastEntryTest {

	// WriteCache instance
	private WriteCache writeCache;

	// Cache configuration
	private static final int ENTRY_SIZE = 1024;
	private static final int MAX_ENTRIES = 10;
	private static final int CACHE_SIZE = ENTRY_SIZE * MAX_ENTRIES;

	// Test parameters
	private long ledgerId;
	private Class<? extends Exception> expectedException;
	
	// Test environment
	private int numEntries = 4;
	private ByteBuf expectedEntry;

	// Rule to manage expected exception
	@Rule public ExpectedException exceptionRule = ExpectedException.none();

	public WriteCacheGetLastEntryTest(long ledgerId, Class<? extends Exception> expectedException) {
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

	// Setup the test environment
	@Before
	public void setUp() {	
		writeCache = new WriteCache(ByteBufAllocator.DEFAULT, CACHE_SIZE);
		
		// Add  entries to the cache
		ByteBuf entry = null;
		for (long i = 0; i <= numEntries; i++) {
			entry = TestUtil.generateEntry(ENTRY_SIZE);
			writeCache.put(1L, i, entry);
		}
		
		expectedEntry = entry;
		if (ledgerId != 1) {
			expectedEntry = null;
		}
		
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
	public void getLastEntryTest() {
			
		// Retrieve last entry from the cache
		ByteBuf actualEntry = writeCache.getLastEntry(ledgerId);

		// Assert that the last entry is the same as the retrieved entry from the cache
		assertEquals(expectedEntry, actualEntry);
	}
}
