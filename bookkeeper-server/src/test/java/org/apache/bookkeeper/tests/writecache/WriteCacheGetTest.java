package org.apache.bookkeeper.tests.writecache;

import static org.junit.Assert.assertEquals;

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

@RunWith(Parameterized.class)
public class WriteCacheGetTest {

	// WriteCache instance
	private WriteCache writeCache;

	// Cache configuration
	private static final int ENTRY_SIZE = 1024;
	private static final int MAX_ENTRIES = 10;
	private static final int CACHE_SIZE = ENTRY_SIZE * MAX_ENTRIES;

	// Test parameters
	private long ledgerId;
	private long entryId;
	private Class<? extends Exception> expectedException;
	
	// Test environment
	private ByteBuf entry;

	// Rule to manage expected exception
	@Rule public ExpectedException exceptionRule = ExpectedException.none();

	public WriteCacheGetTest(long ledgerId, long entryId, Class<? extends Exception> expectedException) {
		this.ledgerId = ledgerId;
		this.entryId = entryId;
		this.expectedException = expectedException;
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			
			// Minimal test suite
			{ 1L, 1L, null },
			{ -1L, 0L, IllegalArgumentException.class },
			{ 0L, -1L, IllegalArgumentException.class },
			
			// Added after the improvement of the test suite
			//{ 0L, 0L, null }
		});
	}

	// Setup the test environment
	@Before
	public void setUp() {	
		writeCache = new WriteCache(ByteBufAllocator.DEFAULT, CACHE_SIZE);
		entry = TestUtil.generateEntry(ENTRY_SIZE);
		
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
	public void getTest() {

		// Get entry data
		byte[] dst = new byte[ENTRY_SIZE-16];
		entry.getBytes(16, dst);

		// Add the entry to the cache
		boolean result = writeCache.put(ledgerId, entryId, entry);
		if (!result) {
			// Entry was not added to the cache
			throw new IndexOutOfBoundsException();
		}

		// Retrieve the entry from the cache
		ByteBuf bufGet = writeCache.get(ledgerId, entryId);
		byte[] dstGet = new byte[ENTRY_SIZE-16];
		bufGet.getBytes(16, dstGet);
		
		// Convert data into string
		String expected = new String(dst, StandardCharsets.UTF_8);
		String actual = new String(dstGet, StandardCharsets.UTF_8);

		// Assert that the added entry is the same as the retrieved entry from the cache
		assertEquals(expected, actual);
	}
}
