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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

@RunWith(Parameterized.class)
public class BookieTest {
	
	private static final int ENTRY_SIZE = 1024;
	private static final int MAX_ENTRIES = 10;
	private static final int CACHE_SIZE = ENTRY_SIZE * MAX_ENTRIES;

	private Bookie bookie;
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
			{ Unpooled.wrappedBuffer(new byte[ENTRY_SIZE]), false, null, null, null } 
		});
	}
	
	@Before
	public void setUp() throws IOException, InterruptedException, BookieException {
		ServerConfiguration conf = TestConfiguration.getConfiguration();
		bookie = new BookieImpl(conf);
	}
	
	@Test
	public void test() throws IOException, BookieException, InterruptedException {
		bookie.addEntry(entry, ackBeforeSync, cb, ctx, masterKey);
		assertEquals(2,2);
	}
}
