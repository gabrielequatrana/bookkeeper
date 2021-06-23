package org.apache.bookkeeper.tests.util;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.Enumeration;

import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.conf.ServerConfiguration;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class TestUtil {

	public static ServerConfiguration getConfiguration() throws SocketException {
		ServerConfiguration conf = new ServerConfiguration();
		conf.setTLSEnabledProtocols("TLSv1.2,TLSv1.1");
		conf.setJournalFlushWhenQueueEmpty(true);
		conf.setJournalFormatVersionToWrite(5);
		conf.setAllowEphemeralPorts(true);
		conf.setBookiePort(0);
		conf.setGcWaitTime(1000);
		conf.setDiskUsageThreshold(0.999f);
		conf.setDiskUsageWarnThreshold(0.99f);
		conf.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
		conf.setProperty(DbLedgerStorage.WRITE_CACHE_MAX_SIZE_MB, 4);
		conf.setProperty(DbLedgerStorage.READ_AHEAD_CACHE_MAX_SIZE_MB, 4);
		conf.setListeningInterface(getLoopbackInterfaceName());
		conf.setAllowLoopback(true);
		return conf;
	}

	private static String getLoopbackInterfaceName() throws SocketException {
		Enumeration<NetworkInterface> nifs = NetworkInterface.getNetworkInterfaces();
		for (NetworkInterface nif : Collections.list(nifs)) {
			if (nif.isLoopback()) {
				return nif.getName();
			}
		}

		return null;
	}
	
	public static ByteBuf generateEntry(long ledgerId, long entryId) {
		byte[] data = ("ledger-" + ledgerId + "-" + entryId).getBytes();
		ByteBuf buf = Unpooled.buffer(8 + 8 + data.length);
		buf.writeLong(ledgerId);
		buf.writeLong(entryId);
		buf.writeBytes(data);
		return buf;
	}
}
