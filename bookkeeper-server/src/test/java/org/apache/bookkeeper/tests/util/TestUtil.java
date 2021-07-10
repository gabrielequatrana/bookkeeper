package org.apache.bookkeeper.tests.util;

import java.io.File;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.Enumeration;

import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.conf.ServerConfiguration;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class TestUtil {
	
	private TestUtil() {
		
	}

	public static ServerConfiguration getConfiguration(File journalDir, File ledgerDir) throws SocketException {
		float usage = 1.0f - ((float) ledgerDir.getUsableSpace()) / ledgerDir.getTotalSpace();
		
		ServerConfiguration confReturn = new ServerConfiguration();
		confReturn.setTLSEnabledProtocols("TLSv1.2,TLSv1.1");
		confReturn.setJournalFlushWhenQueueEmpty(true);
		confReturn.setJournalFormatVersionToWrite(5);
		confReturn.setAllowEphemeralPorts(true);
		confReturn.setBookiePort(0);
		confReturn.setGcWaitTime(1000);
		confReturn.setDiskUsageThreshold(0.999f);
		confReturn.setDiskUsageWarnThreshold(0.99f);
		confReturn.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
		confReturn.setProperty(DbLedgerStorage.WRITE_CACHE_MAX_SIZE_MB, 4);
		confReturn.setProperty(DbLedgerStorage.READ_AHEAD_CACHE_MAX_SIZE_MB, 4);
		confReturn.setListeningInterface(getLoopbackInterfaceName());
		confReturn.setAllowLoopback(true);
		confReturn.setJournalDirsName(new String[] { journalDir.getAbsolutePath() });
		confReturn.setLedgerDirNames(new String[] { ledgerDir.getAbsolutePath() });
		confReturn.setMetadataServiceUri(null);
		confReturn.setDiskUsageThreshold(usage / 2);
		confReturn.setDiskUsageWarnThreshold(usage / 3);
		confReturn.setMinUsableSizeForEntryLogCreation(Long.MIN_VALUE);
		confReturn.setLedgerStorageClass(InterleavedLedgerStorage.class.getName());
		return confReturn;
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
	
	public static ByteBuf generateEntry(int entrySize) {
		return Unpooled.wrappedBuffer(new byte[entrySize]);
	}
	
	public static ByteBuf validEntry() {
		return generateEntry(1L, 1L);
	}
	
	public static ByteBuf invalidEntry() {
		return generateEntry(-1L, -1L);
	}
}
