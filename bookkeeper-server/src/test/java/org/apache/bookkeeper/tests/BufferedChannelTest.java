package org.apache.bookkeeper.tests;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.apache.bookkeeper.bookie.BufferedChannel;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

@RunWith(Parameterized.class)
public class BufferedChannelTest {
	
	private static final String DIR = "tmpDir";
	private static final String FILE = "tmpFile";

	private BufferedChannel bufferedChannel;
	private FileChannel fc;
	private int capacity;
	
	private int fileSize;
	private int startIndex;
	private int readLength;
	private byte[] bytes;
	
	public BufferedChannelTest(int capacity, int fileSize, int startIndex, int readLength) {
		this.capacity = capacity;
		this.fileSize = fileSize;
		this.startIndex = startIndex;
		this.readLength = readLength;
	}
	
	@Parameters
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(new Object[][] {
			{ 3, 3, 1, 2 },
			//{ 0, 0, 0, -1 }
		});
	}
	
	@BeforeClass
	public static void setUp() {
		if (!Files.exists(Paths.get(DIR))) {
			File dir = new File(DIR);
			dir.mkdir();
		}
	}
	
	@Before
	public void configure() throws IOException {
		generateFile(fileSize);
		Path path = Paths.get(DIR, FILE);
		fc = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
		fc.position(fc.size());
	}
	
	@AfterClass
	public static void teardown() {
		File dir = new File(DIR);
		String[] entries = dir.list();
		for (String string : entries) {
			File file = new File(dir.getPath(), string);
			file.delete();
		}
		dir.delete();
	}
	
	@After
	public void cleanUp() throws IOException {
		bufferedChannel.clear();
		bufferedChannel.close();
		fc.close();
	}
	
	@Test
	public void readTest() throws Exception {
		bufferedChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, capacity);
		ByteBuf buf = Unpooled.buffer();
		buf.capacity(readLength);
		
		int numBytesRead = bufferedChannel.read(buf, startIndex, readLength);
		System.out.println("Bytes read: " + numBytesRead);
		
		byte[] bytesRead = buf.array();
		
		int numBytesExpected = 0;
		if (fileSize - startIndex >= readLength) {
			numBytesExpected = readLength;
		}
		else {
			numBytesExpected = bytes.length - startIndex - readLength;
		}
		
		byte[] expectedBytes = Arrays.copyOfRange(bytes, startIndex, startIndex + numBytesExpected);
		
		assertEquals(Arrays.toString(expectedBytes), Arrays.toString(bytesRead));
	}
	
	private void generateFile(int size) throws IOException {
		bytes = new byte[size];
		Random rand = new Random();
		rand.nextBytes(bytes);
		
		FileOutputStream stream = new FileOutputStream(DIR + "/" + FILE);
		stream.write(bytes);
		stream.close();
	}
}
