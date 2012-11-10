package com.bizosys.hsearch.byteutils;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.byteutils.ByteArrays.ArrayBytes;
import com.google.protobuf.ByteString;
import com.oneline.ferrari.TestAll;

public class ByteStringTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
		public static String mode = modes[1];  
		
		public static void main(String[] args) throws Exception {
			ByteStringTest t = new ByteStringTest();
			
			if ( modes[0].equals(mode) ) {
				TestAll.run(new TestCase[]{t});
			} else if  ( modes[1].equals(mode) ) {
		        TestFerrari.testRandom(t);
		        
			} else if  ( modes[2].equals(mode) ) {
				t.setUp();
				t.testNoExtraction();
				t.tearDown();
			}
		}

		@Override
		protected void setUp() throws Exception {
		}
		
		@Override
		protected void tearDown() throws Exception {
		}
		

		public void testNoExtraction() throws Exception {	
			
			List<byte[]> bytesA = new ArrayList<byte[]>();
			float mem = Runtime.getRuntime().freeMemory()/1024/1024;
			System.out.println("Start :" + mem);
			for ( int i=0; i<50000; i++) {
				bytesA.add(new byte[1024]);
			}
			mem = Runtime.getRuntime().freeMemory()/1024/1024;
			System.out.println("copyFrom :" + mem);
			byte[] outB = SortedBytesArray.getInstance().toBytes(bytesA);
			
			mem = Runtime.getRuntime().freeMemory()/1024/1024;
			System.out.println("toByteArray :" + mem);
			
			SortedBytesArray.getInstance().parse(outB).getValueAt(0);
			mem = Runtime.getRuntime().freeMemory()/1024/1024;			
			System.out.println("getVal :" + mem);
		}	
		
		
		public void testExtraction() throws Exception {	
			ByteArrays.ArrayBytes.Builder builder = ByteArrays.ArrayBytes.newBuilder();
			float mem = Runtime.getRuntime().freeMemory()/1024/1024;
			System.out.println("Start :" + mem);
			for ( int i=0; i<50000; i++) {
				builder.addVal( ByteString.copyFrom(new byte[1024]));
			}
			mem = Runtime.getRuntime().freeMemory()/1024/1024;
			System.out.println("copyFrom :" + mem);
			byte[] outB = builder.build().toByteArray();
			
			mem = Runtime.getRuntime().freeMemory()/1024/1024;
			System.out.println("toByteArray :" + mem);
			
			ArrayBytes ar = ByteArrays.ArrayBytes.parseFrom(outB);
			mem = Runtime.getRuntime().freeMemory()/1024/1024;
			System.out.println("parseFrom :" + mem);
			
			ar.getVal(0);
			mem = Runtime.getRuntime().freeMemory()/1024/1024;			
			System.out.println("getVal :" + mem);

			ar.getValList();
			mem = Runtime.getRuntime().freeMemory()/1024/1024;
			System.out.println("getValList :" + mem);
			
		}
				
}
