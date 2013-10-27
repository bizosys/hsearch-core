package com.bizosys.hsearch.byteutils;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.oneline.ferrari.TestAll;

public class SortedBytesBitsetTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
		public static String mode = modes[2];  
		
		public static void main(String[] args) throws Exception {
			SortedBytesBitsetTest t = new SortedBytesBitsetTest();
			
			if ( modes[0].equals(mode) ) {
				TestAll.run(new TestCase[]{t});
			} else if  ( modes[1].equals(mode) ) {
		        TestFerrari.testRandom(t);
		        
			} else if  ( modes[2].equals(mode) ) {
				t.setUp();
				t.testSingleBitset();
				t.tearDown();
			}
		}

		@Override
		protected void setUp() throws Exception {
		}
		
		@Override
		protected void tearDown() throws Exception {
		}
		
		public void testSanity() throws Exception {	
			List<BitSetWrapper> sortedList = new ArrayList<BitSetWrapper>();
			int MAX = 5;
			for ( int i = 0; i<MAX; i++) {
				BitSetWrapper b = new BitSetWrapper();
				b.set(i * 10);
				sortedList.add(b);
			}

			ISortedByte<BitSetWrapper> ser = SortedBytesBitset.getInstance();
			byte[] bytes = ser.toBytes(sortedList);
			System.out.println( "Bits :" + bytes.length);

			ISortedByte<BitSetWrapper> deser = SortedBytesBitset.getInstance();
			deser.parse(bytes);
			
			for ( int i = 0; i<MAX; i++) {
				BitSetWrapper b = deser.getValueAt(i);
				assertTrue(b.get(i * 10));
				assertFalse(b.get((i * 10) + 1));
			}
		}
		
		public void testEquality() throws Exception {	
			List<BitSetWrapper> sortedList = new ArrayList<BitSetWrapper>();
			int MAX = 10;
			BitSetWrapper b = new BitSetWrapper();

			for ( int i = 0; i<MAX; i++) {
				b.set(i);
			}
			sortedList.add(b);
			BitSetWrapper c = new BitSetWrapper();
			c.set(10);
			sortedList.add(c);

			ISortedByte<BitSetWrapper> ser = SortedBytesBitset.getInstance();
			byte[] bytes = ser.toBytes(sortedList);
			System.out.println( "Bits :" + bytes.length);

			ISortedByte<BitSetWrapper> deser = SortedBytesBitset.getInstance();
			deser.parse(bytes);
			assertEquals(2, deser.getSize());
			assertEquals(1, deser.getEqualToIndex(c));
			List<BitSetWrapper> vals = new ArrayList<BitSetWrapper>();
			deser.addAll(vals);
			assertEquals(2, vals.size());
			assertTrue(vals.get(1).get(10));
		}
		
		public void testSingleBitset() throws Exception {
			BitSetWrapper b = new BitSetWrapper();
			b.set(0);
			b.set(100);
			b.set(1000);
			b.set(10000);
			
			SortedBytesBitset sbt = SortedBytesBitset.getInstanceBitset();
			byte[] data = sbt.bitSetToBytes(b);
			BitSetWrapper c = sbt.bytesToBitSet(data, 0, data.length);
			assertTrue(c.get(1000));
		}
}
