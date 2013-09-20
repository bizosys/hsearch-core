package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.oneline.ferrari.TestAll;

public class SortedBytesBitsetTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
		public static String mode = modes[1];  
		
		public static void main(String[] args) throws Exception {
			SortedBytesBitsetTest t = new SortedBytesBitsetTest();
			
			if ( modes[0].equals(mode) ) {
				TestAll.run(new TestCase[]{t});
			} else if  ( modes[1].equals(mode) ) {
		        TestFerrari.testRandom(t);
		        
			} else if  ( modes[2].equals(mode) ) {
				t.setUp();
				t.testSanity();
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
			List<BitSet> sortedList = new ArrayList<BitSet>();
			int MAX = 10;
			for ( int i=0; i<MAX; i++) {
				BitSet b = new BitSet(i*10000000);
				b.set(i);
				//b.set(i*10000000);
				sortedList.add(b);
			}

			ISortedByte<BitSet> ser = SortedBytesBitset.getInstance();
			byte[] bytes = ser.toBytes(sortedList);
			System.out.println( "Bits :" + bytes.length);

			ISortedByte<BitSet> deser = SortedBytesBitset.getInstance();
			deser.parse(bytes);
			
			for ( int i=0; i<MAX; i++) {
				BitSet b = deser.getValueAt(i);
				System.out.println( b.size() + "\t" + b.get(i) + "\t" + b.get(i*100) + "\t" + b.cardinality());
			}
		}
}
