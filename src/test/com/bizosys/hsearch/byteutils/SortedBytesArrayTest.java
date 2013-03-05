package com.bizosys.hsearch.byteutils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.oneline.ferrari.TestAll;

public class SortedBytesArrayTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
		public static String mode = modes[2];  
		
		public static void main(String[] args) throws Exception {
			SortedBytesArrayTest t = new SortedBytesArrayTest();
			
			if ( modes[0].equals(mode) ) {
				TestAll.run(new TestCase[]{t});
			} else if  ( modes[1].equals(mode) ) {
		        TestFerrari.testRandom(t);
		        
			} else if  ( modes[2].equals(mode) ) {
				t.setUp();
				t.testIndexOf();
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
			List<byte[]> sortedList = new ArrayList<byte[]>();
			
			for ( int i=0; i<1000; i++) {
				sortedList.add( Storable.putLong(i));
			}
			ISortedByte<byte[]> instance = SortedBytesArray.getInstance();
			byte[] bytes = instance.toBytes(sortedList);
			instance.parse(bytes);
			List<byte[]> reclaimed = new ArrayList<byte[]>();
			instance.addAll(reclaimed);
			
			int val = 0;
			for (byte[] bs : reclaimed) {
				assertEquals( val, Storable.getLong(0, bs));
				val++;
			}
		}
		
		public void testIndexOf() throws Exception {	
			List<byte[]> sortedList = new ArrayList<byte[]>();
			
			for ( int i=0; i<1000; i++) {
				sortedList.add( Storable.putLong(i));
			}
			
			byte[] bytes = SortedBytesArray.getInstance().toBytes(sortedList);
			int index = SortedBytesArray.getInstance().parse(bytes).getEqualToIndex(Storable.putLong(23L));
			assertEquals(23, index);
		}		

		public void testValueAtBoundaries() throws Exception {	
			List<byte[]> sortedList = new ArrayList<byte[]>();
			
			for ( int i=0; i<1000; i++) {
				sortedList.add( Storable.putLong(i));
			}
			
			ISortedByte<byte[]> instance = SortedBytesArray.getInstance();
			byte[] bytes = instance.toBytes(sortedList);
			instance.parse(bytes);
			
			assertEquals( 0, Storable.getLong(0, instance.parse(bytes).getValueAt(0)));
			assertEquals( 999, Storable.getLong(0, instance.parse(bytes).getValueAt(999)));
			assertEquals( 499, Storable.getLong(0, instance.parse(bytes).getValueAt(499)));
			
			try {
				instance.parse(bytes).getValueAt(1000);
			} catch (IOException ex) {
				assertEquals("Maximum position in array is 1000 and accessed 1000", ex.getMessage());
			}
			
			try {
				instance.parse(bytes).getValueAt(-1);
			} catch (Exception ex) {
				assertTrue(ex instanceof NegativeArraySizeException);
			}			
			
		}

}
