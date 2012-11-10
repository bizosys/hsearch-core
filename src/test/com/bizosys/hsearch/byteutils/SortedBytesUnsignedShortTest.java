package com.bizosys.hsearch.byteutils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.oneline.ferrari.TestAll;

public class SortedBytesUnsignedShortTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
		public static String mode = modes[2];  
		
		public static void main(String[] args) throws Exception {
			SortedBytesUnsignedShortTest t = new SortedBytesUnsignedShortTest();
			
			if ( modes[0].equals(mode) ) {
				TestAll.run(new TestCase[]{t});
			} else if  ( modes[1].equals(mode) ) {
		        TestFerrari.testRandom(t);
		        
			} else if  ( modes[2].equals(mode) ) {
				t.setUp();
				t.testLessthan();
				t.tearDown();
			}
		}

		@Override
		protected void setUp() throws Exception {
		}
		
		@Override
		protected void tearDown() throws Exception {
		}
		
		public void testEqual() throws Exception {	
			List<Integer> sortedList = new ArrayList<Integer>();
			sortedList.add(new Integer(0));
			sortedList.add(new Integer(2));
			sortedList.add(new Integer(12));
			sortedList.add(new Integer(10));
			sortedList.add(new Integer(10));
			sortedList.add(new Integer(15));
			sortedList.add(new Integer(18));
			
			for ( int i=0; i<1000; i++) {
				sortedList.add(i);
			}
			Collections.sort(sortedList);
			
			byte[] bytes = SortedBytesUnsignedShort.getInstance().toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>();   
			SortedBytesUnsignedShort.getInstance().parse(bytes).getEqualToIndexes(10, positions);
			
			assertNotNull(positions);
			assertEquals(3, positions.size());
			for (int pos : positions) {
				assertEquals(10 , SortedBytesUnsignedShort.getInstance().parse(bytes).getValueAt(pos).intValue());
			}
		}
		
		public void testLessthan() throws Exception {	
			List<Integer> sortedList = new ArrayList<Integer>();
			for ( int i=0; i<1000; i++) {
				sortedList.add(i);
			}
			Collections.sort(sortedList);

			
			byte[] bytes = SortedBytesUnsignedShort.getInstance().toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>();   
			SortedBytesUnsignedShort.getInstance().parse(bytes).getLessThanIndexes(130, positions);
			
			assertNotNull(positions);
			assertTrue(!positions.contains(130) );
			assertTrue(!positions.contains(999) );
			assertTrue(positions.contains(128) );
			assertTrue(positions.contains(0) );
			assertTrue(!positions.contains(-1) );
			
			assertEquals(130, positions.size());
			
			for (int pos : positions) {
				assertTrue( (SortedBytesUnsignedShort.getInstance().parse(bytes).getValueAt(pos) < 130) );
			}
		}		
		
		public void testLessthanMultiValue() throws Exception {	
			List<Integer> sortedList = new ArrayList<Integer>();
			sortedList.add(new Integer(0));
			sortedList.add(new Integer(5));
			sortedList.add(new Integer(2));
			sortedList.add(new Integer(12));
			sortedList.add(new Integer(10));
			sortedList.add(new Integer(10));
			sortedList.add(new Integer(15));
			sortedList.add(new Integer(18));
			
			for ( int i=0; i<1000; i++) {
				sortedList.add(i);
			}
			Collections.sort(sortedList);

			
			byte[] bytes = SortedBytesUnsignedShort.getInstance().toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>();   
			SortedBytesUnsignedShort.getInstance().parse(bytes).getLessThanIndexes(10, positions);
			
			assertNotNull(positions);
			assertEquals(13, positions.size());
			
			for (int pos : positions) {
				assertTrue( (SortedBytesUnsignedShort.getInstance().parse(bytes).getValueAt(pos) < 10) );
			}
		}				
		
		public void testLessthanEqual() throws Exception {	
			List<Integer> sortedList = new ArrayList<Integer>();
			
			for ( int i=0; i<1000; i++) {
				sortedList.add(i);
			}
			Collections.sort(sortedList);

			byte[] bytes = SortedBytesUnsignedShort.getInstance().toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>();  
			SortedBytesUnsignedShort.getInstance().parse(bytes).getLessThanEqualToIndexes(700, positions);
			
			assertNotNull(positions);
			assertEquals(701, positions.size());
			for (int pos : positions) {
				assertTrue( (SortedBytesUnsignedShort.getInstance().parse(bytes).getValueAt(pos) <= 700) );
			}
		}			
		
		public void testGreaterthan() throws Exception {	
			List<Integer> sortedList = new ArrayList<Integer>();
			for ( int i=0; i<1000; i++) {
				sortedList.add(i);
			}
			Collections.sort(sortedList);

			
			byte[] bytes = SortedBytesUnsignedShort.getInstance().toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>();   
			SortedBytesUnsignedShort.getInstance().parse(bytes).getGreaterThanEqualToIndexes(121, positions);
			
			assertNotNull(positions);
			assertTrue(positions.contains(498) );
			assertTrue(positions.contains(499) );
			assertTrue(positions.contains(500) );
			assertTrue(positions.contains(999) );
			assertTrue(positions.contains(121) );
			assertTrue(!positions.contains(120) );
			assertEquals(1000-121, positions.size());
			for (int pos : positions) {
				assertTrue( (SortedBytesUnsignedShort.getInstance().parse(bytes).getValueAt(pos) >= 121) );
			}
		}		
		
		public void testGreaterthanEqual() throws Exception {	
			List<Integer> sortedList = new ArrayList<Integer>();
			for ( int i=0; i<1000; i++) {
				sortedList.add(i);
			}
			
			Collections.sort(sortedList);

			byte[] bytes = SortedBytesUnsignedShort.getInstance().toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>(); 
			SortedBytesUnsignedShort.getInstance().parse(bytes).getGreaterThanIndexes(800, positions);
			
			assertNotNull(positions);
			assertTrue(positions.contains(801) );
			assertTrue(positions.contains(999));
			assertTrue(!positions.contains(800));
			assertEquals(1000-800-1, positions.size());
			for (int pos : positions) {
				assertTrue( (SortedBytesUnsignedShort.getInstance().parse(bytes).getValueAt(pos) > 800) );
			}
		}		
		
		public void testPresetMinimumValue() throws Exception {	
			List<Integer> sortedList = new ArrayList<Integer>();
			for ( int i=-400; i<1000; i++) {
				sortedList.add(i);
			}
			
			Collections.sort(sortedList);

			SortedBytesUnsignedShort sortedBytes = SortedBytesUnsignedShort.getInstanceShort();
			sortedBytes = sortedBytes.setMinimumValueLimit( (short) -400);
			

			byte[] bytes = sortedBytes.toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>();  
			sortedBytes.parse(bytes).getLessThanEqualToIndexes(-398, positions);
			
			assertNotNull(positions);
			assertEquals(3, positions.size());
			for (int pos : positions) {
				//System.out.println(sortedBytes.getValueAt(bytes,pos));
				assertTrue( (sortedBytes.parse(bytes).getValueAt(pos) <= -398) );
			}
			
		}			
		
		public void testRangeValuesInclusive() throws Exception {	
			List<Integer> sortedList = new ArrayList<Integer>();
			for ( int i=400; i<1000; i++) {
				sortedList.add(i);
			}
			
			Collections.sort(sortedList);

			SortedBytesUnsignedShort sortedBytes = SortedBytesUnsignedShort.getInstanceShort().setMinimumValueLimit((short)-100);
			
			byte[] bytes = sortedBytes.toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>();  
			sortedBytes.parse(bytes).getRangeIndexesInclusive(550, 560, positions);
			
			assertNotNull(positions);
			assertEquals(11, positions.size());
			for (int pos : positions) {
				//System.out.println(sortedBytes.getValueAt(bytes,pos)); 
				assertTrue( (sortedBytes.parse(bytes).getValueAt(pos) >= 550 && sortedBytes.parse(bytes).getValueAt(pos) <= 560) );
			}
		}		
		
		public void testRangeValuesExclusive() throws Exception {	
			List<Integer> sortedList = new ArrayList<Integer>();
			for ( int i=400; i<1000; i++) {
				sortedList.add(i);
			}
			
			Collections.sort(sortedList);

			SortedBytesUnsignedShort sortedBytes = SortedBytesUnsignedShort.getInstanceShort();
			
			byte[] bytes = sortedBytes.toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>();  
			sortedBytes.parse(bytes).getRangeIndexes(550, 560, positions);
			
			assertNotNull(positions);
			assertEquals(9, positions.size());
			for (int pos : positions) {
				assertTrue( (sortedBytes.parse(bytes).getValueAt(pos) > 550 && sortedBytes.parse(bytes).getValueAt(pos) < 560) );
			}
		}		
}
