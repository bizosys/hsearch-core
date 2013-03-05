package com.bizosys.hsearch.byteutils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.oneline.ferrari.TestAll;

public class SortedBytesIntegerTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
		public static String mode = modes[2];  
		
		public static void main(String[] args) throws Exception {
			SortedBytesIntegerTest t = new SortedBytesIntegerTest();
			
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
			
			byte[] bytes = SortedBytesInteger.getInstance().toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>();  
			SortedBytesInteger.getInstance().parse(bytes).getEqualToIndexes(10, positions);
			
			assertNotNull(positions);
			for (int pos : positions) {
				assertEquals(SortedBytesInteger.getInstance().parse(bytes).getValueAt(pos).intValue(), 10 );
			}
			assertEquals(3, positions.size());
		}
		
		public void testEqualWithOffset() throws Exception {	
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
			
			byte[] bytes = SortedBytesInteger.getInstance().toBytes(sortedList);
			byte[] finalbytes = new byte[bytes.length + 300];
			System.arraycopy(bytes, 0, finalbytes, 300, bytes.length);
			
			int foundLoc = SortedBytesInteger.getInstance().parse(
				finalbytes,300,finalbytes.length - 300).getEqualToIndex(10);
			
			assertTrue(foundLoc != -1);
			System.out.println(foundLoc);
			int foundVal = SortedBytesInteger.getInstance().parse(
				finalbytes,300,finalbytes.length - 300).getValueAt(foundLoc);
			assertEquals(10, foundVal );
		}		
		
		public void testLessthan() throws Exception {	
			List<Integer> sortedList = new ArrayList<Integer>();
			for ( int i=0; i<1000; i++) {
				sortedList.add(i);
			}
			Collections.sort(sortedList);
			
			byte[] bytes = SortedBytesInteger.getInstance().toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>();  
			SortedBytesInteger.getInstance().parse(bytes).getLessThanIndexes(130, positions);
			
			//System.out.println(positions.toString());
			assertNotNull(positions);
			assertTrue(! positions.contains(130) );
			assertTrue(!positions.contains(999) );
			assertTrue(positions.contains(129) );
			assertTrue(positions.contains(0) );
			assertTrue(!positions.contains(-1) );
			
			assertEquals(130, positions.size());
			
			for (int pos : positions) {
				assertTrue( (SortedBytesInteger.getInstance().parse(bytes).getValueAt(pos) < 130) );
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

			byte[] bytes = SortedBytesInteger.getInstance().toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>();
			SortedBytesInteger.getInstance().parse(bytes).getLessThanIndexes(10, positions);
			
			assertNotNull(positions);
			
			for (int pos : positions) {
				assertTrue( (SortedBytesInteger.getInstance().parse(bytes).getValueAt(pos) < 10) );
			}
			assertEquals(13, positions.size());
		}				
		
		public void testLessthanEqual() throws Exception {	
			List<Integer> sortedList = new ArrayList<Integer>();
			
			for ( int i=0; i<1000; i++) {
				sortedList.add(i);
			}
			Collections.sort(sortedList);
			
			byte[] bytes = SortedBytesInteger.getInstance().toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>();
			SortedBytesInteger.getInstance().parse(bytes).getLessThanEqualToIndexes(700, positions);
			
			assertNotNull(positions);
			assertEquals(701, positions.size());
			for (int pos : positions) {
				assertTrue( (SortedBytesInteger.getInstance().parse(bytes).getValueAt(pos) <= 700) );
			}
		}			
		
		public void testGreaterthan() throws Exception {	
			List<Integer> sortedList = new ArrayList<Integer>();
			for ( int i=0; i<1000; i++) {
				sortedList.add(i);
			}
			Collections.sort(sortedList);
			
			byte[] bytes = SortedBytesInteger.getInstance().toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>(); 
			SortedBytesInteger.getInstance().parse(bytes).getGreaterThanEqualToIndexes(121, positions);
			
			assertNotNull(positions);
			assertTrue(positions.contains(498) );
			assertTrue(positions.contains(499) );
			assertTrue(positions.contains(500) );
			assertTrue(positions.contains(999) );
			assertTrue(positions.contains(121) );
			assertTrue(!positions.contains(120) );
			assertEquals(1000-121, positions.size());
			for (int pos : positions) {
				assertTrue( (SortedBytesInteger.getInstance().parse(bytes).getValueAt(pos) >= 121) );
			}
		}		
		
		public void testGreaterthanEqual() throws Exception {	
			List<Integer> sortedList = new ArrayList<Integer>();
			for ( int i=0; i<1000; i++) {
				sortedList.add(i);
			}
			
			byte[] bytes = SortedBytesInteger.getInstance().toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>(); 
			SortedBytesInteger.getInstance().parse(bytes).getGreaterThanIndexes(800, positions);
			
			assertNotNull(positions);
			assertTrue(positions.contains(801) );
			assertTrue(positions.contains(999));
			assertTrue(!positions.contains(800));
			assertEquals(1000-800-1, positions.size());
			for (int pos : positions) {
				assertTrue( (SortedBytesInteger.getInstance().parse(bytes).getValueAt(pos) > 800) );
			}
		}
		
		public void testRandomOperationsGTCheck(Integer checkNo,  
				Integer input1, Integer input2, Integer input3, Integer input4,Integer input5,
				Integer input6, Integer input7, Integer input8, Integer input9,Integer input10
				) throws Exception {

			List<Integer> sortedList = new ArrayList<Integer>();
			sortedList.add(input1);
			sortedList.add(input2);
			sortedList.add(input3);
			sortedList.add(input4);
			sortedList.add(input5);
			sortedList.add(input6);
			sortedList.add(input7);
			sortedList.add(input8);
			sortedList.add(input9);
			sortedList.add(input10);
			Collections.sort(sortedList);
			
			byte[] bytes = SortedBytesInteger.getInstance().toBytes(sortedList);
			for (Integer aInput : sortedList) {
				List<Integer> positions = new ArrayList<Integer>();  
				SortedBytesInteger.getInstance().parse(bytes).getGreaterThanIndexes(aInput, positions);
				
				for (Integer bInput : positions) {
					if ( aInput > bInput) assertTrue(positions.contains(bInput));
				}
				
			}
		}
		
		public void testRandomOperationsLTCheck(Integer checkNo,  
				Integer input1, Integer input2, Integer input3, Integer input4,Integer input5,
				Integer input6, Integer input7, Integer input8, Integer input9,Integer input10
				) throws Exception {

			List<Integer> sortedList = new ArrayList<Integer>();
			sortedList.add(input1);
			sortedList.add(input2);
			sortedList.add(input3);
			sortedList.add(input4);
			sortedList.add(input5);
			sortedList.add(input6);
			sortedList.add(input7);
			sortedList.add(input8);
			sortedList.add(input9);
			sortedList.add(input10);
			Collections.sort(sortedList);
			
			byte[] bytes = SortedBytesInteger.getInstance().toBytes(sortedList);
			for (Integer aInput : sortedList) {
				List<Integer> positions = new ArrayList<Integer>();  
				SortedBytesInteger.getInstance().parse(bytes).getLessThanIndexes(aInput, positions);
				
				for (Integer bInput : positions) {
					if ( aInput < bInput) assertTrue(positions.contains(bInput));
				}
				
			}
		}		
}
