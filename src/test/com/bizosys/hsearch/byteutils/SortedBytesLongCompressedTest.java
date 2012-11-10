package com.bizosys.hsearch.byteutils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.oneline.ferrari.TestAll;

public class SortedBytesLongCompressedTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
		public static String mode = modes[1];  
		
		public static void main(String[] args) throws Exception {
			SortedBytesLongCompressedTest t = new SortedBytesLongCompressedTest();
			
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
			List<Long> sortedList = new ArrayList<Long>();
			for ( int i=0; i<1000; i++) {
				sortedList.add(new Long(i));
			}
			Collections.sort(sortedList);
			
			byte[] bytes = SortedBytesLongCompressed.getInstance().toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>();  
			SortedBytesLongCompressed.getInstance().parse(bytes).getEqualToIndexes(10L, positions);
			assertNotNull(positions);
			
			assertEquals(10 , SortedBytesLongCompressed.getInstance().parse(bytes).getValueAt(10).intValue());
			
			sortedList.add(new Long(10));
			sortedList.add(new Long(10));
			
			bytes = SortedBytesLongCompressed.getInstance().toBytes(sortedList);
			for (int pos : positions) {
				assertEquals(SortedBytesLongCompressed.getInstance().parse(bytes).getValueAt(pos).intValue(), 10 );
			}
		}
		
		public void testLessthan() throws Exception {	
			List<Long> sortedList = new ArrayList<Long>();
			for ( int i=0; i<1000; i++) {
				sortedList.add( new Long(i));
			}
			Collections.sort(sortedList);
			
			byte[] bytes = SortedBytesLongCompressed.getInstance().toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>();  
			SortedBytesLongCompressed.getInstance().parse(bytes).getLessThanIndexes(130L, positions);
			
			assertNotNull(positions);
			assertTrue(! positions.contains(130) );
			assertTrue(!positions.contains(999) );
			assertTrue(positions.contains(129) );
			assertTrue(positions.contains(0) );
			assertTrue(!positions.contains(-1) );
			
			assertEquals(130, positions.size());
			
			for (int pos : positions) {
				assertTrue( (SortedBytesLongCompressed.getInstance().parse(bytes).getValueAt(pos) < 130) );
			}
		}		
		
		public void testLessthanMultiValue() throws Exception {	
			List<Long> sortedList = new ArrayList<Long>();
			sortedList.add(new Long(0));
			sortedList.add(new Long(5));
			sortedList.add(new Long(2));
			sortedList.add(new Long(12));
			sortedList.add(new Long(10));
			sortedList.add(new Long(10));
			sortedList.add(new Long(15));
			sortedList.add(new Long(18));
			
			for ( long i=0; i<1000; i++) {
				sortedList.add(i);
			}
			
			Collections.sort(sortedList);

			byte[] bytes = SortedBytesLongCompressed.getInstance().toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>();
			SortedBytesLongCompressed.getInstance().parse(bytes).getLessThanIndexes(10L, positions);
			
			assertNotNull(positions);
			
			for (int pos : positions) {
				assertTrue( (SortedBytesLongCompressed.getInstance().parse(bytes).getValueAt(pos) < 10) );
			}
			assertEquals(13, positions.size());
		}				
		
		public void testLessthanEqual() throws Exception {	
			List<Long> sortedList = new ArrayList<Long>();
			
			for ( int i=0; i<1000; i++) {
				sortedList.add(new Long(i));
			}
			Collections.sort(sortedList);
			
			byte[] bytes = SortedBytesLongCompressed.getInstance().toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>();
			SortedBytesLongCompressed.getInstance().parse(bytes).getLessThanEqualToIndexes(700L, positions);
			
			assertNotNull(positions);
			assertEquals(701, positions.size());
			for (int pos : positions) {
				assertTrue( (SortedBytesLongCompressed.getInstance().parse(bytes).getValueAt(pos) <= 700) );
			}
		}			
		
		public void testGreaterthan() throws Exception {	
			List<Long> sortedList = new ArrayList<Long>();
			for ( long i=0; i<1000; i++) {
				sortedList.add(i);
			}
			Collections.sort(sortedList);
			
			byte[] bytes = SortedBytesLongCompressed.getInstance().toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>(); 
			SortedBytesLongCompressed.getInstance().parse(bytes).getGreaterThanIndexes(121L, positions);
			assertNotNull(positions);
			assertTrue(positions.contains(498) );
			assertTrue(positions.contains(499) );
			assertTrue(positions.contains(500) );
			assertTrue(positions.contains(999) );
			assertTrue( positions.contains(122) );
			assertTrue(! positions.contains(121) );
			assertEquals(1000-121-1, positions.size());
			for (int pos : positions) {
				assertTrue( (SortedBytesLongCompressed.getInstance().parse(bytes).getValueAt(pos) > 121) );
			}
		}		
		
		public void testGreaterthanEqual() throws Exception {	
			List<Long> sortedList = new ArrayList<Long>();
			for ( int i=0; i<1000; i++) {
				sortedList.add(new Long(i));
			}
			
			byte[] bytes = SortedBytesLongCompressed.getInstance().toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>(); 
			SortedBytesLongCompressed.getInstance().parse(bytes).getGreaterThanIndexes(800L, positions);
			
			assertNotNull(positions);
			assertTrue(positions.contains(801) );
			assertTrue(positions.contains(999));
			assertTrue(!positions.contains(800));
			assertEquals(1000-800-1, positions.size());
			for (int pos : positions) {
				assertTrue( (SortedBytesLongCompressed.getInstance().parse(bytes).getValueAt(pos) > 800) );
			}
		}
		
		public void testRandomOperationsGTCheck(Long checkNo,  
				Long input1, Long input2, Long input3, Long input4,Long input5,
				Long input6, Long input7, Long input8, Long input9,Long input10
				) throws Exception {

			List<Long> sortedList = new ArrayList<Long>();
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
			
			byte[] bytes = SortedBytesLongCompressed.getInstance().toBytes(sortedList);
			for (Long aInput : sortedList) {
				List<Integer> positions = new ArrayList<Integer>();  
				SortedBytesLongCompressed.getInstance().parse(bytes).getGreaterThanIndexes(aInput, positions);
				
				for (Integer bInput : positions) {
					if ( aInput > bInput) assertTrue(positions.contains(bInput));
				}
				
			}
		}
		
		public void testRandomOperationsLTCheck(Long checkNo,  
				Long input1, Long input2, Long input3, Long input4,Long input5,
				Long input6, Long input7, Long input8, Long input9,Long input10
				) throws Exception {

			List<Long> sortedList = new ArrayList<Long>();
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
			
			byte[] bytes = SortedBytesLongCompressed.getInstance().toBytes(sortedList);
			
			ISortedByte<Long> cp = SortedBytesLongCompressed.getInstance().parse(bytes); 
			for (Long aInput : sortedList) {
				List<Integer> positions = new ArrayList<Integer>();  
				cp.getLessThanIndexes(aInput, positions);
				
				for (Integer bInput : positions) {
					if ( aInput < bInput) assertTrue(positions.contains(bInput));
				}
				
			}
		}		
}
