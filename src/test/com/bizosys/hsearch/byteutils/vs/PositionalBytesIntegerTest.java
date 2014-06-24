package com.bizosys.hsearch.byteutils.vs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.oneline.ferrari.TestAll;

public class PositionalBytesIntegerTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
		public static String mode = modes[2];  
		
		public static void main(String[] args) throws Exception {
			PositionalBytesIntegerTest t = new PositionalBytesIntegerTest();
			
			if ( modes[0].equals(mode) ) {
				TestAll.run(new TestCase[]{t});
			} else if  ( modes[1].equals(mode) ) {
		        TestFerrari.testRandom(t);
		        
			} else if  ( modes[2].equals(mode) ) {
				t.setUp();
				t.testRangeCheck();
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
			
			PositionalBytesInteger pos = (PositionalBytesInteger) PositionalBytesInteger.getInstance(Integer.MIN_VALUE);
			byte[] bytes = pos.toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>();  
			pos.parse(bytes).getEqualToIndexes(10, positions);
			assertNotNull(positions);
			for (int position : positions) {
				assertEquals(pos.parse(bytes).getValueAt(position).intValue(), 10 );
			}
			assertEquals(3, positions.size());
		}
		
		public void testEqualWithOffset() throws Exception {	
			List<Integer> sortedList = new ArrayList<Integer>();
			sortedList.add(new Integer(-1));
			sortedList.add(new Integer(2));
			sortedList.add(new Integer(12));
			sortedList.add(new Integer(10));
			sortedList.add(new Integer(10));
			sortedList.add(new Integer(15));
			sortedList.add(new Integer(18));
			
			for ( int i=0; i<1000; i++) {
				sortedList.add(i);
			}
			
			PositionalBytesInteger pos = (PositionalBytesInteger) PositionalBytesInteger.getInstance(Integer.MIN_VALUE);
			byte[] bytes = pos.toBytes(sortedList);
			byte[] finalbytes = new byte[bytes.length + 300];
			System.arraycopy(bytes, 0, finalbytes, 300, bytes.length);
			
			int foundLoc = pos.parse(finalbytes,300,finalbytes.length - 300).getEqualToIndex(10);
			
			assertTrue(foundLoc != -1);
			int foundVal = pos.parse(finalbytes,300,finalbytes.length - 300).getValueAt(foundLoc);
			assertEquals(10, foundVal );
		}		
		
		public void testLessthan() throws Exception {	
			List<Integer> sortedList = new ArrayList<Integer>();
			for ( int i=0; i<1000; i++) {
				sortedList.add(i);
			}
			
			PositionalBytesInteger pos = (PositionalBytesInteger) PositionalBytesInteger.getInstance(Integer.MIN_VALUE);
			byte[] bytes = pos.toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>();  
			pos.parse(bytes).getLessThanIndexes(130, positions);
			
			//System.out.println(positions.toString());
			assertNotNull(positions);
			assertTrue(! positions.contains(130) );
			assertTrue(!positions.contains(999) );
			assertTrue(positions.contains(129) );
			assertTrue(positions.contains(0) );
			assertTrue(!positions.contains(-1) );
			
			assertEquals(130, positions.size());

			for (int position : positions) {
				assertTrue( (pos.parse(bytes).getValueAt(position) < 130) );
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

			PositionalBytesInteger pos = (PositionalBytesInteger) PositionalBytesInteger.getInstance(Integer.MIN_VALUE);
			byte[] bytes = pos.toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>();
			pos.parse(bytes).getLessThanIndexes(10, positions);
			
			assertNotNull(positions);
			
			for (int position : positions) {
				assertTrue( (pos.parse(bytes).getValueAt(position) < 10) );
			}
			assertEquals(13, positions.size());
		}				
		
		public void testLessthanEqual() throws Exception {	
			List<Integer> sortedList = new ArrayList<Integer>();
			sortedList.add(32);
			sortedList.add(82);
			for ( int i=0; i<1000; i++) {
				sortedList.add(i);
			}
			
			PositionalBytesInteger pos = (PositionalBytesInteger) PositionalBytesInteger.getInstance(Integer.MIN_VALUE);
			byte[] bytes = pos.toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>();
			pos.parse(bytes).getLessThanEqualToIndexes(700, positions);
			
			assertNotNull(positions);
			assertEquals(703, positions.size());
			for (int position : positions) {
				assertTrue( (pos.parse(bytes).getValueAt(position) <= 700) );
			}
		}			
		
		public void testGreaterthan() throws Exception {	
			List<Integer> sortedList = new ArrayList<Integer>();
			for ( int i=0; i<1000; i++) {
				sortedList.add(i);
			}
			
			PositionalBytesInteger pos = (PositionalBytesInteger) PositionalBytesInteger.getInstance(Integer.MIN_VALUE);
			byte[] bytes = pos.toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>(); 
			pos.parse(bytes).getGreaterThanEqualToIndexes(121, positions);
			
			assertNotNull(positions);
			assertTrue(positions.contains(498) );
			assertTrue(positions.contains(499) );
			assertTrue(positions.contains(500) );
			assertTrue(positions.contains(999) );
			assertTrue(positions.contains(121) );
			assertTrue(!positions.contains(120) );
			assertEquals(1000-121, positions.size());
			for (int position : positions) {
				assertTrue( (pos.parse(bytes).getValueAt(position) >= 121) );
			}
		}		
		
		public void testGreaterthanEqual() throws Exception {	
			List<Integer> sortedList = new ArrayList<Integer>();
			for ( int i=0; i<1000; i++) {
				sortedList.add(i);
			}
			
			PositionalBytesInteger pos = (PositionalBytesInteger) PositionalBytesInteger.getInstance(Integer.MIN_VALUE);
			byte[] bytes = pos.toBytes(sortedList);
			List<Integer> positions = new ArrayList<Integer>(); 
			pos.parse(bytes).getGreaterThanIndexes(800, positions);
			
			assertNotNull(positions);
			assertTrue(positions.contains(801) );
			assertTrue(positions.contains(999));
			assertTrue(!positions.contains(800));
			assertEquals(1000-800-1, positions.size());
			for (int position : positions) {
				assertTrue( (pos.parse(bytes).getValueAt(position) > 800) );
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
			
			PositionalBytesInteger pos = (PositionalBytesInteger) PositionalBytesInteger.getInstance(Integer.MIN_VALUE);
			byte[] bytes = pos.toBytes(sortedList);
			for (Integer aInput : sortedList) {
				List<Integer> positions = new ArrayList<Integer>();  
				pos.parse(bytes).getGreaterThanIndexes(aInput, positions);
				
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
			
			PositionalBytesInteger pos = (PositionalBytesInteger) PositionalBytesInteger.getInstance(Integer.MIN_VALUE);
			byte[] bytes = pos.toBytes(sortedList);
			for (Integer aInput : sortedList) {
				List<Integer> positions = new ArrayList<Integer>();  
				pos.parse(bytes).getLessThanIndexes(aInput, positions);
				
				for (Integer bInput : positions) {
					if ( aInput < bInput) assertTrue(positions.contains(bInput));
				}
				
			}
		}		
		
		public void testRangeCheck() throws IOException{
			List<Integer> list = new ArrayList<Integer>();
			list.add(10);
			list.add(12);
			list.add(1);
			list.add(3);
			list.add(8);
			
			for(int i = 0; i < 1000; i++){
				list.add(i);
			}
			
			PositionalBytesInteger pos = (PositionalBytesInteger) PositionalBytesInteger.getInstance(Integer.MIN_VALUE);
			byte[] bytes = pos.toBytes(list);
			List<Integer> positions = new ArrayList<Integer>();
			pos.parse(bytes).getRangeIndexesInclusive(5, 10, positions);
			for (Integer position : positions) {
				System.out.println(list.get(position));
			}
		}
}
