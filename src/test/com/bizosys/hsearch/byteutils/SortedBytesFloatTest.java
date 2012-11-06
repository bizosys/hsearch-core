package com.bizosys.hsearch.byteutils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.oneline.ferrari.TestAll;

public class SortedBytesFloatTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
		public static String mode = modes[0];  
		
		public static void main(String[] args) throws Exception {
			SortedBytesFloatTest t = new SortedBytesFloatTest();
			
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
			List<Float> sortedList = new ArrayList<Float>();
			sortedList.add(new Float(0.0F));
			sortedList.add(new Float(2.1F));
			sortedList.add(new Float(12.3F));
			sortedList.add(new Float(10.0F));
			sortedList.add(new Float(10.0F));
			sortedList.add(new Float(15.17F));
			sortedList.add(new Float(18F));
			
			for ( int i=0; i<1000; i++) {
				sortedList.add(new Float(i* 1.0));
			}
			
			Collections.sort(sortedList);
			byte[] bytes = SortedBytesFloat.getInstance().toBytes(sortedList, false);
			List<Integer> positions = new ArrayList<Integer>();
			SortedBytesFloat.getInstance().getEqualToIndexes(bytes, 10.0F, positions);
			
			assertNotNull(positions);
			assertEquals(3, positions.size());
			for (int pos : positions) {
				assertEquals(SortedBytesFloat.getInstance().getValueAt(bytes,pos), 10.0F );
			}
		}
		
		public void testLessthan() throws Exception {	
			List<Float> sortedList = new ArrayList<Float>();
			for ( int i=0; i<1000; i++) {
				sortedList.add(new Float(i));
			}
			
			Collections.sort(sortedList);
			
			byte[] bytes = SortedBytesFloat.getInstance().toBytes(sortedList, false);
			List<Integer> positions = new ArrayList<Integer>();
			SortedBytesFloat.getInstance().getLessThanIndexes(bytes, 130.0F, positions);
			
			//System.out.println(positions.toString());
			assertNotNull(positions);
			assertTrue(! positions.contains(130) );
			assertTrue(!positions.contains(999) );
			assertTrue(positions.contains(129) );
			assertTrue(positions.contains(0) );
			assertTrue(!positions.contains(-1) );
			
			assertEquals(130, positions.size());
			
			for (int pos : positions) {
				assertTrue( (SortedBytesFloat.getInstance().getValueAt(bytes,pos) < 130) );
			}
		}		
		public void testLessthanMultiValue() throws Exception {	
			List<Float> sortedList = new ArrayList<Float>();
			sortedList.add(new Float(0));
			sortedList.add(new Float(5));
			sortedList.add(new Float(2));
			sortedList.add(new Float(12));
			sortedList.add(new Float(10));
			sortedList.add(new Float(10));
			sortedList.add(new Float(15));
			sortedList.add(new Float(18));
			
			for ( int i=0; i<1000; i++) {
				sortedList.add(new Float(i));
			}
			
			Collections.sort(sortedList);
			
			byte[] bytes = SortedBytesFloat.getInstance().toBytes(sortedList, false);
			List<Integer> positions = new ArrayList<Integer>();
			SortedBytesFloat.getInstance().getLessThanIndexes(bytes, 10F, positions);
			
			assertNotNull(positions);
			assertEquals(13, positions.size());
			
			for (int pos : positions) {
				assertTrue( (SortedBytesFloat.getInstance().getValueAt(bytes,pos) < 10) );
			}
		}				
		
		public void testLessthanEqual() throws Exception {	
			List<Float> sortedList = new ArrayList<Float>();
			
			for ( int i=0; i<1000; i++) {
				sortedList.add(new Float(i));
			}
			
			Collections.sort(sortedList);
			byte[] bytes = SortedBytesFloat.getInstance().toBytes(sortedList, false);
			List<Integer> positions = new ArrayList<Integer>();
			SortedBytesFloat.getInstance().getLessThanEqualToIndexes(bytes, 700.0F, positions);
			
			assertNotNull(positions);
			assertEquals(701, positions.size());
			for (int pos : positions) {
				assertTrue( (SortedBytesFloat.getInstance().getValueAt(bytes,pos) <= 700) );
			}
		}			
		
		public void testGreaterthan() throws Exception {	
			List<Float> sortedList = new ArrayList<Float>();
			for ( int i=0; i<1000; i++) {
				sortedList.add(new Float(i));
			}
			
			Collections.sort(sortedList);
			
			byte[] bytes = SortedBytesFloat.getInstance().toBytes(sortedList, false);
			List<Integer> positions = new ArrayList<Integer>();
			SortedBytesFloat.getInstance().getGreaterThanEqualToIndexes(bytes, 121.0F, positions);
			
			assertNotNull(positions);
			assertTrue(positions.contains(498) );
			assertTrue(positions.contains(499) );
			assertTrue(positions.contains(500) );
			assertTrue(positions.contains(999) );
			assertTrue(positions.contains(121) );
			assertTrue(!positions.contains(120) );
			assertEquals(1000-121, positions.size());
			for (int pos : positions) {
				assertTrue( (SortedBytesFloat.getInstance().getValueAt(bytes,pos) >= 121) );
			}
		}		
		
		public void testGreaterthanEqual() throws Exception {	
			List<Float> sortedList = new ArrayList<Float>();
			for ( int i=0; i<1000; i++) {
				sortedList.add(new Float(i));
			}
			
			Collections.sort(sortedList);
			
			byte[] bytes = SortedBytesFloat.getInstance().toBytes(sortedList, false);
			List<Integer> positions = new ArrayList<Integer>() ;
			SortedBytesFloat.getInstance().getGreaterThanIndexes(bytes, 800.0F, positions);
			
			assertNotNull(positions);
			assertTrue(positions.contains(801) );
			assertTrue(positions.contains(999));
			assertTrue(!positions.contains(800));
			assertEquals(1000-800-1, positions.size());
			for (int pos : positions) {
				assertTrue( (SortedBytesFloat.getInstance().getValueAt(bytes,pos) > 800) );
			}
		}
		
		public void testRandomOperationsGTCheck(Float checkNo,  
				Float input1, Float input2, Float input3, Float input4,Float input5,
				Float input6, Float input7, Float input8, Float input9,Float input10
				) throws Exception {

			List<Float> sortedList = new ArrayList<Float>();
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
			
			byte[] bytes = SortedBytesFloat.getInstance().toBytes(sortedList, false);
			for (Float aInput : sortedList) {
				List<Integer> positions = new ArrayList<Integer>();
				SortedBytesFloat.getInstance().getGreaterThanIndexes(bytes, aInput, positions);
				
				for (Integer bInput : positions) {
					if ( aInput > bInput) assertTrue(positions.contains(bInput));
				}
				
			}
		}
		
		public void testRandomOperationsLTCheck(Integer checkNo,  
				Float input1, Float input2, Float input3, Float input4,Float input5,
				Float input6, Float input7, Float input8, Float input9,Float input10
				) throws Exception {

			List<Float> sortedList = new ArrayList<Float>();
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
			System.out.println(sortedList.toString());
			Collections.sort(sortedList);

			byte[] bytes = SortedBytesFloat.getInstance().toBytes(sortedList, false);
			for (Float aInput : sortedList) {
				List<Integer> positions = new ArrayList<Integer>();
				SortedBytesFloat.getInstance().getLessThanIndexes(bytes, aInput, positions);
				
				for (Integer bInput : positions) {
					if ( aInput < bInput) assertTrue(positions.contains(bInput));
				}
				
			}
		}		
		
		public void testRangeCheck() throws Exception {

			List<Float> sortedList = new ArrayList<Float>();
			sortedList.add(234.2F);
			sortedList.add(239.2F);
			sortedList.add(299.2F);
			sortedList.add(301.00F);
			sortedList.add(367.90F);
			Collections.sort(sortedList);

			byte[] inputData = SortedBytesFloat.getInstance().toBytes(sortedList, false);
			List<Integer> positions = new ArrayList<Integer>();
			SortedBytesFloat.getInstance().getRangeIndexes(inputData, 201F, 250F, positions);

			for (Integer aPos : positions) {
				System.out.println(sortedList.get(aPos) );
			}
			
			assertEquals(2, positions.size());
			for (Integer aPos : positions) {
				boolean isGood = (sortedList.get(aPos) > 201F && sortedList.get(aPos) < 250F);
				assertTrue(isGood);
			}
		}			
}
