package com.bizosys.hsearch.treetable;

import java.util.Arrays;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.oneline.ferrari.TestAll;

public class Cell4Test extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
		public static String mode = modes[2];  
		
		public static void main(String[] args) throws Exception {
			Cell4Test t = new Cell4Test();
			
			if ( modes[0].equals(mode) ) {
				TestAll.run(new TestCase[]{t});
			} else if  ( modes[1].equals(mode) ) {
		        TestFerrari.testRandom(t);
		        
			} else if  ( modes[2].equals(mode) ) {
				t.setUp();
				t.testSort();
				t.tearDown();
			}
		}

		@Override
		protected void setUp() throws Exception {
		}
		
		@Override
		protected void tearDown() throws Exception {
		}
		
		public void testSorterConstructor() throws Exception {
			
			Cell4<Integer, Integer, Integer, Integer> ser = new Cell4<Integer, Integer, Integer, Integer>(
					SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance(), 
					SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance());

			for ( int i=0; i<2; i++) {
				for ( int j=10; j<20; j++) {
					for ( int k=100; k<200; k++) {
						ser.put(i, j, k, k * 10);
					}
				}
			}
			
			ser.sort (new CellComparator.IntegerComparator<Integer>());
			
			//Test Parsing
			Cell4<Integer, Integer, Integer, Integer> deser = new Cell4<Integer, Integer, Integer, Integer>(
					SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance(), 
					SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance());

			deser.parseElements(ser.toBytes());
			System.out.println( deser.getMap().toString());
		}
		
		public void testBytesSectionConstructor() throws Exception {
			
			Cell4<Integer, Integer, Integer, Integer> ser = new Cell4<Integer, Integer, Integer, Integer>(
					SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance(), 
					SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance());

			for ( int i=0; i<2; i++) {
				for ( int j=10; j<20; j++) {
					for ( int k=100; k<200; k++) {
						ser.put(i, j, k, k * 10);
					}
				}
			}
			
			ser.sort (new CellComparator.IntegerComparator<Integer>());
			
			byte[] data = ser.toBytes();
			byte[] appendedData = new byte[12 + data.length + 12];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 12, data.length);
			BytesSection dataSection = new BytesSection(appendedData, 12, data.length);

			//Test Parsing
			Cell4<Integer, Integer, Integer, Integer> deser = new Cell4<Integer, Integer, Integer, Integer>(
					SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance(), 
					SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance(), dataSection);

			System.out.println( deser.getMap().toString());
		}
		
		public void testSort() throws Exception {
			
			Cell4<String, String, String, Integer> ser = new Cell4<String, String, String, Integer>(
					SortedBytesString.getInstance(), SortedBytesString.getInstance(), 
					SortedBytesString.getInstance(), SortedBytesInteger.getInstance());

			for ( int i=0;i<250000; i++) {
				ser.put("bangalore", "jayanagar", "560083", 523);
				ser.put("mumbai", "vasai", "560083", 123);
				ser.put("bangalore", "koramangala", "0000", -23);
				ser.put("bangalore", "indiranagar", "560086", 18);
			}
			
			ser.sort (new CellComparator.IntegerComparator<String>());
			
			byte[] data = ser.toBytes();
			byte[] appendedData = new byte[12 + data.length + 12];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 12, data.length);
			BytesSection dataSection = new BytesSection(appendedData, 12, data.length);

			//Test Parsing
			Cell4<String, String, String, Integer> deser = new Cell4<String, String, String, Integer>(
					SortedBytesString.getInstance(), SortedBytesString.getInstance(), 
					SortedBytesString.getInstance(), SortedBytesInteger.getInstance(), dataSection);

			deser.parseElements();
			for (String lst : deser.sortedList.keySet()) {
				deser.sortedList.get(lst).parseElements();
				for (String m : deser.sortedList.get(lst).sortedList.keySet()) {
					System.out.println(lst + "-" + m);
				}
				
			}
		}		
		
}
