package com.bizosys.hsearch.treetable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.byteutils.SortedBytesChar;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesLong;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.oneline.ferrari.TestAll;

public class Cell4Test extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
		public static String mode = modes[1];  
		
		public static void main(String[] args) throws Exception {
			Cell4Test t = new Cell4Test();
			
			if ( modes[0].equals(mode) ) {
				TestAll.run(new TestCase[]{t});
			} else if  ( modes[1].equals(mode) ) {
		        TestFerrari.testRandom(t);
		        
			} else if  ( modes[2].equals(mode) ) {
				t.setUp();
				t.testBytesSectionConstructor();
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
		
}
