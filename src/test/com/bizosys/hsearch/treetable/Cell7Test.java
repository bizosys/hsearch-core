package com.bizosys.hsearch.treetable;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.byteutils.SortedBytesFloat;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.byteutils.SortedBytesUnsignedShort;
import com.oneline.ferrari.TestAll;

public class Cell7Test extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
		public static String mode = modes[2];  
		
		public static void main(String[] args) throws Exception {
			Cell7Test t = new Cell7Test();
			
			if ( modes[0].equals(mode) ) {
				TestAll.run(new TestCase[]{t});
			} else if  ( modes[1].equals(mode) ) {
		        TestFerrari.testRandom(t);
		        
			} else if  ( modes[2].equals(mode) ) {
				t.setUp();
				t.testAddOnEmptySet();
				t.tearDown();
			}
		}

		@Override
		protected void setUp() throws Exception {
		}
		
		@Override
		protected void tearDown() throws Exception {
		}
		
		public void testAddOnEmptySet() throws Exception {
			
			Cell7<String, Integer, String, Integer, Integer, Integer, Float> searchTable = getSearchTable();

			SortedBytesUnsignedShort sub = SortedBytesUnsignedShort.getInstanceShort();
			
			String inputData = 
					"REC1|1199732752|hsearch1|11|1111|1|1.0\n" +
					"REC1|1199732752|hsearch1|11|2222|2|2.0\n" +
					"REC1|1199732752|hsearch1|22|1111|3|1.0\n" +
					"REC1|1199732752|hsearch2|11|9999|5|1.0\n" +
					"REC1|1199732752|hsearch2|33|5555|4|1.0";
			
			String[] lines = inputData.split("\n");
			for (String aLine : lines) {
				String[] fld = aLine.split("\\|");
				searchTable.put(fld[0], Integer.parseInt(fld[1]), fld[2] , Integer.parseInt(fld[3]), Integer.parseInt(fld[4]), Integer.parseInt(fld[5]) , Float.parseFloat(fld[6]));
			}
			searchTable.sort (new CellComparator.FloatComparator<Integer>());


			byte[] data = searchTable.toBytes();
			
			//Test Parsing
			Cell7<String, Integer, String, Integer, Integer, Integer, Float> searchTableDeser = getSearchTable();
			Map<String, Cell6<Integer, String, Integer, Integer, Integer, Float>> mapHashCodecs = searchTableDeser.getMap(data);
			
			for (Cell6<Integer, String, Integer, Integer, Integer, Float> val6 : mapHashCodecs.values() ) {
				for (Cell5<String, Integer, Integer, Integer, Float> val5 : val6.values() ) {
					System.out.println(val6.keySet().toString() + val5.keySet().toString());
				}
			}
			assertEquals(1, mapHashCodecs.size());
			inputData = inputData + "\n";
		}

		public void testSubsequentAdd() throws Exception {	

		}
		
		public void testUpdateExisting() throws Exception {	
		}

		public void testUpdateNonExisting() throws Exception {	
		}

		public void testDeleteOnEmpty() throws Exception {	
		}

		public void testDeleteFistElement() throws Exception {	
		}

		public void testDeleteMidElement() throws Exception {	
		}

		public void testDeleteLastElement() throws Exception {	
		}

		public void testDeleteNonExistingElement() throws Exception {	
		}

		public void testDeleteAndReAdd() throws Exception {	
		}
		
		public void testBooleanBoolean() throws Exception {
			
		}
		
		public void testByteByte() throws Exception {
			
		}

		public void testShortShort() throws Exception {
			
		}
		
		public void testIntInt() throws Exception {
			
		}
		public void testFloatFloat() throws Exception {
			
		}
		public void testDoubleDouble() throws Exception {
			
		}
		public void testLongLong() throws Exception {
			
		}
		public void testStringString() throws Exception {
			
		}
		
		private Cell7<String,Integer, String, Integer, Integer, Integer, Float> getSearchTable() {
			Cell7<String,Integer, String, Integer, Integer, Integer, Float> searchTable = new
					Cell7<String,Integer, String, Integer, Integer, Integer, Float>(
					SortedBytesString.getInstance(), 
					SortedBytesInteger.getInstance(), 
					SortedBytesString.getInstance(), 
					SortedBytesUnsignedShort.getInstance(), 
					SortedBytesUnsignedShort.getInstance(), 
					SortedBytesInteger.getInstance(), 
					SortedBytesFloat.getInstance());
			return searchTable;
		}		
}
