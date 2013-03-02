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

public class Cell6Test extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
		public static String mode = modes[0];  
		
		public static void main(String[] args) throws Exception {
			Cell6Test t = new Cell6Test();
			
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
			
			Cell6<Integer, String, Integer, Integer, Integer, Float> searchTable = getSearchTable();

			SortedBytesUnsignedShort sub = SortedBytesUnsignedShort.getInstanceShort();
			
			String inputData = 
					"1199732752|hsearch1|11|1111|1|1.0\n" +
					"1199732752|hsearch1|11|2222|2|2.0\n" +
					"1199732752|hsearch1|22|1111|3|1.0\n" +
					"1199732752|hsearch2|11|9999|5|1.0\n" +
					"1199732752|hsearch2|33|5555|4|1.0";
			
			String[] lines = inputData.split("\n");
			for (String aLine : lines) {
				String[] fld = aLine.split("\\|");
				searchTable.put( 
					Integer.parseInt(fld[0]), fld[1] , Integer.parseInt(fld[2]), Integer.parseInt(fld[3]), Integer.parseInt(fld[4]) , Float.parseFloat(fld[5]));
			}
			searchTable.sort (new CellComparator.FloatComparator<Integer>());


			byte[] data = searchTable.toBytes();
			
			//Test Parsing
			Cell6<Integer, String, Integer, Integer, Integer, Float> searchTableDeser = getSearchTable();
			Map<Integer, Cell5<String, Integer, Integer, Integer, Float>> mapHashCodecs = searchTableDeser.getMap(data);
			assertEquals(1, mapHashCodecs.size());
			
			StringBuilder outputData = new StringBuilder();
			Iterator<Entry<Integer, Cell5<String, Integer, Integer, Integer, Float>>> hascodecItr = mapHashCodecs.entrySet().iterator();  
			while ( hascodecItr.hasNext() ) {
				Entry<Integer, Cell5<String, Integer, Integer, Integer, Float>> aHash = hascodecItr.next();
				Integer _hashCode = aHash.getKey();
				Cell5<String, Integer, Integer, Integer, Float> cell5 = aHash.getValue();
				Iterator<Entry<String, Cell4<Integer, Integer, Integer, Float>>> itemItr = 
						cell5.getMap().entrySet().iterator();  
				while ( itemItr.hasNext()) {
					Entry<String, Cell4<Integer, Integer, Integer, Float>> aTerm = itemItr.next();
					String _term = aTerm.getKey();
					Cell4<Integer, Integer, Integer, Float> cell4 = aTerm.getValue();
					Iterator<Entry<Integer, Cell3<Integer, Integer, Float>>> docItr = cell4.getMap().entrySet().iterator();
					while ( docItr.hasNext()) {

						Entry<Integer, Cell3<Integer, Integer, Float>> aDoc = docItr.next();
						Integer _doc = aDoc.getKey();
						Cell3<Integer, Integer, Float> cell3 = aDoc.getValue();
						
						Iterator<Entry<Integer, Cell2<Integer, Float>>> termtypeItr = cell3.getMap().entrySet().iterator();
						while ( termtypeItr.hasNext()) {
						
							Entry<Integer, Cell2<Integer, Float>> word = termtypeItr.next();
							Integer _wordtype = word.getKey();
							Cell2<Integer, Float> cell2 = word.getValue();
							
							for (CellKeyValue<Integer, Float> _word : cell2.getMap()) {
								outputData.append(_hashCode + "|" + _term + "|" + _doc + "|" + _wordtype + "|" + _word.getKey() + "|" + _word.getValue() + "\n");
							}
						}
					}
					
				}
			}
			inputData = inputData + "\n";
			assertEquals(inputData, outputData.toString());
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
		
		private Cell6<Integer, String, Integer, Integer, Integer, Float> getSearchTable() {
			Cell6<Integer, String, Integer, Integer, Integer, Float> searchTable = new
					Cell6<Integer, String, Integer, Integer, Integer, Float>(
					SortedBytesInteger.getInstance(), 
					SortedBytesString.getInstance(), 
					SortedBytesUnsignedShort.getInstance(), 
					SortedBytesUnsignedShort.getInstance(), 
					SortedBytesInteger.getInstance(), 
					SortedBytesFloat.getInstance());
			return searchTable;
		}		
}
