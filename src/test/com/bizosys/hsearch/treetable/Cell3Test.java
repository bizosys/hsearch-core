package com.bizosys.hsearch.treetable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesFloat;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesLong;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.oneline.ferrari.TestAll;

public class Cell3Test extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
		public static String mode = modes[2];  
		
		public static void main(String[] args) throws Exception {
			Cell3Test t = new Cell3Test();
			
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
			
			Cell3<String, Integer, Long> tc = new Cell3<String, Integer, Long>(
					SortedBytesString.getInstance(), SortedBytesInteger.getInstance(), SortedBytesLong.getInstance());

			tc.add("entry100", 0, 1111L);
			tc.add("entry100", 0, 1111L);
			tc.add("entry100", 0, 2222L);
			tc.add("entry100", 0, 3333L);
			tc.add("entry100", 0, 4444L);
			tc.add("entry100", 0, 5555L);
			tc.add("entry100", 1, 4444L);
			tc.add("entry100", 1, 4444L);
			tc.add("entry100", 1, 4444L);
			
			tc.add("entry101", 2, 9999L);

			tc.sort (new CellComparator.LongComparator<Integer>());

			Set<String> uniqueKeys = new HashSet<String>();
			uniqueKeys.addAll(tc.sortedList.keySet());
			
			//Test Parsing
			Cell3<String, Integer, Long> tcNewParsing = new Cell3<String, Integer, Long>(
					SortedBytesString.getInstance(), SortedBytesInteger.getInstance(), SortedBytesLong.getInstance());
			tcNewParsing.data = tc.toBytes();
			
			tcNewParsing.parseElements();
			assertEquals(2, tcNewParsing.sortedList.size());
			
			for (String key : tcNewParsing.sortedList.keySet()) {
				assertTrue( uniqueKeys.contains(key) );
			}
			

			Iterator<Cell2<Integer, Long>> valItr = tcNewParsing.sortedList.values().iterator();
			Cell2<Integer, Long> value = valItr.next();
			value.parseElements();
			List<Integer> allKeys = new ArrayList<Integer>();
			List<Long> allVals = new ArrayList<Long>();
			value.getAllKeys(allKeys);
			value.getAllValues(allVals);
			
			assertEquals(9 , allKeys.size());
			assertTrue("[0, 0, 0, 0, 1, 1, 1, 0, 0]".equals(allKeys.toString()) );
			assertEquals("[1111, 1111, 2222, 3333, 4444, 4444, 4444, 4444, 5555]", allVals.toString());
			

			value = valItr.next();
			value.parseElements();
			allKeys = new ArrayList<Integer>();
			allVals = new ArrayList<Long>();
			value.getAllKeys(allKeys);
			value.getAllValues(allVals);
			
			assertEquals(1 , allKeys.size());
			assertTrue("[2]".equals(allKeys.toString()) );
			assertEquals("[9999]", allVals.toString());
			
			//Find Matching
			Cell3<String, Integer, Long> tcNewFinding = new Cell3<String, Integer, Long>(
					SortedBytesString.getInstance(), SortedBytesInteger.getInstance(), SortedBytesLong.getInstance());
			tcNewFinding.data = tc.toBytes();

			List<Cell2<Integer, Long>> all = new ArrayList<Cell2<Integer, Long>>();
			tcNewFinding.findValues("entry100", null, null, all);
			for (Cell2<Integer, Long> cell2 : all) {
				cell2.parseElements();
				StringBuilder sb = new StringBuilder();
				for (CellKeyValue<Integer, Long> kv : cell2.sortedList) {
					sb.append(kv.key.toString() + "-" + kv.value.toString() + "|");
				}
				assertEquals("0-1111|0-1111|0-2222|0-3333|1-4444|1-4444|1-4444|0-4444|0-5555|", sb.toString());
			}
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
}
