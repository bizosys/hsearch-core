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

public class Cell3Test extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
		public static String mode = modes[1];  
		
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
			Cell3<String, Integer, Long> tcNewFinding = new Cell3<String, Integer, Long>(
					SortedBytesString.getInstance(), SortedBytesInteger.getInstance(), SortedBytesLong.getInstance());
			tcNewFinding.put("asas", 12, 5L);
			tcNewFinding.put("asas", 12, 5L);
			tcNewFinding.put("asas", 12, 5L);
			tcNewFinding.put("asas", 12, 5L);
			tcNewFinding.put("asas", 12, 5L);
			tcNewFinding.put("asas", 12, 5L);
			byte[] x = tcNewFinding.toBytes();

			Cell3<String, Integer, Long> tcNewFinding3 = new Cell3<String, Integer, Long>(
					SortedBytesString.getInstance(), SortedBytesInteger.getInstance(), SortedBytesLong.getInstance(), x);
			
		}
		
		@Override
		protected void tearDown() throws Exception {
		}
		
		public void testAddOnEmptySet() throws Exception {
			
			Cell3<String, Integer, Long> tc = new Cell3<String, Integer, Long>(
					SortedBytesString.getInstance(), SortedBytesInteger.getInstance(), SortedBytesLong.getInstance());

			tc.put("entry100", 0, 1111L);
			tc.put("entry100", 0, 1111L);
			tc.put("entry100", 0, 2222L);
			tc.put("entry100", 0, 3333L);
			tc.put("entry100", 0, 4444L);
			tc.put("entry100", 0, 5555L);
			tc.put("entry100", 1, 4444L);
			tc.put("entry100", 1, 4444L);
			tc.put("entry100", 1, 4444L);
			
			tc.put("entry101", 2, 9999L);
			tc.sort (new CellComparator.LongComparator<Integer>());

			Set<String> uniqueKeys = tc.getMap().keySet();
			
			//Test Parsing
			Cell3<String, Integer, Long> tcNewParsing = new Cell3<String, Integer, Long>(
					SortedBytesString.getInstance(), SortedBytesInteger.getInstance(), 
					SortedBytesLong.getInstance());

			tcNewParsing.parseElements(tc.toBytes());
			assertEquals(2, tcNewParsing.getMap().size());
			

			Iterator<Cell2<Integer, Long>> valItr = tcNewParsing.getMap().values().iterator();
			Cell2<Integer, Long> value = valItr.next();
			Collection<Integer> allKeys = value.keySet();
			Collection<Long> allVals = value.values();
			
			assertEquals(9 , allKeys.size());
			assertTrue("[0, 0, 0, 0, 1, 1, 1, 0, 0]".equals(allKeys.toString()) );
			assertEquals("[1111, 1111, 2222, 3333, 4444, 4444, 4444, 4444, 5555]", allVals.toString());
			

			value = valItr.next();
			value.parseElements();
			allKeys = value.keySet();
			allVals = value.values();
			
			assertEquals(1 , allKeys.size());
			assertTrue("[2]".equals(allKeys.toString()) );
			assertEquals("[9999]", allVals.toString());
			
			//Find Matching
			Cell3<String, Integer, Long> tcNewFinding = new Cell3<String, Integer, Long>(
					SortedBytesString.getInstance(), 
					SortedBytesInteger.getInstance(), SortedBytesLong.getInstance());

			Collection<Cell2<Integer, Long>> all = tcNewFinding.values("entry100");
			for (Cell2<Integer, Long> cell2 : all) {
				cell2.parseElements();
				StringBuilder sb = new StringBuilder();
				for (CellKeyValue<Integer, Long> kv : cell2.getMap()) {
					sb.append(kv.key.toString() + "-" + kv.value.toString() + "|");
				}
				assertEquals("0-1111|0-1111|0-2222|0-3333|1-4444|1-4444|1-4444|0-4444|0-5555|", sb.toString());
			}
		}
		
		public void testSorterConstructor() throws Exception {
			
			Cell3<String, Integer, Long> ser = new Cell3<String, Integer, Long>(
					SortedBytesString.getInstance(), SortedBytesInteger.getInstance(), SortedBytesLong.getInstance());

			ser.put("entry100", 0, 0000L);
			ser.put("entry100", 1, 1111L);
			ser.put("entry101", 2, 2222L);
			ser.put("entry101", 1, 1111L);
			ser.put("entry101", 3, 3333L);
			
			ser.sort (new CellComparator.LongComparator<Integer>());
			
			//Test Parsing
			Cell3<String, Integer, Long> deser = new Cell3<String, Integer, Long>(
					SortedBytesString.getInstance(), SortedBytesInteger.getInstance(), 
					SortedBytesLong.getInstance());

			deser.parseElements(ser.toBytes());
			assertEquals(2, deser.getMap().size());
			assertEquals("[entry100, entry101]", deser.getMap().keySet().toString());
			Iterator<Cell2<Integer, Long>> valItr = deser.getMap().values().iterator();

			Cell2<Integer, Long> value = valItr.next();
			assertEquals("[0, 1]", value.keySet().toString());
			assertEquals("[0, 1111]", value.values().toString());

			value = valItr.next();
			assertEquals("[1, 2, 3]", value.keySet().toString());
			assertEquals("[1111, 2222, 3333]", value.values().toString());
		}
		
		public void testBytesSectionConstructor() throws Exception {
			
			Cell3<String, Integer, Long> ser = new Cell3<String, Integer, Long>(
					SortedBytesString.getInstance(), SortedBytesInteger.getInstance(), SortedBytesLong.getInstance());

			ser.put("entry100", 0, 0000L);
			ser.put("entry100", 1, 1111L);
			ser.put("entry101", 2, 2222L);
			ser.put("entry101", 1, 1111L);
			ser.put("entry101", 3, 3333L);
			
			ser.sort (new CellComparator.LongComparator<Integer>());
			
			byte[] data = ser.toBytes();
			byte[] appendedData = new byte[12 + data.length + 12];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 12, data.length);
			BytesSection dataSection = new BytesSection(appendedData, 12, data.length);

			//Test Parsing
			Cell3<String, Integer, Long> deser = new Cell3<String, Integer, Long>(
					SortedBytesString.getInstance(), SortedBytesInteger.getInstance(), 
					SortedBytesLong.getInstance(), dataSection);
			
			assertEquals(2, deser.getMap().size());
			assertEquals("[entry100, entry101]", deser.getMap().keySet().toString());
			Iterator<Cell2<Integer, Long>> valItr = deser.getMap().values().iterator();

			Cell2<Integer, Long> value = valItr.next();
			assertEquals("[0, 1]", value.keySet().toString());
			assertEquals("[0, 1111]", value.values().toString());

			value = valItr.next();
			assertEquals("[1, 2, 3]", value.keySet().toString());
			assertEquals("[1111, 2222, 3333]", value.values().toString());
		}
		
		public void testGetMap() throws Exception {
			
			Cell3<String, Integer, Long> ser = new Cell3<String, Integer, Long>(
					SortedBytesString.getInstance(), SortedBytesInteger.getInstance(), SortedBytesLong.getInstance());

			for ( int i=0; i<3; i++) {
				for ( int j=0; j<2; j++) {
					ser.put("c3" + i, j, (long) j * 1000);
				}
				
			}
			
			ser.sort (new CellComparator.LongComparator<Integer>());
			
			byte[] data = ser.toBytes();
			byte[] appendedData = new byte[12 + data.length + 12];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 12, data.length);
			BytesSection dataSection = new BytesSection(appendedData, 12, data.length);

			//Test Parsing
			Cell3<String, Integer, Long> deser = new Cell3<String, Integer, Long>(
					SortedBytesString.getInstance(), SortedBytesInteger.getInstance(), 
					SortedBytesLong.getInstance(), dataSection);
			assertEquals("{c30=[0-0, 1-1000], c31=[0-0, 1-1000], c32=[0-0, 1-1000]}", deser.getMap().toString() );
			
			Map<String, Cell2<Integer, Long>> rows = new HashMap<String, Cell2<Integer, Long>>();
			deser.getMap("c30", null, null, rows);
			assertEquals("{c30=[0-0, 1-1000]}", rows.toString());
		}
		
		public void testGetMapWithData() throws Exception {
			
			Cell3<Integer, Integer, Long> ser = new Cell3<Integer, Integer, Long>(
					SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance(), SortedBytesLong.getInstance());

			for ( int i=0; i<3; i++) {
				for ( int j=10; j<12; j++) {
					ser.put(i, j, (long) j * 1000);
				}
			}
			
			ser.sort (new CellComparator.LongComparator<Integer>());
			
			byte[] data = ser.toBytes();
			byte[] appendedData = new byte[12 + data.length + 12];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 12, data.length);
			BytesSection dataSection = new BytesSection(appendedData, 12, data.length);

			//Test Parsing
			Cell3<Integer, Integer, Long> deser = new Cell3<Integer, Integer, Long>(
				SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance(), 
				SortedBytesLong.getInstance(), dataSection);
			
			Map<Integer, Cell2<Integer, Long>> rows = new HashMap<Integer, Cell2<Integer, Long>>();
			deser.getMap(null, 1, 2, rows);
			assertEquals("{1=[10-10000, 11-11000], 2=[10-10000, 11-11000]}", rows.toString());
			
			rows = deser.getMap(data);
			assertEquals("{0=[10-10000, 11-11000], 1=[10-10000, 11-11000], 2=[10-10000, 11-11000]}", rows.toString());
		}
		
		public void testValues() throws Exception {
			
			Cell3<Integer, Integer, Long> ser = new Cell3<Integer, Integer, Long>(
					SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance(), SortedBytesLong.getInstance());

			for ( int i=0; i<3; i++) {
				for ( int j=10; j<12; j++) {
					ser.put(i, i * j, (long) i * j * 1000);
				}
			}
			
			ser.sort (new CellComparator.LongComparator<Integer>());
			
			byte[] data = ser.toBytes();
			byte[] appendedData = new byte[12 + data.length + 12];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 12, data.length);
			BytesSection dataSection = new BytesSection(appendedData, 12, data.length);

			//Test Parsing
			Cell3<Integer, Integer, Long> deser = new Cell3<Integer, Integer, Long>(
				SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance(), 
				SortedBytesLong.getInstance(), dataSection);
			
			assertEquals("[[20-20000, 22-22000]]", deser.values(2).toString());
			assertEquals("[[10-10000, 11-11000]]", deser.values(1, 1).toString());
			
			java.util.List<Cell2<Integer, Long>> vals = new ArrayList<Cell2<Integer,Long>>();
			deser.values(1, 1, vals);
			assertEquals("[[10-10000, 11-11000]]", vals.toString());

			assertEquals("[[10-10000, 11-11000]]", vals.toString());
			
			
			assertEquals("[[0-0, 0-0], [10-10000, 11-11000], [20-20000, 22-22000]]", deser.values().toString());
			
		}
		
		public void testParseElements() throws Exception {
			
			Cell3<Byte, Integer, Long> ser = new Cell3<Byte, Integer, Long>(
					SortedBytesChar.getInstance(), SortedBytesInteger.getInstance(), SortedBytesLong.getInstance());

			for ( byte c='a'; c<'d'; c++) {
				for ( int j=10; j<12; j++) {
					ser.put( c, j, (long) j * 1000);
				}
			}
			
			ser.sort (new CellComparator.LongComparator<Integer>());
			
			byte[] data = ser.toBytes();
			byte[] appendedData = new byte[12 + data.length + 12];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 12, data.length);
			BytesSection dataSection = new BytesSection(appendedData, 12, data.length);

			//Test Parsing
			Cell3<Byte, Integer, Long> deser = new Cell3<Byte, Integer, Long>(
				SortedBytesChar.getInstance(), SortedBytesInteger.getInstance(), 
				SortedBytesLong.getInstance(), dataSection);
			
			deser.parseElements();
			assertEquals("{97=[10-10000, 11-11000], 98=[10-10000, 11-11000], 99=[10-10000, 11-11000]}", deser.getMap().toString());
		}
}
