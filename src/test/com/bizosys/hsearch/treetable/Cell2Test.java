package com.bizosys.hsearch.treetable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesFloat;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesLong;
import com.oneline.ferrari.TestAll;

public class Cell2Test extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
		public static String mode = modes[2];  
		
		public static void main(String[] args) throws Exception {
			Cell2Test t = new Cell2Test();
			
			if ( modes[0].equals(mode) ) {
				TestAll.run(new TestCase[]{t});
			} else if  ( modes[1].equals(mode) ) {
		        TestFerrari.testRandom(t);
		        
			} else if  ( modes[2].equals(mode) ) {
				t.setUp();
				t.testSubsequentAdd(23, "aim");
				t.tearDown();
			}
		}

		@Override
		protected void setUp() throws Exception {
		}
		
		@Override
		protected void tearDown() throws Exception {
		}
		
		public void testAddOnEmptySet(Long aLong, Float aFloat) throws Exception {
			if ( null == aLong || null == aFloat ) return;
			
			Cell2<Long, Float> tc = new Cell2<Long, Float>(
					SortedBytesLong.getInstance(), SortedBytesFloat.getInstance());

			tc.add(aLong, aFloat);
			tc.sort (new CellComparator.FloatComparator<Long>());
			byte[] data = tc.toBytesOnSortedData();
			
			//Test Parsing
			Cell2<Long, Float> tcNewParsing = new Cell2<Long, Float>(
				SortedBytesLong.getInstance(), SortedBytesFloat.getInstance(),data);			
			
			tcNewParsing.parseElements();
			assertEquals(1, tcNewParsing.getMap().size());
			
			for (CellKeyValue<Long, Float> cell : tcNewParsing.getMap()) {
				assertEquals(cell.key.longValue(), aLong.longValue());
				assertEquals(cell.value.floatValue(), aFloat.floatValue());
			}

			//Find Matching
			Cell2<Long, Float> tcNewFinding = new Cell2<Long, Float>(
				SortedBytesLong.getInstance(), SortedBytesFloat.getInstance(),data);			
			
			Set<Long> all = tcNewFinding.keySet(aFloat);
			
			assertEquals(1, all.size());
			assertEquals(all.iterator().next().longValue(), aLong.longValue());
		
		}

		public void testSubsequentAdd(Integer aInt, String aString) throws Exception {	
			if ( null == aString || null == aInt ) return;
			
			Cell2<Integer, byte[]> tc = new Cell2<Integer, byte[]>(
					SortedBytesInteger.getInstance(), SortedBytesArray.getInstance());

			tc.add(aInt, aString.getBytes());
			tc.add(46, "new46".getBytes());
			
			tc.sort (new CellComparator.BytesComparator<Integer>());
			byte[] data = tc.toBytesOnSortedData();
			
			Cell2<Integer, byte[]> tcNewFinding = new Cell2<Integer, byte[]>(
					SortedBytesInteger.getInstance(), SortedBytesArray.getInstance(), data);
			
			List<byte[]> foundValues = new ArrayList<byte[]>();
			tcNewFinding.values("new46".getBytes(), foundValues);
			assertEquals(foundValues.size(), 1);
			for (byte[] bs : foundValues) {
				assertEquals("new46" , new String(bs));
			}

			Collection<Integer> foundKeys = tcNewFinding.keySet("new46".getBytes() );
			assertEquals(foundValues.size(), 1);
			for (Integer i : foundKeys) {
				assertEquals( 46, i.intValue());
			}

			tcNewFinding.parseElements();
			
			for (CellKeyValue<Integer, byte[]> elem : tcNewFinding.getMap()) {
				assertTrue(elem.key == aInt.intValue() || elem.key == 46);
				assertTrue( "new46".equals(new String(elem.value)) || aString.equals(new String(elem.value))  );
			}

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
