package com.bizosys.hsearch.treetable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesBoolean;
import com.bizosys.hsearch.byteutils.SortedBytesFloat;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesLong;
import com.oneline.ferrari.TestAll;
import com.sun.org.apache.xpath.internal.operations.Bool;

public class Cell2Test extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
		public static String mode = modes[1];  
		
		public static void main(String[] args) throws Exception {
			Cell2Test t = new Cell2Test();
			
			if ( modes[0].equals(mode) ) {
				TestAll.run(new TestCase[]{t});
			} else if  ( modes[1].equals(mode) ) {
		        TestFerrari.testRandom(t);
		        
			} else if  ( modes[2].equals(mode) ) {
				t.setUp();
				t.testCallbackFiltering();
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
		
		public void testCallback() throws Exception {
			Cell2Visitor visitor = new Cell2Visitor<Integer, Float>() {
				@Override
				public void visit(Integer k, Float v) {
					System.out.println(k.toString() + "/" + v.toString());
					if ( k.intValue() == 23) {
						assertEquals(23.1F, v.floatValue());
					} else if ( k.intValue() == 24) {
						assertEquals(24.1F, v.floatValue());
					} else {
						assertTrue( k != 23 || k != 24);
					}
				}
			};
			
			Cell2<Integer, Float> table  = new Cell2<Integer, Float>(
				SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance());
			table.add(23, 23.1F);
			table.add(24, 24.1F);
			
			Cell2<Integer, Float> tableNew  = new Cell2<Integer, Float>(
					SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance(), table.toBytesOnSortedData());
			tableNew.process(visitor);
		}

		public void testCallbackResponse() throws Exception {
			Cell2Visitor visitor = new Cell2Visitor<Integer, Float>() {
				@Override public final void visit(final Integer k, final Float v) {}};
			
			Cell2<Integer, Float> table  = new Cell2<Integer, Float>(
				SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance());
			for ( int i=0; i<5000000; i++) { table.add(i, (float) (i + .1));}
			byte[] ser = table.toBytesOnSortedData();
			System.out.println("Serialization Length :" + ser.length);
			
			long start = System.currentTimeMillis();
			for ( int i=0; i<10; i++) {
				Cell2<Integer, Float> tableNew  = new Cell2<Integer, Float>(
						SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance(), ser);
				tableNew.process(visitor);
			}
			long end = System.currentTimeMillis();
			System.out.println("Response time ms :" + (end - start));
		}
		
		public void testCallbackFiltering() throws Exception {
			
			Cell2Visitor visitor = new Cell2Visitor<Integer, Float>() {
				@Override
				public void visit(Integer k, Float v) {
					System.out.println(k.toString() + "/" + v.toString());
				}
			};
			
			Cell2<Integer, Float> table  = new Cell2<Integer, Float>(
				SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance());
			for ( int i=0; i<10000000; i++) {
				table.add(i, i + .1F);
			}
			
			byte[] data = table.toBytesOnSortedData();
			
			long st = System.currentTimeMillis();
			Cell2<Integer, Float> tableNew  = new Cell2<Integer, Float>(
					SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance(), data);
			tableNew.process(null, 10F, 20F,  visitor);
			long ed = System.currentTimeMillis();
			System.out.println(ed - st);
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
			Cell2<Boolean, Boolean> tc = new Cell2<Boolean, Boolean>(
					SortedBytesBoolean.getInstance(), SortedBytesBoolean.getInstance());

			tc.add(true, true);
			tc.add(false, true);
			
			tc.sort (new CellComparator.BooleanComparator<Boolean>());
			byte[] data = tc.toBytesOnSortedData();
			
			Cell2<Boolean, Boolean> tcNewFinding = new Cell2<Boolean, Boolean>(
					SortedBytesBoolean.getInstance(), SortedBytesBoolean.getInstance(), data);
			
			List<Boolean> foundValues = new ArrayList<Boolean>();
			tcNewFinding.keySet(true,foundValues);

			for (Boolean bs : foundValues) {
				System.out.println(bs);
			}


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
