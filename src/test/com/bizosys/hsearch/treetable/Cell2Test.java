package com.bizosys.hsearch.treetable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesBoolean;
import com.bizosys.hsearch.byteutils.SortedBytesChar;
import com.bizosys.hsearch.byteutils.SortedBytesDouble;
import com.bizosys.hsearch.byteutils.SortedBytesFloat;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesLong;
import com.bizosys.hsearch.byteutils.SortedBytesShort;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.byteutils.Storable;
import com.bizosys.hsearch.util.HSearchLog;
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
				t.testNonUnique();
				t.tearDown();
			}
		}

		@Override
		protected void setUp() throws Exception {
		}
		
		@Override
		protected void tearDown() throws Exception {
		}
		
		public void testNonUnique() throws Exception {
	    	Cell2<Boolean, Float> ser = new Cell2<Boolean, Float>(
	        		SortedBytesBoolean.getInstance(), SortedBytesFloat.getInstance());
	    	for ( float i=0; i<10; i++) {
		    	ser.add(false, i);
	    	}
	    	ser.sort(new CellComparator.FloatComparator<Boolean>());

	    	Cell2<Boolean, Float> deser = new Cell2<Boolean, Float>(
	        		SortedBytesBoolean.getInstance(), SortedBytesFloat.getInstance(),
	        		ser.toBytesOnSortedData());
	    	
	    	final Cell2Visitor<Boolean, Float> visitor = new Cell2Visitor<Boolean, Float>() {
				@Override
				public final void visit(final Boolean k, final Float v) {
					//System.out.println(k + "-" +  v);
				}
			};
	    	
			long start = System.currentTimeMillis();
	    	deser.process(visitor);
			long end = System.currentTimeMillis();
			System.out.println(end - start);
		}
		
		
		public void testOneRow() throws Exception {
	    	Cell2<Integer, String> ser = new Cell2<Integer, String>(
	        		SortedBytesInteger.getInstance(), SortedBytesString.getInstance());
	    	ser.add(100, "AAAAAAAAAA");

	    	
	    	Cell2<Integer, String> deser = new Cell2<Integer, String>(
	        		SortedBytesInteger.getInstance(), SortedBytesString.getInstance(),
	        		ser.toBytesOnSortedData());
	    	HashMap<Integer, String> out = new HashMap<Integer, String>();
	    	deser.populate(out);
	    	System.out.println(out.toString());
		}
		
		public void testMultiRow() throws Exception {
	    	
			List<byte[]> merged = new ArrayList<byte[]>();
			for ( int i=0; i<3; i++ ) {
				Cell2<Integer, String> ser = new Cell2<Integer, String>(
		        		SortedBytesInteger.getInstance(), SortedBytesString.getInstance());
		    	ser.add(100, "AAAAAAAAAA");
		    	merged.add(ser.toBytesOnSortedData());
			}
			
			SortedBytesArray sbaSet = SortedBytesArray.getInstanceArr();
			byte[] mergedData = sbaSet.toBytes(merged);
			
			////
			
			SortedBytesArray sbaDeser = SortedBytesArray.getInstanceArr();
			sbaDeser.parse(mergedData);
			int size = sbaDeser.getSize();
			System.out.println(size);
			
        	SortedBytesArray.Reference ref = new SortedBytesArray.Reference();
        	
        	for ( int i=0; i<size; i++) {
        		sbaDeser.getValueAtReference(i,ref);
            	Cell2<Integer, String> cell2 = new Cell2<Integer, String>(
            		SortedBytesInteger.getInstance(), SortedBytesString.getInstance(),
            		new BytesSection(mergedData, ref.offset, ref.length));
            	Map<Integer, String> elems = new HashMap<Integer, String>();
            	cell2.populate(elems);
            	System.out.println("Cell2 :" + elems.toString());
        	}	
        	
		}

		public void testSorterOnlyConsrtuctor() throws Exception {
			
			Cell2<Boolean, Short> ser = new Cell2<Boolean, Short>(
					SortedBytesBoolean.getInstance(), SortedBytesShort.getInstance());

			ser.add(true, (short) 1);
			ser.add(false, (short) 2);
			
			ser.sort (new CellComparator.ShortComparator<Boolean>());
			
			//Test Parsing
			Cell2<Boolean, Short> deser = new Cell2<Boolean, Short>(
					SortedBytesBoolean.getInstance(), SortedBytesShort.getInstance(),
					ser.toBytesOnSortedData());			
			
			deser.parseElements();
			assertEquals(2, deser.getMap().size());
			
			for (CellKeyValue<Boolean, Short> cell : deser.getMap()) {
				assertTrue( ( cell.value.floatValue() == 1) || ( cell.value.floatValue() == 2));
			}
		}
		
		public void testBytesSectionConsrtuctor() throws Exception {
			
			Cell2<Short, Integer> ser = new Cell2<Short, Integer>(
					SortedBytesShort.getInstance(), SortedBytesInteger.getInstance());

			for ( int i=1; i< 100; i++ ) ser.add((short) i, i);
			ser.sort (new CellComparator.IntegerComparator<Short>());
			
			byte[] data = ser.toBytesOnSortedData();
			byte[] appendedData = new byte[data.length + 12];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 0, data.length);
			
			//Test Parsing
			Cell2<Short, Integer> deser = new Cell2<Short, Integer>(
					SortedBytesShort.getInstance(), SortedBytesInteger.getInstance(),
					new BytesSection(appendedData, 0, data.length));			
			
			deser.parseElements();
			assertEquals(99, deser.getMap().size());
			
			for (CellKeyValue<Short, Integer> cell : deser.getMap()) {
				assertTrue( ( cell.key > 0 && cell.key < 100));
				assertTrue( cell.value.shortValue() == cell.key);
			}
		}
		
		public void testGetMap() throws Exception {
			
			Cell2<Integer, Long> ser = new Cell2<Integer, Long>(
					SortedBytesInteger.getInstance(), SortedBytesLong.getInstance());

			for ( int i=1; i< 100; i++ ) ser.add(i, (long) i);
			ser.sort (new CellComparator.LongComparator<Integer>());
			
			byte[] data = ser.toBytesOnSortedData();
			
			//Test Parsing
			Cell2<Integer, Long> deser = new Cell2<Integer, Long>(
					SortedBytesInteger.getInstance(), SortedBytesLong.getInstance());			
			
			List<CellKeyValue<Integer, Long>> values = deser.getMap(data);
			
			assertEquals(values.size(), 99);
			for (CellKeyValue<Integer, Long> cell : values) {
				assertTrue( ( cell.key > 0 && cell.key < 100));
				assertTrue( cell.value.shortValue() == cell.key);
			}
		}
		
		public void testGetMapWithContainer() throws Exception {
			
			Cell2<Double, Float> ser = new Cell2<Double, Float>(
					SortedBytesDouble.getInstance(), SortedBytesFloat.getInstance());

			for ( int i=1; i< 100; i++ ) ser.add((double)i, (float) i);
			ser.sort (new CellComparator.FloatComparator<Double>());
			
			byte[] data = ser.toBytesOnSortedData();
			byte[] appendedData = new byte[12 + data.length + 12];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 12, data.length);
			BytesSection dataSection = new BytesSection(appendedData, 12, data.length);
			
			//Test Parsing
			Cell2<Double, Float> deser = new Cell2<Double, Float>(
					SortedBytesDouble.getInstance(), SortedBytesFloat.getInstance(), dataSection);			
			
			List<CellKeyValue<Double, Float>> values = new ArrayList<CellKeyValue<Double, Float>>();
			deser.getMap(values);
			assertEquals(values.size(), 99);
			
			for (CellKeyValue<Double, Float> cell : values) {
				assertTrue( ( cell.key > 0 && cell.key < 100));
				assertTrue( cell.value.intValue() == cell.key.intValue());
			}
		}

		public void testGetMapWithKVContainer() throws Exception {
			
			Cell2<String, String> ser = new Cell2<String, String>(
					SortedBytesString.getInstance(), SortedBytesString.getInstance());

			for ( int i=1; i< 100; i++ ) ser.add("k" + i, "v" + i);
			ser.sort (new CellComparator.StringComparator<String>());
			
			byte[] data = ser.toBytesOnSortedData();
			byte[] appendedData = new byte[12 + data.length];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 12, data.length);
			BytesSection dataSection = new BytesSection(appendedData, 12, data.length);
			
			//Test Parsing
			Cell2<String, String> deser = new Cell2<String, String>(
					SortedBytesString.getInstance(), SortedBytesString.getInstance(), dataSection);			
			
			List<String> keys = new ArrayList<String>();
			List<String> values = new ArrayList<String>();
			deser.getMap(keys, values);
			
			assertEquals(keys.size(), 99);
			assertEquals(values.size(), 99);
			
			assertTrue( keys.get(0).equals("k1"));
			assertTrue( values.get(0).equals("v1"));
			assertTrue( keys.get(98).equals("k99"));
			assertTrue( values.get(98).equals("v99"));
		}

		public void testFilterMapWithKVContainer() throws Exception {
			
			Cell2<Integer, Float> ser = new Cell2<Integer, Float>(
					SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance());

			for ( int i=1; i< 100; i++ ) ser.add(i, (float) i);
			ser.sort (new CellComparator.FloatComparator<Integer>());
			
			byte[] data = ser.toBytesOnSortedData();
			byte[] appendedData = new byte[12 + data.length + 12];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 12, data.length);
			BytesSection dataSection = new BytesSection(appendedData, 12, data.length);
			
			//Test Parsing
			Cell2<Integer, Float> deser = new Cell2<Integer, Float>(
					SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance(), dataSection);			
			
			List<Integer> positions = new ArrayList<Integer>();
			List<Integer> keys = new ArrayList<Integer>();
			List<Float> values = new ArrayList<Float>();
			deser.getMap(null, new Float(50), new Float(50), positions, keys, values);
			
			assertEquals(keys.size(), 1);
			assertEquals(values.size(), 1);
			
			assertTrue( keys.get(0).intValue() == 50);
			assertTrue( values.get(0).floatValue() == 50);
		}

		public void testFilterPopulate() throws Exception {
			
			Cell2<Integer, String> ser = new Cell2<Integer, String>(
					SortedBytesInteger.getInstance(), SortedBytesString.getInstance());

			for ( int i=1; i< 100; i++ ) ser.add(i, "v" + i);
			ser.sort (new CellComparator.StringComparator<Integer>());
			
			byte[] data = ser.toBytesOnSortedData();
			byte[] appendedData = new byte[12 + data.length + 12];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 12, data.length);
			BytesSection dataSection = new BytesSection(appendedData, 12, data.length);
			
			//Test Parsing
			Cell2<Integer, String> deser = new Cell2<Integer, String>(
					SortedBytesInteger.getInstance(), SortedBytesString.getInstance(), dataSection);			
			
			Map<Integer, String> elems = new HashMap<Integer, String>();
			deser.populate(elems);
			
			assertEquals(elems.size(), 99);
			Iterator<Integer> itr = elems.keySet().iterator();
			assertEquals(itr.next().intValue() , 1);
			assertEquals(elems.get(1) , "v1");
			
			assertEquals(itr.next().intValue() , 2);
			assertEquals(elems.get(2) , "v2");
		}

		public void testIndexOf() throws Exception {
			
			Cell2<byte[], String> ser = new Cell2<byte[], String>(
					SortedBytesArray.getInstance(), SortedBytesString.getInstance());

			for ( int i=1; i< 100; i++ ) ser.add( Storable.putInt(i), "v" + i);
			ser.sort (new CellComparator.StringComparator<byte[]>());
			
			byte[] data = ser.toBytesOnSortedData();
			byte[] appendedData = new byte[12 + data.length + 12];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 12, data.length);
			BytesSection dataSection = new BytesSection(appendedData, 12, data.length);
			
			//Test Parsing
			Cell2<byte[], String> deser = new Cell2<byte[], String>(
					SortedBytesArray.getInstance(), SortedBytesString.getInstance(), dataSection);			
			
			Collection<Integer> positions = deser.indexOf("v50");
			assertEquals(1, positions.size());
			Collection<String> foundVals = deser.valuesAt(positions);
			assertEquals(1, foundVals.size());
			
			assertEquals("v50", foundVals.iterator().next());
		}
		

		public void testIndexRange() throws Exception {
			
			Cell2<String, Long> ser = new Cell2<String, Long>(
					SortedBytesString.getInstance(), SortedBytesLong.getInstance());

			for ( int i=1; i< 100; i++ ) ser.add( "k" + i, (long) i);
			ser.sort (new CellComparator.LongComparator<String>());
			
			byte[] data = ser.toBytesOnSortedData();
			byte[] appendedData = new byte[12 + data.length + 12];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 12, data.length);
			BytesSection dataSection = new BytesSection(appendedData, 12, data.length);
			
			//Test Parsing
			Cell2<String, Long> deser = new Cell2<String, Long>(
					SortedBytesString.getInstance(), SortedBytesLong.getInstance(), dataSection);			
			
			Collection<Integer> positions = deser.indexOf((long)23, (long)25);
			assertEquals(3, positions.size());
			Collection<Long> foundVals = deser.valuesAt(positions);
			assertEquals(3, foundVals.size());
			
			Iterator<Long> v = foundVals.iterator();
			assertEquals((long) 25, v.next().longValue());
			assertEquals((long) 24, v.next().longValue());
			assertEquals( (long) 23, v.next().longValue());
		}
		
		public void testKeySet() throws Exception {
			
			Cell2<Long, Long> ser = new Cell2<Long, Long>(
					SortedBytesLong.getInstance(), SortedBytesLong.getInstance());

			for ( int i=1; i< 100; i++ ) ser.add( (long) i, (long) i);
			ser.sort (new CellComparator.LongComparator<Long>());
			
			byte[] data = ser.toBytesOnSortedData();
			byte[] appendedData = new byte[12 + data.length + 12];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 12, data.length);
			BytesSection dataSection = new BytesSection(appendedData, 12, data.length);
			
			//Test Parsing
			Cell2<Long, Long> deser = new Cell2<Long, Long>(
					SortedBytesLong.getInstance(), SortedBytesLong.getInstance(), dataSection);			
			
			Collection<Long> keys = deser.keySet( (long)23);
			
			assertEquals(1, keys.size());
			assertEquals((long) 23, keys.iterator().next().longValue());
		}

		public void testKeySetWithCollection() throws Exception {
			
			Cell2<Long, Long> ser = new Cell2<Long, Long>(
					SortedBytesLong.getInstance(), SortedBytesLong.getInstance());

			for ( int i=1; i< 100; i++ ) ser.add( (long) i, (long) i);
			ser.sort (new CellComparator.LongComparator<Long>());
			
			byte[] data = ser.toBytesOnSortedData();
			byte[] appendedData = new byte[12 + data.length + 12];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 12, data.length);
			BytesSection dataSection = new BytesSection(appendedData, 12, data.length);
			
			//Test Parsing
			Cell2<Long, Long> deser = new Cell2<Long, Long>(
				SortedBytesLong.getInstance(), SortedBytesLong.getInstance(), dataSection);			
			
			
			Collection<Long> keys = new ArrayList<Long>(); 
			deser.keySet( (long)99, keys);
			
			assertEquals(1, keys.size());
			assertEquals((long) 99, keys.iterator().next().longValue());

			keys.clear(); 
			deser.keySet( (long)100, keys);
			assertEquals(0, keys.size());
		
			keys.clear(); 
			deser.keySet( (long)98, (long)101, keys);
			assertEquals(2, keys.size());
			Iterator<Long> itr = keys.iterator();
			assertEquals((long) 98, itr.next().longValue());
			assertEquals((long) 99, itr.next().longValue());
		}

		public void testValuesWithCollection() throws Exception {
			
			Cell2<Double, Double> ser = new Cell2<Double, Double>(
					SortedBytesDouble.getInstance(), SortedBytesDouble.getInstance());
			
			for ( int i=1; i< 100; i++ ) ser.add( (double) i, (double) i);
			ser.sort (new CellComparator.DoubleComparator<Double>());
			
			byte[] data = ser.toBytesOnSortedData();
			byte[] appendedData = new byte[12 + data.length + 12];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 12, data.length);
			BytesSection dataSection = new BytesSection(appendedData, 12, data.length);
			
			//Test Parsing
			Cell2<Double, Double> deser = new Cell2<Double, Double>(
					SortedBytesDouble.getInstance(), SortedBytesDouble.getInstance(), dataSection);			
			
			Collection<Double> vals = deser.values();
			assertEquals(vals.size(), 99);
			assertEquals(vals.iterator().next().doubleValue(), (double) 1);

			vals = deser.values((double)1, (double)9);
			assertEquals(vals.size(), 9);
			
			Iterator<Double> itr = vals.iterator();
			for ( int i=9; i<0; i++ ) assertEquals(itr.next().doubleValue(), (double) i);
			
			vals = new ArrayList<Double>(); 
			deser.values((double) 1, vals);
			
			assertEquals(vals.size(), 1);
			assertEquals(vals.iterator().next().doubleValue(), (double) 1);
			
			vals = new ArrayList<Double>(); 
			deser.values((double) 1, (double) 200, vals);
			
			assertEquals(vals.size(), 99);
			itr = vals.iterator();
			for ( int i=99; i<0; i++ ) assertEquals(itr.next().doubleValue(), (double) i);
		}

		public void testValueAt() throws Exception {
			
			Cell2<Float, Float> ser = new Cell2<Float, Float>(
					SortedBytesFloat.getInstance(), SortedBytesFloat.getInstance());
			
			for ( int i=1; i< 100; i++ ) ser.add( (float) i, (float) i);
			ser.sort (new CellComparator.FloatComparator<Float>());
			
			byte[] data = ser.toBytesOnSortedData();
			byte[] appendedData = new byte[12 + data.length + 12];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 12, data.length);
			BytesSection dataSection = new BytesSection(appendedData, 12, data.length);
			
			//Test Parsing
			Cell2<Float, Float> deser = new Cell2<Float, Float>(
					SortedBytesFloat.getInstance(), SortedBytesFloat.getInstance(), dataSection);			
			
			Collection<Integer> foundPositions = new ArrayList<Integer>();
			foundPositions.add(0);
			
			Collection<Float> vals = deser.valuesAt(foundPositions);
			assertEquals(vals.size(), 1);
			assertEquals(vals.iterator().next().floatValue(), (float) 1);
		}

		
		public void testAddOnEmptySet(Long aLong, Float aFloat) throws Exception {
			if ( null == aLong || null == aFloat ) return;
			
			Cell2<Long, Float> tc = new Cell2<Long, Float>(
					SortedBytesLong.getInstance(), SortedBytesFloat.getInstance());

			tc.add(aLong, aFloat);
			tc.sort (new CellComparator.FloatComparator<Long>());
			byte[] data = tc.toBytesOnSortedData();
			byte[] appendedData = new byte[12 + data.length + 12];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 12, data.length);
			BytesSection dataSection = new BytesSection(appendedData, 12, data.length);
			
			//Test Parsing
			Cell2<Long, Float> tcNewParsing = new Cell2<Long, Float>(
				SortedBytesLong.getInstance(), SortedBytesFloat.getInstance(),dataSection);			
			
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
			byte[] appendedData = new byte[12 + data.length + 12];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 12, data.length);
			BytesSection dataSection = new BytesSection(appendedData, 12, data.length);
			
			Cell2<Integer, byte[]> tcNewFinding = new Cell2<Integer, byte[]>(
					SortedBytesInteger.getInstance(), SortedBytesArray.getInstance(), dataSection);
			
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
			
			byte[] data = table.toBytesOnSortedData();
			byte[] appendedData = new byte[12 + data.length + 12];
			Arrays.fill(appendedData, (byte) 0);
			System.arraycopy(data, 0, appendedData, 12, data.length);
			BytesSection dataSection = new BytesSection(appendedData, 12, data.length);
			
			
			Cell2<Integer, Float> tableNew  = new Cell2<Integer, Float>(
					SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance(), dataSection);
			tableNew.process(visitor);
		}

		public void testCallbackResponse() throws Exception {
			Cell2Visitor visitor = new Cell2Visitor<Integer, Float>() {
				@Override public final void visit(final Integer k, final Float v) {}};
			
			Cell2<Integer, Float> table  = new Cell2<Integer, Float>(
				SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance());
			for ( int i=0; i<50; i++) { table.add(i, (float) (i + .1));}
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
			for ( int i=0; i<10; i++) {
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
		
		public void toBytesOnSortedDataWithMapTest() throws Exception  {
			Map<String, String> cellL = new HashMap<String, String>();
			for ( int i=0; i< 100; i++) {
				cellL.put("key" + i, "value" + i);
			}
			Cell2<String, String> cellMap = new Cell2<String, String>(
				SortedBytesString.getInstance(), SortedBytesString.getInstance() );
			byte[] ser = cellMap.toBytesOnSortedData(cellL);
			
			Map<String, String> deserL = new HashMap<String, String>();
			
			new Cell2<String, String>(
					SortedBytesString.getInstance(), SortedBytesString.getInstance(), ser ).
					populate(deserL);
			System.out.println(deserL.toString());
			
		}
		
		
}
