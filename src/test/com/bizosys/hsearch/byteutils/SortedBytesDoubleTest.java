package com.bizosys.hsearch.byteutils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.oneline.ferrari.TestAll;

public class SortedBytesDoubleTest extends TestCase{

	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[2];  
	
	public static void main(String[] args) throws Exception {
		SortedBytesDoubleTest t = new SortedBytesDoubleTest();
		
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
		
		List<Double> sortedList = new ArrayList<Double>();
		sortedList.add(new Double(0.0));
		sortedList.add(new Double(2.1));
		sortedList.add(new Double(12.3));
		sortedList.add(new Double(10.0));
		sortedList.add(new Double(10.0));
		sortedList.add(new Double(15.1));
		sortedList.add(new Double(18));
		
		for ( int i=0; i<1000; i++) {
			sortedList.add(new Double(i* 1.0));
		}
		
		Collections.sort(sortedList);
		byte[] bytes = SortedBytesDouble.getInstance().toBytes(sortedList);
		List<Integer> positions = new ArrayList<Integer>();
		SortedBytesDouble.getInstance().parse(bytes).getEqualToIndexes(10.0, positions);
		
		assertNotNull(positions);
		assertEquals(3, positions.size());
		for (int pos : positions) {
			assertEquals(SortedBytesDouble.getInstance().parse(bytes).getValueAt(pos), 10.0 );
		}
	}
	
	public void testLessthan() throws Exception {	
		List<Double> sortedList = new ArrayList<Double>();
		for ( int i=0; i<1000; i++) {
			sortedList.add(new Double(i));
		}
		
		Collections.sort(sortedList);
		
		byte[] bytes = SortedBytesDouble.getInstance().toBytes(sortedList);
		List<Integer> positions = new ArrayList<Integer>();
		SortedBytesDouble.getInstance().parse(bytes).getLessThanIndexes(130.0, positions);
		
		//System.out.println(positions.toString());
		assertNotNull(positions);
		assertTrue(! positions.contains(130) );
		assertTrue(!positions.contains(999) );
		assertTrue(positions.contains(129) );
		assertTrue(positions.contains(0) );
		assertTrue(!positions.contains(-1) );
		
		assertEquals(130, positions.size());
		
		for (int pos : positions) {
			assertTrue( (SortedBytesDouble.getInstance().parse(bytes).getValueAt(pos) < 130.0) );
		}
	}		

	public void testLessthanMultiValue() throws Exception {
		
		List<Double> sortedList = new ArrayList<Double>();
		sortedList.add(new Double(0.0));
		sortedList.add(new Double(2.1));
		sortedList.add(new Double(5.0));
		sortedList.add(new Double(5.0));
		sortedList.add(new Double(12.3));
		sortedList.add(new Double(10.0));
		sortedList.add(new Double(10.0));
		sortedList.add(new Double(15.1));
		sortedList.add(new Double(18.7));
		
		for ( int i=0; i<1000; i++) {
			sortedList.add(new Double(i* 1.0));
		}
		
		Collections.sort(sortedList);
		
		byte[] bytes = SortedBytesDouble.getInstance().toBytes(sortedList);
		
		List<Integer> positions = new ArrayList<Integer>();
		SortedBytesDouble.getInstance().parse(bytes).getLessThanIndexes(10.0, positions);
		
		assertNotNull(positions);
		assertEquals(14, positions.size());
		
		for (int pos : positions) {
			assertTrue( (SortedBytesDouble.getInstance().parse(bytes).getValueAt(pos) < 10.0) );
		}
	}				
	public void testLessthanEqual() throws Exception {	
		List<Double> sortedList = new ArrayList<Double>();
		
		for ( int i=0; i<1000; i++) {
			sortedList.add(new Double(i * 1.0));
		}
		
		Collections.sort(sortedList);
		byte[] bytes = SortedBytesDouble.getInstance().toBytes(sortedList);
		List<Integer> positions = new ArrayList<Integer>();
		SortedBytesDouble.getInstance().parse(bytes).getLessThanEqualToIndexes(700.0, positions);
		
		assertNotNull(positions);
		assertEquals(701, positions.size());
		for (int pos : positions) {
			assertTrue( (SortedBytesDouble.getInstance().parse(bytes).getValueAt(pos) <= 700) );
		}
	}
	
	public void testGreaterthan() throws Exception {	
		List<Double> sortedList = new ArrayList<Double>();
		
		for ( int i=0; i<1000; i++) {
			sortedList.add(new Double(i * 1.0));
		}
		
		Collections.sort(sortedList);
		
		byte[] bytes = SortedBytesDouble.getInstance().toBytes(sortedList);
		List<Integer> positions = new ArrayList<Integer>();
		SortedBytesDouble.getInstance().parse(bytes).getGreaterThanIndexes(121.0, positions);
		
		assertNotNull(positions);
		assertTrue(positions.contains(498) );
		assertTrue(positions.contains(499) );
		assertTrue(positions.contains(500) );
		assertTrue(positions.contains(999) );
		assertTrue(!positions.contains(121) );
		assertTrue(!positions.contains(120) );
		assertEquals(878, positions.size());
		for (int pos : positions) {
			assertTrue( (SortedBytesDouble.getInstance().parse(bytes).getValueAt(pos) > 121) );
		}
	}		
	
	public void testGreaterthanEqual() throws Exception {	
		List<Double> sortedList = new ArrayList<Double>();
		
		for ( int i=0; i<1000; i++) {
			sortedList.add(new Double(i * 1.0));
		}
		
		Collections.sort(sortedList);
		
		byte[] bytes = SortedBytesDouble.getInstance().toBytes(sortedList);
		List<Integer> positions = new ArrayList<Integer>() ;
		SortedBytesDouble.getInstance().parse(bytes).getGreaterThanEqualToIndexes(800.0, positions);
		
		assertNotNull(positions);
		assertTrue(positions.contains(801) );
		assertTrue(positions.contains(999));
		assertTrue(positions.contains(800));
		assertEquals(200, positions.size());
		for (int pos : positions) {
			assertTrue( (SortedBytesDouble.getInstance().parse(bytes).getValueAt(pos) >= 800) );
		}
	}

	public void testRangeCheck() throws Exception {

		List<Double> sortedList = new ArrayList<Double>();
		sortedList.add(234.2);
		sortedList.add(201.0);
		sortedList.add(250.0);
		sortedList.add(239.2);
		sortedList.add(299.2);
		sortedList.add(301.0);
		sortedList.add(367.9);
		Collections.sort(sortedList);

		byte[] inputData = SortedBytesDouble.getInstance().toBytes(sortedList);
		List<Integer> positions = new ArrayList<Integer>();
		SortedBytesDouble.getInstance().parse(inputData).getRangeIndexesInclusive(201.0, 250.0, positions);

		for (Integer aPos : positions) {
			System.out.println(sortedList.get(aPos) );
		}
		
		assertEquals(4, positions.size());
		for (Integer aPos : positions) {
			boolean isGood = (sortedList.get(aPos) >= 201.0 && sortedList.get(aPos) <= 250.0);
			assertTrue(isGood);
		}
	}			

}
