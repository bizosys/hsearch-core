package com.bizosys.hsearch.byteutils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.oneline.ferrari.TestAll;

public class SortedBytesCharTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[2];  
	
	public static void main(String[] args) throws Exception {
		SortedBytesCharTest t = new SortedBytesCharTest();
		
		if ( modes[0].equals(mode) ) {
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) {
	        TestFerrari.testRandom(t);
	        
		} else if  ( modes[2].equals(mode) ) {
			t.setUp();
			t.testEqualWithOffset();
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
		List<Byte> sortedList = new ArrayList<Byte>();
		sortedList.add(new Byte((byte) 'a'));
		sortedList.add(new Byte((byte) 'c'));
		sortedList.add(new Byte((byte) 'a'));
		sortedList.add(new Byte((byte) 'f'));
		sortedList.add(new Byte((byte) 'z'));
		sortedList.add(new Byte((byte) 'y'));
		sortedList.add(new Byte((byte) 'z'));
		
		Collections.sort(sortedList);
		
		byte[] bytes = SortedBytesChar.getInstance().toBytes(sortedList);
		List<Integer> positions = new ArrayList<Integer>();  
		SortedBytesChar.getInstance().parse(bytes).getEqualToIndexes((byte)'z', positions);
		
		assertNotNull(positions);
		for (int pos : positions) {
			assertEquals(SortedBytesChar.getInstance().parse(bytes).getValueAt(pos).intValue(), (byte)'z' );
		}
		assertEquals(2, positions.size());
	}
	public void testEqualWithOffset() throws Exception {	
		List<Byte> sortedList = new ArrayList<Byte>();
		sortedList.add(new Byte((byte) 'a'));
		sortedList.add(new Byte((byte) 'c'));
		sortedList.add(new Byte((byte) 'a'));
		sortedList.add(new Byte((byte) 'f'));
		sortedList.add(new Byte((byte) 'z'));
		sortedList.add(new Byte((byte) 'y'));
		sortedList.add(new Byte((byte) 'z'));
		
		Collections.sort(sortedList);
		
		byte[] bytes = SortedBytesChar.getInstance().toBytes(sortedList);
		byte[] finalbytes = new byte[bytes.length + 4];
		System.arraycopy(bytes, 0, finalbytes, 4, bytes.length);
		
		int foundLoc = SortedBytesChar.getInstance().parse(finalbytes,4,finalbytes.length - 4).
				getEqualToIndex((byte)'z');
		
		assertTrue(foundLoc != -1);
		System.out.println(foundLoc);
		int foundVal = SortedBytesChar.getInstance().parse(
			finalbytes,4,finalbytes.length - 4).getValueAt(foundLoc);
		assertEquals((byte)'z', foundVal );
	}		
	
	public void testLessthan() throws Exception {	
		List<Byte> sortedList = new ArrayList<Byte>();
		sortedList.add(new Byte((byte) 'a'));
		sortedList.add(new Byte((byte) 'c'));
		sortedList.add(new Byte((byte) 'a'));
		sortedList.add(new Byte((byte) 'f'));
		sortedList.add(new Byte((byte) 'z'));
		sortedList.add(new Byte((byte) 'y'));
		sortedList.add(new Byte((byte) 'z'));

		Collections.sort(sortedList);
		
		byte[] bytes = SortedBytesChar.getInstance().toBytes(sortedList);
		List<Integer> positions = new ArrayList<Integer>();  
		SortedBytesChar.getInstance().parse(bytes).getLessThanIndexes((byte)'f', positions);
		
		assertNotNull(positions);
		assertTrue(positions.contains(0));
		assertTrue(!positions.contains(7) );
				
		for (int pos : positions) {
			assertTrue( (SortedBytesChar.getInstance().parse(bytes).getValueAt(pos) < (byte)'f') );
		}
	}			
	
	public void testLessthanEqual() throws Exception {	

		List<Byte> sortedList = new ArrayList<Byte>();
		sortedList.add(new Byte((byte) 'a'));
		sortedList.add(new Byte((byte) 'c'));
		sortedList.add(new Byte((byte) 'a'));
		sortedList.add(new Byte((byte) 'f'));
		sortedList.add(new Byte((byte) 'z'));
		sortedList.add(new Byte((byte) 'y'));
		sortedList.add(new Byte((byte) 'z'));
		Collections.sort(sortedList);
		
		byte[] bytes = SortedBytesChar.getInstance().toBytes(sortedList);
		List<Integer> positions = new ArrayList<Integer>();
		SortedBytesChar.getInstance().parse(bytes).getLessThanEqualToIndexes((byte)'y', positions);
		
		assertNotNull(positions);
		assertEquals(5, positions.size());
		for (int pos : positions) {
			assertTrue( (SortedBytesChar.getInstance().parse(bytes).getValueAt(pos) <= (byte)'y') );
		}
	}			
	
	public void testGreaterthan() throws Exception {	
		List<Byte> sortedList = new ArrayList<Byte>();
		sortedList.add(new Byte((byte) 'a'));
		sortedList.add(new Byte((byte) 'c'));
		sortedList.add(new Byte((byte) 'a'));
		sortedList.add(new Byte((byte) 'f'));
		sortedList.add(new Byte((byte) 'z'));
		sortedList.add(new Byte((byte) 'y'));
		sortedList.add(new Byte((byte) 'z'));

		Collections.sort(sortedList);
		
		byte[] bytes = SortedBytesChar.getInstance().toBytes(sortedList);
		List<Integer> positions = new ArrayList<Integer>(); 
		SortedBytesChar.getInstance().parse(bytes).getGreaterThanEqualToIndexes((byte)'c', positions);
		
		assertNotNull(positions);
		assertTrue(positions.contains(6));
		assertTrue(!positions.contains(0));
		assertEquals(5, positions.size());
		for (int pos : positions) {
			assertTrue( (SortedBytesChar.getInstance().parse(bytes).getValueAt(pos) >= (byte)'c') );
		}
	}		
	
	
	
}
