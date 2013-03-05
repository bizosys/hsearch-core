package com.bizosys.hsearch.byteutils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.oneline.ferrari.TestAll;

public class SortedBytesBooleanTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method" };
	public static String mode = modes[2];

	public static void main(String[] args) throws Exception {
		SortedBytesBooleanTest t = new SortedBytesBooleanTest();

		if (modes[0].equals(mode)) {
			TestAll.run(new TestCase[] { t });
		} else if (modes[1].equals(mode)) {
			TestFerrari.testRandom(t);

		} else if (modes[2].equals(mode)) {
			t.setUp();
			t.testAddAll();
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
		List<Boolean> sortedList = new ArrayList<Boolean>();
		sortedList.add(new Boolean("true"));
		sortedList.add(new Boolean("yes"));
		sortedList.add(new Boolean("no"));
		sortedList.add(new Boolean("true"));
		sortedList.add(new Boolean("NO"));
		sortedList.add(new Boolean(false));
		
		Collections.sort(sortedList);
		System.out.println(sortedList);
		byte[] bytes = SortedBytesBoolean.getInstance().toBytes(sortedList);
		List<Integer> positions = new ArrayList<Integer>();
		SortedBytesBoolean.getInstance().parse(bytes).getEqualToIndexes(true, positions);
		System.out.println(positions.toString());
		assertNotNull(positions);
		for (int pos : positions) {
			assertTrue(SortedBytesBoolean.getInstance().parse(bytes).getValueAt(pos));
		}
		assertEquals(2, positions.size());
	}

	public void testAddAll() throws Exception{

		List<Boolean> sortedList = new ArrayList<Boolean>();
		sortedList.add(new Boolean("true"));
		sortedList.add(new Boolean("yes"));
		sortedList.add(new Boolean("no"));
		sortedList.add(new Boolean("true"));
		sortedList.add(new Boolean("NO"));
		sortedList.add(new Boolean(false));
		
		Collections.sort(sortedList);
		System.out.println("First List :"+sortedList);
		
		SortedBytesBoolean sbb = (SortedBytesBoolean) SortedBytesBoolean.getInstance();
		byte[] bytes = sbb.toBytes(sortedList);
		sbb.parse(bytes);		
		

		List<Integer> positions = new ArrayList<Integer>();
		sbb.getEqualToIndexes(true, positions);
		System.out.println(positions.toString());
		
		List<Boolean> vals = new ArrayList<Boolean>();
		vals.add(new Boolean("true"));
		vals.add(new Boolean(true));
		sbb.addAll(vals);
		System.out.println(sbb.getSize());

		sbb.getEqualToIndexes(true, positions);
		System.out.println(positions.toString());
	}
}
