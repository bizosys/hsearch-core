package com.bizosys.hsearch.byteutils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.oneline.ferrari.TestAll;

public class SortedBytesStringTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method" };
	public static String mode = modes[2];

	public static void main(String[] args) throws Exception {
		SortedBytesStringTest t = new SortedBytesStringTest();

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
		List<String> sortedList = new ArrayList<String>();
		sortedList.add(new String("first"));
		sortedList.add(new String("test"));
		sortedList.add(new String("test"));
		sortedList.add(new String("cos"));
		sortedList.add(new String("cos"));
		sortedList.add(new String("sin"));
		sortedList.add(new String("tan"));
		sortedList.add(new String(""));

		Collections.sort(sortedList);
		System.out.println(sortedList);
		byte[] bytes = SortedBytesString.getInstance().toBytes(sortedList);
		List<Integer> positions = new ArrayList<Integer>();
		SortedBytesString.getInstance().parse(bytes).getEqualToIndexes("test", positions);
		System.out.println(positions.toString());
		assertNotNull(positions);
		for (int pos : positions) {
			assertEquals(SortedBytesString.getInstance().parse(bytes).getValueAt(pos), "test");
		}
		assertEquals(2, positions.size());
	}
	
	public void testSingleEqual()throws Exception{
		List<String> sortedList = new ArrayList<String>();
		sortedList.add(new String("first"));
		sortedList.add(new String("test"));
		sortedList.add(new String("test"));
		sortedList.add(new String("cos"));
		sortedList.add(new String("cos"));

		Collections.sort(sortedList);
		System.out.println(sortedList);
		byte[] bytes = SortedBytesString.getInstance().toBytes(sortedList);

		int pos = SortedBytesString.getInstance().parse(bytes).getEqualToIndex("cos");
		System.out.println(pos);
		assertEquals(SortedBytesString.getInstance().parse(bytes).getValueAt(pos), "cos");
	}
	
	public void testAddAll()throws Exception{
		SortedBytesString sbs = (SortedBytesString) SortedBytesString.getInstance();

		List<String> sortedList = new ArrayList<String>();
		sortedList.add(new String("first"));
		sortedList.add(new String("test"));
		sortedList.add(new String());
		sortedList.add(new String("test"));
		sortedList.add(new String("cos"));
		sortedList.add(new String("cos"));
		
		Collections.sort(sortedList);
		System.out.println(sortedList);
		byte[] bytes = sbs.toBytes(sortedList);
		sbs.parse(bytes);
		List<String> returnVals = new ArrayList<String>();
		sbs.addAll(returnVals);
		System.out.println("returned vals: "+returnVals.toString()+" and size is:"+returnVals.size());
		
		assertEquals(sbs.getSize(), 6);
	}
}
