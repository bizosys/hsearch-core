package com.bizosys.hsearch.byteutils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.oneline.ferrari.TestAll;

public class SortedBytesShortTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method" };
	public static String mode = modes[1];

	public static void main(String[] args) throws Exception {
		SortedBytesShortTest t = new SortedBytesShortTest();

		if (modes[0].equals(mode)) {
			TestAll.run(new TestCase[] { t });
		} else if (modes[1].equals(mode)) {
			TestFerrari.testRandom(t);

		} else if (modes[2].equals(mode)) {
			t.setUp();
			t.testEqual();
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
		List<Short> sortedList = new ArrayList<Short>();
		sortedList.add(new Short((short) 0));
		sortedList.add(new Short((short)2));
		sortedList.add(new Short((short)12));
		sortedList.add(new Short((short)10));
		sortedList.add(new Short((short)10));
		sortedList.add(new Short((short)15));
		sortedList.add(new Short((short)18));

		for (short i = 0; i < 1000; i++) {
			sortedList.add(i);
		}
		Collections.sort(sortedList);

		byte[] bytes = SortedBytesShort.getInstance().toBytes(sortedList);
		List<Integer> positions = new ArrayList<Integer>();
		SortedBytesShort.getInstance().parse(bytes)
				.getEqualToIndexes((short)10, positions);

		assertNotNull(positions);
		for (int pos : positions) {
			assertEquals(SortedBytesShort.getInstance().parse(bytes)
					.getValueAt(pos).shortValue(), 10);
		}
		assertEquals(3, positions.size());
	}

}
