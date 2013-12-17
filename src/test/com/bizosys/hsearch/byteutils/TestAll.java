package com.bizosys.hsearch.byteutils;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;


public class TestAll {
	public static void main(String[] args) throws Exception {
		List<TestCase>  testcases = new ArrayList<TestCase>();
		
		testcases.add( new SortedBytesBooleanTest() );
		testcases.add( new SortedBytesCharTest() );
		testcases.add( new SortedBytesShortTest() );
		testcases.add( new SortedBytesUnsignedShortTest() );
		testcases.add( new SortedBytesIntegerTest() );
		testcases.add( new SortedBytesDoubleTest() );
		testcases.add( new SortedBytesFloatTest() );
		testcases.add( new SortedBytesArrayTest() );
		
		for (TestCase t : testcases) {
			TestFerrari.testRandom(t);
		}
	}
}
