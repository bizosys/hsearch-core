package com.bizosys.hsearch.treetable;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;


public class TestAll {
	public static void main(String[] args) throws Exception {
		List<TestCase>  testcases = new ArrayList<TestCase>();
		
		testcases.add( new Cell2Test() );
		testcases.add( new Cell3Test() );
		testcases.add( new Cell4Test() );
		testcases.add( new Cell6Test() );
		testcases.add( new Cell7Test() );
		testcases.add( new SearchIndexTest() );
		
		for (TestCase t : testcases) {
			TestFerrari.testRandom(t);
		}
	}
}
