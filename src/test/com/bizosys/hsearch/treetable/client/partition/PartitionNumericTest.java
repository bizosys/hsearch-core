package com.bizosys.hsearch.treetable.client.partition;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.oneline.ferrari.TestAll;

public class PartitionNumericTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[1];  
	
	public static void main(String[] args) throws Exception {
		PartitionNumericTest t = new PartitionNumericTest();
		
		if ( modes[0].equals(mode) ) {
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) {
	        TestFerrari.testRandom(t);
	        
		} else if  ( modes[2].equals(mode) ) {
			t.setUp();
			t.testPartitionPoints();
			t.tearDown();
		}
	}

	@Override
	protected void setUp() throws Exception {
	}
	
	@Override
	protected void tearDown() throws Exception {
	}
	

	public void testPartitionPoints() throws Exception {
		PartitionNumeric part = new PartitionNumeric();
		//part.setPartitionsAndRange(colName, familyNames, ranges, partitionIndex)
	}
}