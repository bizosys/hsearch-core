package com.bizosys.hsearch.treetable.storage;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.treetable.Cell2Test;
import com.bizosys.hsearch.treetable.storage.sampleImpl.Client;
import com.oneline.ferrari.TestAll;

public class SearchTester extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[1];  
	
	public static void main(String[] args) throws Exception {
		SearchTester t = new SearchTester();
		
		if ( modes[0].equals(mode) ) {
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) {
	        TestFerrari.testRandom(t);
	        
		} else if  ( modes[2].equals(mode) ) {
			t.setUp();
			t.threeQuery3PartMultiOrTest();
			t.tearDown();
		}
	}

	@Override
	protected void setUp() throws Exception {
	}
	
	@Override
	protected void tearDown() throws Exception {
	}
	
	public void leftBoundaryTest() throws Exception {
	}

	public void rightBoundaryTest() throws Exception {
	}

	public void oneQuery1PartTest() throws Exception {
    	Client ht = new Client();
    	
    	//Before partition starts
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|-1");
            ht.execute("ExamResult:1", multiQueryParts);
            assertEquals(0, ht.finalOutput.size());
    	}   
    	
    	//First partition start
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|0");
            ht.execute("ExamResult:1", multiQueryParts);
            assertEquals("[0]", ht.finalOutput.keySet().toString());
            assertTrue(ht.finalOutput.values().toString().endsWith("|0.0]"));
    	}
    	
    	//First partition middle
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|0.5");
            ht.execute("ExamResult:1", multiQueryParts);
            System.out.println ( ht.finalOutput.toString());
            assertEquals("[5]", ht.finalOutput.keySet().toString());
            assertTrue(ht.finalOutput.values().toString().endsWith("|0.5]"));
    	}

    	
    	//First partition end
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|1");
            ht.execute("ExamResult:1", multiQueryParts);
            assertEquals("[10]", ht.finalOutput.keySet().toString());
            assertTrue(ht.finalOutput.values().toString().endsWith("|1.0]"));
    	}

    	//Middle partition start
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|4");
            ht.execute("ExamResult:1", multiQueryParts);
            assertEquals("[40]", ht.finalOutput.keySet().toString());
            assertTrue(ht.finalOutput.values().toString().endsWith("|4.0]"));
    	}
    	
    	//Middle partition middle
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|4.5");
            ht.execute("ExamResult:1", multiQueryParts);
            assertEquals("[45]", ht.finalOutput.keySet().toString());
            assertTrue(ht.finalOutput.values().toString().endsWith("|4.5]"));
    	}
    	
    	//Middle partition ends
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|5");
            ht.execute("ExamResult:1", multiQueryParts);
            assertEquals("[50]", ht.finalOutput.keySet().toString());
            assertTrue(ht.finalOutput.values().toString().endsWith("|5.0]"));
    	}    	

    	//End partition start
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|9");
            ht.execute("ExamResult:1", multiQueryParts);
            assertEquals("[90]", ht.finalOutput.keySet().toString());
            assertTrue(ht.finalOutput.values().toString().endsWith("|9.0]"));
    	}
    	
    	//Middle partition middle
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|9.5");
            ht.execute("ExamResult:1", multiQueryParts);
            assertEquals("[95]", ht.finalOutput.keySet().toString());
            assertTrue(ht.finalOutput.values().toString().endsWith("|9.5]"));
    	}
    	
    	//Middle partition ends
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|10");
            ht.execute("ExamResult:1", multiQueryParts);
            assertEquals("[100]", ht.finalOutput.keySet().toString());
            assertTrue(ht.finalOutput.values().toString().endsWith("|10.0]"));
    	}      	
    	
    	//Beyond partition ends
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|11");
            ht.execute("ExamResult:1", multiQueryParts);
            assertEquals(0, ht.finalOutput.size());
    	}      	    	
    	
	}

	public void oneQuery2PartTest() throws Exception {
	}

	public void oneQuery3PartTest() throws Exception {
	}

	public void twoQuery1PartTest() throws Exception {
		Client ht = new Client();
    	
    	//Before partition starts
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|-1");
            multiQueryParts.put("ExamResult:2", "*|*|*|*|0");
            ht.execute("ExamResult:1 OR ExamResult:2", multiQueryParts);
            assertEquals(1, ht.finalOutput.size());
            assertEquals("[0]", ht.finalOutput.keySet().toString());

            System.out.println(ht.finalOutput.values().toString());
            assertTrue( ht.finalOutput.values().toString().endsWith("|0.0]"));
    	}   
    	
    	
    	//First partition start
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|0");
            multiQueryParts.put("ExamResult:2", "*|*|*|*|0.6");
            ht.execute("ExamResult:1 OR ExamResult:2", multiQueryParts);
            assertEquals(2, ht.finalOutput.size());
            assertEquals("[0, 6]", ht.finalOutput.keySet().toString());

            System.out.println(ht.finalOutput.values().toString());
            assertTrue( ht.finalOutput.values().toString().matches(".+0\\.0,.+0\\.6\\]"));
    	}
    	
    	//First partition middle
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|0.5");
            multiQueryParts.put("ExamResult:2", "*|*|*|*|0.6");
            ht.execute("ExamResult:1 OR ExamResult:2", multiQueryParts);
            assertEquals(2, ht.finalOutput.size());
            assertEquals("[6, 5]", ht.finalOutput.keySet().toString());

            System.out.println(ht.finalOutput.values().toString());
            assertTrue( ht.finalOutput.values().toString().matches(".+0\\.6,.+0\\.5\\]"));
    	}

    	//First partition end
    	{
       		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|0.5");
            multiQueryParts.put("ExamResult:2", "*|*|*|*|1.0");
            ht.execute("ExamResult:1 OR ExamResult:2", multiQueryParts);
            assertEquals(2, ht.finalOutput.size());
            assertEquals("[10, 5]", ht.finalOutput.keySet().toString());

            System.out.println(ht.finalOutput.values().toString());
            assertTrue( ht.finalOutput.values().toString().matches("\\[.+\\|10\\|1\\.0, .+\\|5\\|0\\.5\\]"));
    	}

	}

	public void twoQuery2PartTest() throws Exception {
	}

	public void twoQuery3PartTest() throws Exception {
	}
	
	public void threeQuery1PartTest() throws Exception {
	}

	public void threeQuery2PartTest() throws Exception {
	}
	
	public void threeQuery3PartMultiOrTest() throws Exception {
		Client ht = new Client();
		
  		ht.finalOutput.clear();
    	Map<String, String> multiQueryParts = new HashMap<String, String>();
        multiQueryParts.put("ExamResult:1", "*|*|*|*|0");
        multiQueryParts.put("ExamResult:2", "*|*|*|*|0.6");
        multiQueryParts.put("ExamResult:3", "*|*|*|*|10.0");
        ht.execute("ExamResult:1 OR ExamResult:2 OR ExamResult:3", multiQueryParts);
        System.out.println(ht.finalOutput.toString());
	}

	public void threeQuery3PartTest() throws Exception {
		Client ht = new Client();
		
		{
	  		ht.finalOutput.clear();
	    	Map<String, String> multiQueryParts = new HashMap<String, String>();
	        multiQueryParts.put("ExamResult:1", "*|*|*|*|0");
	        multiQueryParts.put("ExamResult:2", "*|*|*|*|0.6");
	        multiQueryParts.put("ExamResult:3", "*|*|*|*|10.0");
	        ht.execute("ExamResult:1 AND (ExamResult:2 OR ExamResult:3)", multiQueryParts);
	        System.out.println("Final :" + ht.finalOutput.toString());
		}

		{
	  		ht.finalOutput.clear();
	    	Map<String, String> multiQueryParts = new HashMap<String, String>();
	        multiQueryParts.put("ExamResult:1", "*|*|*|*|0");
	        multiQueryParts.put("ExamResult:2", "*|*|*|*|0.6");
	        multiQueryParts.put("ExamResult:3", "*|*|*|*|10.0");
	        ht.execute("(ExamResult:1 AND ExamResult:2) OR ExamResult:3", multiQueryParts);
	        System.out.println("Final :" + ht.finalOutput.toString());
		}

	}	

}