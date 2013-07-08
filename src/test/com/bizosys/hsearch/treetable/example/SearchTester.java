package com.bizosys.hsearch.treetable.example;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.treetable.Cell2Test;
import com.bizosys.hsearch.treetable.example.impl.Client;
import com.bizosys.hsearch.treetable.example.impl.donotmodify.CountClient;
import com.bizosys.hsearch.treetable.example.impl.donotmodify.ListClient;
import com.oneline.ferrari.TestAll;

public class SearchTester extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[2];  
	
	public static void main(String[] args) throws Exception {
		SearchTester t = new SearchTester();
		
		if ( modes[0].equals(mode) ) {
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) {
	        TestFerrari.testRandom(t);
	        
		} else if  ( modes[2].equals(mode) ) {
			t.setUp();
			t.leftBoundaryTest();
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
		System.out.println("START");
		//CountClient ht = new CountClient();
		
		ListClient ht = new ListClient();
		Map<String, String> multiQueryParts = new HashMap<String, String>();
		multiQueryParts.put("ExamResult:1", "*|student|*|*|*");
		multiQueryParts.put("ExamResult:2", "*|monitor|*|*|*");
		ht.execute("ExamResult:1 OR ExamResult:2", multiQueryParts);
	}

	public void rightBoundaryTest() throws Exception {
	}
	
	public void oneQuery1PartBeforePartitionTest() throws Exception {
		Client ht = new Client();
    	//Before partition starts
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|-1");
            ht.execute("ExamResult:1", multiQueryParts);
            assertEquals(0, ht.finalOutput.size());
    	}  
	}

	public void oneQuery1FirstPartitionStartTest() throws Exception {
    	Client ht = new Client();
    	//First partition start
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|0");
            ht.execute("ExamResult:1", multiQueryParts);
            assertEquals("[22	scout	classx	0	0.0]", ht.finalOutput.toString());
    	}
	}
	
	public void oneQuery1FirstPartitionMiddleTest() throws Exception {
    	Client ht = new Client();
    	
    	//First partition middle
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|0.5");
            ht.execute("ExamResult:1", multiQueryParts);
            assertEquals("[22	monitor	classx	5	0.5]", ht.finalOutput.toString());
    	}

	}
	
	public void oneQuery1FirstPartitionEndTest() throws Exception {
    	Client ht = new Client();
    	
    	//First partition end
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|1");
            ht.execute("ExamResult:1", multiQueryParts);
            assertEquals("[22	captain	classx	10	1.0]", ht.finalOutput.toString());
    	}

	}	

	public void middlePartitionStartTest() throws Exception {
    	Client ht = new Client();
    	//Middle partition start
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|4");
            ht.execute("ExamResult:1", multiQueryParts);
            assertEquals("[22	scout	classx	40	4.0]", ht.finalOutput.toString());
    	}
    	
	}
	
	public void middlePartitionMiddleTest() throws Exception {
    	Client ht = new Client();
    	//Middle partition middle
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|4.5");
            ht.execute("ExamResult:1", multiQueryParts);
            assertEquals("[22	monitor	classx	45	4.5]", ht.finalOutput.toString());
    	}
	}

	public void middlePartionEndTest() throws Exception {
    	Client ht = new Client();
    	//Middle partition ends
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|5");
            ht.execute("ExamResult:1", multiQueryParts);
            assertEquals("[22	captain	classx	50	5.0]", ht.finalOutput.toString());
    	}    	
    	
	}

	public void endPartitionStartTest() throws Exception {
    	Client ht = new Client();
    	//End partition start
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|9");
            ht.execute("ExamResult:1", multiQueryParts);
            assertEquals("[22	captain	classx	90	9.0]", ht.finalOutput.toString());
    	}    	
	}

	public void endPartitionMiddleTest() throws Exception {
    	Client ht = new Client();
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|9.5");
            ht.execute("ExamResult:1", multiQueryParts);
            assertEquals("[22	student	classx	95	9.5]", ht.finalOutput.toString());
    	}
	}

	public void endPartitionEndTest() throws Exception {
    	Client ht = new Client();
    	//Middle partition ends
    	{
    		ht.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|10");
            ht.execute("ExamResult:1", multiQueryParts);
            assertEquals("[22	scout	classx	100	10.0]", ht.finalOutput.toString());
    	}      	
	}

	public void beyondPartitonEndTest() throws Exception {
    	Client ht = new Client();
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

	public void twoQuery1PartStartingTest() throws Exception {
    	
    	//Before partition starts
    	{
    		Client clientOR = new Client();
    		clientOR.finalOutput.clear();
        	Map<String, String> multiQueryParts = new HashMap<String, String>();
            multiQueryParts.put("ExamResult:1", "*|*|*|*|-1");
            multiQueryParts.put("ExamResult:2", "*|*|*|*|0");
           
            clientOR.execute("ExamResult:1 OR ExamResult:2", multiQueryParts);
            assertEquals(1, clientOR.finalOutput.size());
            assertEquals("[22	scout	classx	0	0.0]", clientOR.finalOutput.toString());
            
    		Client clientAND = new Client();
    		clientAND.execute("ExamResult:1 AND ExamResult:2", multiQueryParts);
            assertEquals(0, clientAND.finalOutput.size());
    	}
	}
    	

	public void twoQuery1PartFirstMiddleTest() throws Exception {
		Client clientOR = new Client();
		clientOR.finalOutput.clear();
    	Map<String, String> multiQueryParts = new HashMap<String, String>();
        multiQueryParts.put("ExamResult:1", "*|*|*|*|0");
        multiQueryParts.put("ExamResult:2", "*|*|*|*|0.6");
        
        clientOR.execute("ExamResult:1 OR ExamResult:2", multiQueryParts);
        assertEquals(2, clientOR.finalOutput.size());
        assertEquals("[23	captain	classx	6	0.6, 22	scout	classx	0	0.0]", clientOR.finalOutput.toString());
        
		Client clientAND = new Client();
        clientAND.execute("ExamResult:1 AND ExamResult:2", multiQueryParts);
        assertEquals(0, clientAND.finalOutput.size());
	}

	public void twoQuery1PartMiddleMiddleTest() throws Exception {
		Client clientOR = new Client();
		clientOR.finalOutput.clear();
    	Map<String, String> multiQueryParts = new HashMap<String, String>();
        multiQueryParts.put("ExamResult:1", "*|*|*|*|0.5");
        multiQueryParts.put("ExamResult:2", "*|*|*|*|0.6");
        
        clientOR.execute("ExamResult:1 OR ExamResult:2", multiQueryParts);
        assertEquals(2, clientOR.finalOutput.size());
        assertEquals("[23	captain	classx	6	0.6, 22	monitor	classx	5	0.5]", clientOR.finalOutput.toString());
        
		Client clientAND = new Client();
        clientAND.execute("ExamResult:1 AND ExamResult:2", multiQueryParts);
        assertEquals(0, clientAND.finalOutput.size());
	}
	
	public void twoQuery1PartMiddleEndTest() throws Exception {
		Client clientOR = new Client();
		clientOR.finalOutput.clear();
    	Map<String, String> multiQueryParts = new HashMap<String, String>();
        multiQueryParts.put("ExamResult:1", "*|*|*|*|0.5");
        multiQueryParts.put("ExamResult:2", "*|*|*|*|1.0");
        
        clientOR.execute("ExamResult:1 OR ExamResult:2", multiQueryParts);
        assertEquals(2, clientOR.finalOutput.size());
        System.out.println(clientOR.finalOutput.toString());
        assertEquals("[22	captain	classx	10	1.0, 22	monitor	classx	5	0.5]", clientOR.finalOutput.toString());
        
		Client clientAND = new Client();
        clientAND.execute("ExamResult:1 AND ExamResult:2", multiQueryParts);
        assertEquals(0, clientAND.finalOutput.size());
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

	public void notQueryAtFirstTest() throws Exception {
		Client ht = new Client();
		
  		ht.finalOutput.clear();
    	Map<String, String> multiQueryParts = new HashMap<String, String>();
        multiQueryParts.put("ExamResult:1", "*|*|*|!0|*");
        ht.execute("ExamResult:1", multiQueryParts);
        Set<Integer> expected = new HashSet<Integer>();
        for ( int i=1; i<=100; i++) expected.add(i); 
        for (String line : ht.finalOutput) {
        	int start = line.indexOf('\t', line.indexOf('\t', line.indexOf('\t')+1)+1);
            int end = ( line.indexOf('\t', start+1) );
            int foundId = Integer.parseInt(line.substring(start+1, end));
            System.out.println(foundId);
            assertTrue( expected.contains(foundId) );
            expected.remove(foundId);
		}
        assertTrue( expected.size() == 0);
        
	}
	
	public void notQueryAtLastTest() throws Exception {
		Client ht = new Client();
		
  		ht.finalOutput.clear();
    	Map<String, String> multiQueryParts = new HashMap<String, String>();
        multiQueryParts.put("ExamResult:1", "*|*|*|!100|*");
        ht.execute("ExamResult:1", multiQueryParts);
        Set<Integer> expected = new HashSet<Integer>();
        for ( int i=0; i<100; i++) expected.add(i); 
        for (String line : ht.finalOutput) {
        	int start = line.indexOf('\t', line.indexOf('\t', line.indexOf('\t')+1)+1);
            int end = ( line.indexOf('\t', start+1) );
            int foundId = Integer.parseInt(line.substring(start+1, end));
            assertTrue( expected.contains(foundId) );
            expected.remove(foundId);
		}
        assertTrue( expected.size() == 0);
	}

	public void notQueryAtMidTest() throws Exception {
		Client ht = new Client();
		
  		ht.finalOutput.clear();
    	Map<String, String> multiQueryParts = new HashMap<String, String>();
        multiQueryParts.put("ExamResult:1", "*|*|*|!50|*");
        ht.execute("ExamResult:1", multiQueryParts);
        Set<Integer> expected = new HashSet<Integer>();
        for ( int i=0; i<=100; i++) expected.add(i);
        expected.remove(50);
        for (String line : ht.finalOutput) {
        	int start = line.indexOf('\t', line.indexOf('\t', line.indexOf('\t')+1)+1);
            int end = ( line.indexOf('\t', start+1) );
            int foundId = Integer.parseInt(line.substring(start+1, end));
            assertTrue( expected.contains(foundId) );
            expected.remove(foundId);
		}
        assertTrue( expected.size() == 0);
	}

	public void threeQuery3PartTest() throws Exception {
		Client ht = new Client();
		
		{
	    	Map<String, String> multiQueryParts = new HashMap<String, String>();
	    	StringBuilder sb = new StringBuilder();

	    	multiQueryParts.put("ExamResult:1", "*|*|*|*|0");
	        ht.execute("ExamResult:1", multiQueryParts);
	        sb.append("ExamResult:1 :" + ht.finalOutput.toString() + "\n");
	    	multiQueryParts.clear();
	  		ht.finalOutput.clear();

	    	multiQueryParts.put("ExamResult:2", "*|*|*|*|0.6");
	        ht.execute("ExamResult:2", multiQueryParts);
	        sb.append("ExamResult:2 :" + ht.finalOutput.toString() + "\n");
	    	multiQueryParts.clear();
	  		ht.finalOutput.clear();

	    	multiQueryParts.put("ExamResult:3", "*|*|*|*|10");
	        ht.execute("ExamResult:3", multiQueryParts);
	        sb.append("ExamResult:3 :" + ht.finalOutput.toString() + "\n");
	    	multiQueryParts.clear();
	  		ht.finalOutput.clear();

	    	multiQueryParts.put("ExamResult:1", "*|*|*|*|!0");
	  		multiQueryParts.put("ExamResult:2", "*|*|*|*|[0.6:10.0]");
	  		multiQueryParts.put("ExamResult:3", "*|*|*|*|{0.9,10.0}");
	        multiQueryParts.put("ExamResult:4", "*|*|*|*|10.0");
	        ht.execute("ExamResult:4 AND (ExamResult:3 OR ExamResult:1 OR ExamResult:2)", multiQueryParts);
	        
	        sb.append("Final :" + ht.finalOutput.toString());
	        System.out.println(sb.toString());
	        assertEquals("[22	scout	classx	100	10.0, 22	scout	classx	100	10.0, 22	scout	classx	100	10.0, 22	scout	classx	100	10.0]", ht.finalOutput.toString());
		}
	}
}