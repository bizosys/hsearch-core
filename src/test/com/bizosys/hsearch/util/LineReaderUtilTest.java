package com.bizosys.hsearch.util;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.oneline.ferrari.TestAll;

public class LineReaderUtilTest  extends TestCase {
	
	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[2];  
	
	public static void main(String[] args) throws Exception {
		LineReaderUtilTest t = new LineReaderUtilTest();
		
		if ( modes[0].equals(mode) ) {
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) {
	        TestFerrari.testRandom(t);
	        
		} else if  ( modes[2].equals(mode) ) {
			t.setUp();
			t.testEndsWithDoubleSeparator();
			t.tearDown();
		}
	}

	@Override
	protected void setUp() throws Exception {
	}
	
	@Override
	protected void tearDown() throws Exception {
	}
	

	public void testEndsWithField() throws Exception {
		{
			String[] result = new String[5];
			LineReaderUtil.fastSplit(result, "1|2|3|4", '|');
			System.out.println("****[" + result[3] + "]");
		}
	
		{
			List<String> result = new ArrayList<String>();
			LineReaderUtil.fastSplit(result, "1|2|3|4", '|');
			System.out.println("****[" + result.get(3) + "]");
		}
	}
	
	public void testEndsWithSeparator() throws Exception {
		{
			String[] result = new String[5];
			LineReaderUtil.fastSplit(result, "1|2|3|4|", '|');
			System.out.println("****[" + result[4] + "]");
		}
	
		{
			List<String> result = new ArrayList<String>();
			LineReaderUtil.fastSplit(result, "1|2|3|4|", '|');
			System.out.println("****[" + result.get(4) + "]");
		}
	}	
	
	public void testBeginsWithSeparator() throws Exception {
		{
			String[] result = new String[5];
			LineReaderUtil.fastSplit(result, "|1|2|3|4", '|');
			System.out.println("****[" + result[4] + "]");
		}
	
		{
			List<String> result = new ArrayList<String>();
			LineReaderUtil.fastSplit(result, "|1|2|3|4", '|');
			System.out.println("****[" + result.get(4) + "]");
		}
	}		
	
	public void testEndsWithDoubleSeparator() throws Exception {
		{
			String[] result = new String[6];
			LineReaderUtil.fastSplit(result, "1|2|3|4||", '|');
			System.out.println("****[" + result[5] + "]");
		}
	
		{
			List<String> result = new ArrayList<String>();
			LineReaderUtil.fastSplit(result, "1|2|3|4||", '|');
			System.out.println("****[" + result.get(5) + "]");
		}
	}		
	
}
