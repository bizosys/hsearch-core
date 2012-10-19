package com.bizosys.hsearch.byteutils;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.distribution.Distribution;
import com.oneline.ferrari.TestAll;

public class DistributionTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
		public static String mode = modes[1];  
		
		public static void main(String[] args) throws Exception {
			DistributionTest t = new DistributionTest();
			
			if ( modes[0].equals(mode) ) {
				TestAll.run(new TestCase[]{t});
			} else if  ( modes[1].equals(mode) ) {
		        TestFerrari.testRandom(t);
		        
			} else if  ( modes[2].equals(mode) ) {
				t.setUp();
				//t.testFloats();
				t.tearDown();
			}
		}

		@Override
		protected void setUp() throws Exception {
		}
		
		@Override
		protected void tearDown() throws Exception {
		}
		
		public void testFloats(Float input1, Float input2, Float input3, Float input4,
				Float input5, Float input6, Float input7, Float input8,
				Float input9, Float input10, Float input11, Float input12
				) throws Exception {	

			List<Float> sortedList = new ArrayList<Float>();
			sortedList.add(input1);
			sortedList.add(input2);
			sortedList.add(input3);
			sortedList.add(input4);
			sortedList.add(input5);
			sortedList.add(input6);
			sortedList.add(input7);
			sortedList.add(input8);
			sortedList.add(input9);
			sortedList.add(input10);
			sortedList.add(input11);
			sortedList.add(input12);
			
			float[] cmpoints = Distribution.distributesFloat(sortedList, 8);
			for (float f : cmpoints) {
				System.out.print(f + "       ,      ");
			}
			System.out.println("");
			
		}
}
