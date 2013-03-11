package com.bizosys.hsearch.distribution;

import java.util.ArrayList;
import java.util.Collection;

public class DistributeDouble {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Collection<Integer> inputs = new ArrayList<Integer>();
		for ( int i=0; i<1000000;i++) inputs.add(i);
		
		int[] d = Distribution.distributesInteger(inputs, 4);
		for (int e : d) {
			System.out.println("X:" + e);
		}
	}

}
