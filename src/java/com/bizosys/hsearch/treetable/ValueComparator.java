package com.bizosys.hsearch.treetable;

import java.util.Comparator;

public class ValueComparator {

	public static class FloatComparator implements Comparator<Float>  { 
		@Override
		public int compare(Float o1, Float o2) {
			if ( o1.floatValue() == o2.floatValue() ) return 0;
			if ( o1.floatValue() < o2.floatValue()) return -1;
			return 1;
		}
	}
	

}
