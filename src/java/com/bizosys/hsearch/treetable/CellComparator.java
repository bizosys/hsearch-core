package com.bizosys.hsearch.treetable;

import java.util.Comparator;

public class CellComparator {

	public static class BooleanComparator<K> implements Comparator<CellKeyValue<K, Boolean>>  { 
		@Override
		public int compare(CellKeyValue<K, Boolean> o1, CellKeyValue<K, Boolean> o2) {
			if ( o1.getValue() == o2.getValue() ) return 0;
			else return 1;
		}
	}
	
	public static class ByteComparator<K> implements Comparator<CellKeyValue<K, Byte>>  { 
		@Override
		public int compare(CellKeyValue<K, Byte> o1, CellKeyValue<K, Byte> o2) {
			if ( o1.getValue() == o2.getValue() ) return 0;
			if ( o1.getValue() < o2.getValue() ) return -1;
			else return 1;
		}
	}

	public static class ShortComparator<K> implements Comparator<CellKeyValue<K, Short>>  { 
		@Override
		public int compare(CellKeyValue<K, Short> o1, CellKeyValue<K, Short> o2) {
			if ( o1.getValue() == o2.getValue() ) return 0;
			if ( o1.getValue() < o2.getValue() ) return -1;
			else return 1;
		}
	}
	
	public static class IntegerComparator<K> implements Comparator<CellKeyValue<K, Integer>>  { 
		@Override
		public int compare(CellKeyValue<K, Integer> o1, CellKeyValue<K, Integer> o2) {
			if ( o1.getValue() == o2.getValue() ) return 0;
			if ( o1.getValue() < o2.getValue() ) return -1;
			else return 1;
		}
	}

	public static class FloatComparator<K> implements Comparator<CellKeyValue<K, Float>>  { 
		@Override
		public int compare(CellKeyValue<K, Float> o1, CellKeyValue<K, Float> o2) {
			if ( o1.getValue() == o2.getValue() ) return 0;
			if ( o1.getValue() < o2.getValue() ) return -1;
			else return 1;
		}
	}
	
	public static class LongComparator<K> implements Comparator<CellKeyValue<K, Long>>  { 
		@Override
		public int compare(CellKeyValue<K, Long> o1, CellKeyValue<K, Long> o2) {
			if ( o1.getValue() == o2.getValue() ) return 0;
			if ( o1.getValue() < o2.getValue() ) return -1;
			else return 1;
		}
	}

	public static class DoubleComparator<K> implements Comparator<CellKeyValue<K, Double>>  { 
		@Override
		public int compare(CellKeyValue<K, Double> o1, CellKeyValue<K, Double> o2) {
			if ( o1.getValue() == o2.getValue() ) return 0;
			if ( o1.getValue() < o2.getValue() ) return -1;
			else return 1;
		}
	}

	public static class StringComparator<K> implements Comparator<CellKeyValue<K, byte[]>>  { 
		@Override
		public int compare(CellKeyValue<K, byte[]> o1, CellKeyValue<K, byte[]> o2) {
			return  ( new String(o1.getValue()).compareTo(new String(o2.getValue())) );
		}
	}
	
}
