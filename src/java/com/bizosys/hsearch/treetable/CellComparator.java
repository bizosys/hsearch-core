package com.bizosys.hsearch.treetable;

import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class CellComparator {

	public static <K,V extends Comparable<? super V>> SortedSet<Map.Entry<K,V>> entriesSortedByValues(Map<K,V> map) {
        SortedSet<Map.Entry<K,V>> sortedEntries = new TreeSet<Map.Entry<K,V>>(
            new Comparator<Map.Entry<K,V>>() {
                @Override public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2) {
                    int res = e1.getValue().compareTo(e2.getValue());
                    return res != 0 ? res : 1; // Special fix to preserve items with equal values
                }
            }
        );
        sortedEntries.addAll(map.entrySet());
        return sortedEntries;
    }
	
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

	public static class BytesComparator<K> implements Comparator<CellKeyValue<K, byte[]>>  { 
		@Override
		public int compare(CellKeyValue<K, byte[]> o1, CellKeyValue<K, byte[]> o2) {
			return  ( new String(o1.getValue()).compareTo(new String(o2.getValue())) );
		}
	}
	
	public static class StringComparator<K> implements Comparator<CellKeyValue<K, String>>  { 
		@Override
		public int compare(CellKeyValue<K, String> o1, CellKeyValue<K, String> o2) {
			return  ( o1.getValue().compareTo(o2.getValue()) );
		}
	}	
	
}
