/*
 * Copyright 2010 Bizosys Technologies Limited
 *
 * Licensed to the Bizosys Technologies Limited (Bizosys) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The Bizosys licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
			return Float.compare(o1.getValue(), o2.getValue());
		}
	}

	public static class DoubleComparator<K> implements Comparator<CellKeyValue<K, Double>>  { 
		@Override
		public int compare(CellKeyValue<K, Double> o1, CellKeyValue<K, Double> o2) {
			return Double.compare(o1.getValue(), o2.getValue());
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
