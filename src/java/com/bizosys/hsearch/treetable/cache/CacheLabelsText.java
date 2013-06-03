package com.bizosys.hsearch.treetable.cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * ----------------------------------------------------------------------
 * Cache on Aggregation
 * If the value is a string keep unique strings.
 * If the value is a number keep the range.
 * For non-cache levels, make String value as "";
 * For non-cache levels, make <<Integer.MIN_VALUE/Short.MIN_VALUE/Bytes.MIN_VALUE/Double.MIN_VALUE>>
 * We will create another table called original_table.cache without any partition.
 * Look in the cache table.. Find matching study Ids
 * On the big search, scope to the matching study ids only.  
 * ----------------------------------------------------------------------
 */
public final class CacheLabelsText {

	public static class L1Cache<L1> {
		public Map<L1, Set<String>> l1cache = new HashMap<L1, Set<String>>();
		public final void put (L1 l1, String val) {
			
			if ( l1cache.containsKey(l1)) {
				l1cache.get(l1).add(val);
			} else {
				Set<String> uniqueVal = new HashSet<String>();
				uniqueVal.add(val);
				l1cache.put(l1, uniqueVal);
			}
		}
	}
	
	public static class L2Cache<L1, L2> {
		public Map<L1, Map<L2, Set<String>> > l2cache = new HashMap<L1, Map<L2,Set<String>>>();
		public final void put (L1 l1, L2 l2, String val) {
			
			if ( l2cache.containsKey(l1)) {
				Map<L2, Set<String>> l1M = l2cache.get(l1);
				if ( l1M.containsKey(l2)) {
					Set<String> uniqueVals = l1M.get(l2);
					uniqueVals.add(val);
				} else {
					Set<String> uniqueVals = new HashSet<String>();
					uniqueVals.add(val);
					l1M.put(l2, uniqueVals);
					l2cache.put(l1, l1M);
				}
			} else {
				Map<L2, Set<String>> l1M = new HashMap<L2, Set<String>>();
				Set<String> uniqueVals = new HashSet<String>();
				uniqueVals.add(val);
				l1M.put(l2, uniqueVals);
				l2cache.put(l1, l1M);
			}
		}
	}
	
	public static class L3Cache<L1, L2, L3> {
		public Map<L1, Map<L2, Map<L3, Set<String>>> > l3cache = new HashMap<L1, Map<L2,Map<L3, Set<String>>>>();
		public final void put (L1 l1, L2 l2, L3 l3, String val) {
			
			if ( l3cache.containsKey(l1)) {
				Map<L2, Map<L3, Set<String>>> l1M = l3cache.get(l1);
				if ( l1M.containsKey(l2)) {
					Map<L3, Set<String>> l2M = l1M.get(l2);
					if ( l2M.containsKey(l3)) {
						Set<String> uniqueVals = l2M.get(l3);
						uniqueVals.add(val);
					} else {
						Set<String> uniqueVals = new HashSet<String>();
						uniqueVals.add(val);
						l2M.put(l3, uniqueVals);
					}
				} else {
					Map<L3, Set<String>> l2M = new HashMap<L3, Set<String>>();
					Set<String> uniqueVals = new HashSet<String>();
					uniqueVals.add(val);
					l2M.put(l3, uniqueVals);
					l1M.put(l2, l2M);
				}
			} else {
				Map<L2, Map<L3, Set<String>>> l1M = new HashMap<L2, Map<L3,Set<String>>>();
				Map<L3, Set<String>> l2M = new HashMap<L3, Set<String>>();
				Set<String> uniqueVals = new HashSet<String>();
				uniqueVals.add(val);
				l2M.put(l3, uniqueVals);
				l1M.put(l2, l2M);
				l3cache.put(l1, l1M);
			}
		}
	}
}
