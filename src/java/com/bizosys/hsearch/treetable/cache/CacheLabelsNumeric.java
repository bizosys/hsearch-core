package com.bizosys.hsearch.treetable.cache;

import java.util.HashMap;
import java.util.Map;

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
public final class CacheLabelsNumeric {

	public static class ValueRange {
		
		public double minVal = Long.MAX_VALUE;
		public double maxVal = Long.MIN_VALUE;

		public ValueRange(double minVal, double maxVal) {
			this.minVal = minVal;
			this.maxVal = maxVal;
		}
	}
	
	public static class L1Cache<L1> {
		public Map<L1, ValueRange> l1cache = new HashMap<L1, ValueRange>();
		public final void put (L1 l1, double val) {
			
			if ( l1cache.containsKey(l1)) {
				ValueRange valRange = l1cache.get(l1);
				if ( valRange.minVal > val) valRange.minVal = val; 
				if ( valRange.maxVal < val) valRange.maxVal = val; 
			} else {
				l1cache.put(l1, new ValueRange(val, val));
			}
		}
	}
	
	public static class L2Cache<L1, L2> {
		public Map<L1, Map<L2, ValueRange> > l2cache = new HashMap<L1, Map<L2,ValueRange>>();
		public final void put (L1 l1, L2 l2, double val) {
			
			if ( l2cache.containsKey(l1)) {
				Map<L2, ValueRange> l1M = l2cache.get(l1);
				if ( l1M.containsKey(l2)) {
					ValueRange valRange = l1M.get(l2);
					if ( valRange.minVal > val) valRange.minVal = val; 
					if ( valRange.maxVal < val) valRange.maxVal = val; 
				} else {
					l1M.put(l2, new ValueRange(val, val));
					l2cache.put(l1, l1M);
				}
			} else {
				Map<L2, ValueRange> l1M = new HashMap<L2, ValueRange>();
				l1M.put(l2, new ValueRange(val, val));
				l2cache.put(l1, l1M);
			}
		}
	}
	
	public static class L3Cache<L1, L2, L3> {
		public Map<L1, Map<L2, Map<L3, ValueRange>> > l3cache = new HashMap<L1, Map<L2,Map<L3, ValueRange>>>();
		public final void put (L1 l1, L2 l2, L3 l3, double val) {
			
			if ( l3cache.containsKey(l1)) {
				Map<L2, Map<L3, ValueRange>> l1M = l3cache.get(l1);
				if ( l1M.containsKey(l2)) {
					Map<L3, ValueRange> l2M = l1M.get(l2);
					if ( l2M.containsKey(l3)) {
						ValueRange valRange = l2M.get(l3);
						if ( valRange.minVal > val) valRange.minVal = val; 
						if ( valRange.maxVal < val) valRange.maxVal = val; 
					} else {
						l2M.put(l3, new ValueRange(val, val));
					}
				} else {
					Map<L3, ValueRange> l2M = new HashMap<L3, ValueRange>();
					l2M.put(l3, new ValueRange(val, val));
					l1M.put(l2, l2M);
				}
			} else {
				Map<L2, Map<L3, ValueRange>> l1M = new HashMap<L2, Map<L3,ValueRange>>();
				Map<L3, ValueRange> l2M = new HashMap<L3, ValueRange>();
				l2M.put(l3, new ValueRange(val, val));
				l1M.put(l2, l2M);
				l3cache.put(l1, l1M);
			}
		}
	}
}
