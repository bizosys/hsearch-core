package com.bizosys.hsearch.treetable.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import com.bizosys.hsearch.PerformanceLogger;
import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.hbase.HbaseLog;

public class HSearchGenericCoprocessorImpl extends BaseEndpointCoprocessor
		implements HSearchGenericCoprocessor {
	
	public static boolean DEBUG_ENABLED = HbaseLog.l.isDebugEnabled();
	public static boolean INFO_ENABLED = PerformanceLogger.l.isInfoEnabled(); 
	
	
	/**
	 * For Multi query, it sends the total records found along with individual break up
	 * long1 - Multi query total matched
	 * long2 - Part query1 total matched
	 * long3 - Part query2 total matched
	 * ...
	 * @param filter
	 * @return
	 * @throws IOException
	 */
	public final long[] getCount(byte[][] families, byte[][] cols, HSearchGenericFilter filter) throws IOException {
		
		if ( DEBUG_ENABLED ) HbaseLog.l.debug( Thread.currentThread().getName() + " @ coprocessor : getCount");
		
		long startTime = 0L;
		if ( INFO_ENABLED ) startTime = System.currentTimeMillis();

		InternalScanner scanner = null;
		long[] queryPartCountsWithTotallingAtTop = new long[filter.getTotalQueryParts() + 1];

		try {

			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(1);
			scan.setMaxVersions(1);
			int familiesT = families.length;
			
			for (int i=0; i<familiesT; i++) {
				if ( DEBUG_ENABLED ) HbaseLog.l.debug( Thread.currentThread().getName() + 
					" @ adding family " + new String(families[i]));
				scan = scan.addColumn(families[i], cols[i]);
			}
			
			if ( null != filter) scan = scan.setFilter(filter);

			RegionCoprocessorEnvironment environment = (RegionCoprocessorEnvironment) getEnvironment();

			scanner = environment.getRegion().getScanner(scan);
			
			List<KeyValue> curVals = new ArrayList<KeyValue>();
			boolean done = false;
			
			Arrays.fill(queryPartCountsWithTotallingAtTop, 0);
			Collection<Long> foundCounts = new ArrayList<Long>();
			
			do {
				curVals.clear();
				done = scanner.next(curVals);
				for (KeyValue kv : curVals) {
					foundCounts.clear();
					byte[] input = kv.getValue();
					if ( null == input) continue;
					if ( 0 == input.length) continue;
					
					filter.deSerializeCounts(input, foundCounts);
					int i=0;
					for (Long aCount : foundCounts) {
						if ( DEBUG_ENABLED ) HbaseLog.l.debug( "Row Count : " + i + " > " + aCount.longValue());
						queryPartCountsWithTotallingAtTop[i] += aCount.longValue();
						i++;
					}
				}
				
			} while (done);
			
			if ( DEBUG_ENABLED ) {
				for (long l : queryPartCountsWithTotallingAtTop)
					HbaseLog.l.debug( "Region Counting : " + l);
			}

			return queryPartCountsWithTotallingAtTop;
			
		} finally {
			if ( null != scanner) {
				try {
					scanner.close();
				} catch (Exception ex) {
					
				}
			}
			if ( INFO_ENABLED ) {
				PerformanceLogger.l.info("HSearchGenericCoprocessorImpl|getCount|" + 
						queryPartCountsWithTotallingAtTop[0] + "|" +
						( System.currentTimeMillis() - startTime) + "ms") ;
			}

		}
	}

	/**
	 * For Multi query, it sends the aggv of all matched values
	 * double1 - double2 : Multi query aggv
	 * double3 - double4 : Part query1 aggv
	 * double5 - double6 : Part query2 aggv
	 */
	public double[] getAggregates(byte[][] families, byte[][] cols, HSearchGenericFilter filter) throws IOException {
		
		
		if ( DEBUG_ENABLED ) HbaseLog.l.debug( Thread.currentThread().getName() + " @ coprocessor : getAggregates");
		InternalScanner scanner = null;

		try {

			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(1);
			scan.setMaxVersions(1);
			int familiesT = families.length;
			
			for (int i=0; i<familiesT; i++) {
				if ( DEBUG_ENABLED ) HbaseLog.l.debug( Thread.currentThread().getName() + 
					" @ adding family " + new String(families[i]));
				scan = scan.addColumn(families[i], cols[i]);
			}
			
			if ( null != filter) scan = scan.setFilter(filter);

			RegionCoprocessorEnvironment environment = (RegionCoprocessorEnvironment) getEnvironment();

			List<KeyValue> curVals = new ArrayList<KeyValue>();
			boolean done = false;
			
			int merged = AggregateUtils.getMergedCount(filter.outputType.getOutputType());
			int resultBunch = filter.getTotalQueryParts() + 1;
			double[] queryPartAggvWithTotallingAtTop = new double[ resultBunch * merged];
			AggregateUtils.initializeDefault( filter.outputType.getOutputType(), 
				queryPartAggvWithTotallingAtTop, resultBunch);

			if ( DEBUG_ENABLED) {
				HbaseLog.l.debug("Total Query Output Records =" + queryPartAggvWithTotallingAtTop + 
					" , Queries = " + resultBunch + " , merged=" + merged);
			}
			
			List<Double> foundAggregates = new ArrayList<Double>();
			scanner = environment.getRegion().getScanner(scan);
			
			do {
				curVals.clear();
				done = scanner.next(curVals);
				for (KeyValue kv : curVals) {
					foundAggregates.clear();
					byte[] input = kv.getValue();
					if ( null == input) continue;
					filter.deSerializeAgreegates(input, foundAggregates);
					
					AggregateUtils.computeAgreegates(filter, resultBunch,
						queryPartAggvWithTotallingAtTop, foundAggregates);
				}
				
			} while (done);
			
			if ( DEBUG_ENABLED ) {
				for (double l : queryPartAggvWithTotallingAtTop)
					HbaseLog.l.debug( "Final Aggv : " + l);
			}

			return queryPartAggvWithTotallingAtTop;
			
		} finally {
			if ( null != scanner) {
				try {
					scanner.close();
				} catch (Exception ex) {
					
				}
			}
		}
	}



    /**
     * Get Matching rows 
     * @param filter
     * @return
     * @throws IOException
     */
	public byte[] getRows(byte[][] families, byte[][] cols, HSearchGenericFilter filter) throws IOException {
		if ( DEBUG_ENABLED ) HbaseLog.l.debug( Thread.currentThread().getName() + " @ coprocessor : getRows");
		InternalScanner scanner = null;

		try {

			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(1);
			scan.setMaxVersions(1);
			int familiesT = families.length;
			
			for (int i=0; i<familiesT; i++) {
				if ( DEBUG_ENABLED ) HbaseLog.l.debug( Thread.currentThread().getName() + 
					" @ adding family " + new String(families[i]));
				scan = scan.addColumn(families[i], cols[i]);
			}
			
			if ( null != filter) scan = scan.setFilter(filter);

			RegionCoprocessorEnvironment environment = (RegionCoprocessorEnvironment) getEnvironment();

			scanner = environment.getRegion().getScanner(scan);
			
			List<KeyValue> curVals = new ArrayList<KeyValue>();
			boolean done = false;
			
			Collection<byte[]> collection = new ArrayList<byte[]>(1024);
			do {
				curVals.clear();
				done = scanner.next(curVals);
				for (KeyValue kv : curVals) {
					byte[] input = kv.getValue();
					if ( null == input) continue;
					collection.add(kv.getRow());
					collection.add(input);
				}
				
			} while (done);
			
			if ( DEBUG_ENABLED ) {
				HbaseLog.l.debug( "Total Rows Packed : " + collection.size() / 2);
			}

			return SortedBytesArray.getInstance().toBytes(collection);
			
		} finally {
			if ( null != scanner) {
				try {
					scanner.close();
				} catch (Exception ex) {
					
				}
			}
		}
	}
	
    /**
     * Get Matching rows 
     * @param filter
     * @return
     * @throws IOException
     */
	public byte[] getFacets(byte[][] families, byte[][] cols, HSearchGenericFilter filter) throws IOException {
		if ( DEBUG_ENABLED ) HbaseLog.l.debug( Thread.currentThread().getName() + " @ coprocessor : getRows");
		InternalScanner scanner = null;

		try {

			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(1);
			scan.setMaxVersions(1);
			int familiesT = families.length;
			
			for (int i=0; i<familiesT; i++) {
				if ( DEBUG_ENABLED ) HbaseLog.l.debug( Thread.currentThread().getName() + 
					" @ adding family " + new String(families[i]));
				scan = scan.addColumn(families[i], cols[i]);
			}
			
			if ( null != filter) scan = scan.setFilter(filter);

			RegionCoprocessorEnvironment environment = (RegionCoprocessorEnvironment) getEnvironment();

			scanner = environment.getRegion().getScanner(scan);
			
			List<KeyValue> curVals = new ArrayList<KeyValue>();
			boolean done = false;
			
			Map<String, Integer> facets = new HashMap<String, Integer>();
			do {
				curVals.clear();
				done = scanner.next(curVals);
				for (KeyValue kv : curVals) {
					byte[] input = kv.getValue();
					if ( null == input) continue;
					filter.deSerializeFacets(input, facets);
				}
				
			} while (done);
			
			if ( DEBUG_ENABLED ) {
				HbaseLog.l.debug( "Total Facets Packed : " + facets.size());
			}

			return HSearchGenericFilter.facetsToBytes(facets);
			
		} finally {
			if ( null != scanner) {
				try {
					scanner.close();
				} catch (Exception ex) {
					
				}
			}
		}
	}
	
}
