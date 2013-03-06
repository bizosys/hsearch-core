package com.bizosys.hsearch.treetable.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import com.bizosys.hsearch.PerformanceLogger;
import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.hbase.HbaseLog;
import com.bizosys.hsearch.treetable.client.OutputType;

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

		try {

			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(1);
			scan.setMaxVersions(1);
			int familiesT = families.length;
			
			for (int i=0; i<familiesT; i++) {
				if ( DEBUG_ENABLED ) HbaseLog.l.debug( Thread.currentThread().getName() + 
					" @ adding family " + families[i]);
				scan = scan.addColumn(families[i], cols[i]);
			}
			
			if ( null != filter) scan = scan.setFilter(filter);

			RegionCoprocessorEnvironment environment = (RegionCoprocessorEnvironment) getEnvironment();

			scanner = environment.getRegion().getScanner(scan);
			
			List<KeyValue> curVals = new ArrayList<KeyValue>();
			boolean done = false;
			
			long[] queryPartCountsWithTotallingAtTop = new long[filter.getTotalQueryParts() + 1];
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
					
					filter.deSerializeOutput(input, foundCounts);
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
			System.out.println(INFO_ENABLED);
			if ( INFO_ENABLED ) {
				PerformanceLogger.l.info("HSearchGenericCoprocessorImpl|getCount|" + ( System.currentTimeMillis() - startTime)) ;
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
			scan.setMaxVersions(1);
			int familiesT = families.length;
			
			for (int i=0; i<familiesT; i++) {
				if ( DEBUG_ENABLED ) HbaseLog.l.debug( Thread.currentThread().getName() + 
					" @ adding family " + families[i]);
				scan = scan.addColumn(families[i], cols[i]);
			}
			
			if ( null != filter) scan = scan.setFilter(filter);

			RegionCoprocessorEnvironment environment = (RegionCoprocessorEnvironment) getEnvironment();

			List<KeyValue> curVals = new ArrayList<KeyValue>();
			boolean done = false;
			
			int merged = 1;
			switch ( filter.outputType.getOutputType()) {
				case OutputType.OUTPUT_MIN:
				case OutputType.OUTPUT_MAX:
				case OutputType.OUTPUT_AVG:
				case OutputType.OUTPUT_SUM:
					merged = 1;
					break;
				case OutputType.OUTPUT_MIN_MAX:
					merged = 2;
					break;

				case OutputType.OUTPUT_MIN_MAX_AVG:
				case OutputType.OUTPUT_MIN_MAX_COUNT:
				case OutputType.OUTPUT_MIN_MAX_SUM:
					merged = 3;
					break;
					
				case OutputType.OUTPUT_MIN_MAX_AVG_COUNT:
				case OutputType.OUTPUT_MIN_MAX_SUM_AVG:
				case OutputType.OUTPUT_MIN_MAX_SUM_COUNT:
					merged = 4;
					break;
				case OutputType.OUTPUT_MIN_MAX_AVG_SUM_COUNT:
					merged = 5;
					break;
				default:
					throw new IOException("HSearchCoprocessor Not a agregate type - " + filter.outputType.toStringHumanReadable());
			}

			int resultBunch = filter.getTotalQueryParts() + 1;
			double[] queryPartAggvWithTotallingAtTop = new double[ resultBunch * merged];
			Arrays.fill(queryPartAggvWithTotallingAtTop, 0);
			
			List<Double> foundAggregates = new ArrayList<Double>();
			scanner = environment.getRegion().getScanner(scan);
			
			do {
				curVals.clear();
				done = scanner.next(curVals);
				for (KeyValue kv : curVals) {
					foundAggregates.clear();
					byte[] input = kv.getValue();
					if ( null == input) continue;
					filter.deSerializeOutput(input, foundAggregates);
					if ( null == foundAggregates) continue;
					
					switch ( filter.outputType.getOutputType()) {
						case OutputType.OUTPUT_MIN:
							computeAggregates(OutputType.OUTPUT_MIN, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 0);
							break;
						case OutputType.OUTPUT_MAX:
							computeAggregates(OutputType.OUTPUT_MAX, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 0);
							break;
						case OutputType.OUTPUT_AVG:
							computeAggregates(OutputType.OUTPUT_AVG, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 0);
							break;
						case OutputType.OUTPUT_SUM:
							computeAggregates(OutputType.OUTPUT_SUM, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 0);
							break;
						case OutputType.OUTPUT_MIN_MAX:
							computeAggregates(OutputType.OUTPUT_MIN, queryPartAggvWithTotallingAtTop, foundAggregates,resultBunch, 0);
							computeAggregates(OutputType.OUTPUT_MAX, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 1);
							break;
	
						case OutputType.OUTPUT_MIN_MAX_AVG:
							computeAggregates(OutputType.OUTPUT_MIN, queryPartAggvWithTotallingAtTop, foundAggregates,resultBunch, 0);
							computeAggregates(OutputType.OUTPUT_MAX, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 1);
							computeAggregates(OutputType.OUTPUT_AVG, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 2);
							break;
						case OutputType.OUTPUT_MIN_MAX_COUNT:
							computeAggregates(OutputType.OUTPUT_MIN, queryPartAggvWithTotallingAtTop, foundAggregates,resultBunch, 0);
							computeAggregates(OutputType.OUTPUT_MAX, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 1);
							computeAggregates(OutputType.OUTPUT_COUNT, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 2);
							break;
						case OutputType.OUTPUT_MIN_MAX_SUM:
							computeAggregates(OutputType.OUTPUT_MIN, queryPartAggvWithTotallingAtTop, foundAggregates,resultBunch, 0);
							computeAggregates(OutputType.OUTPUT_MAX, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 1);
							computeAggregates(OutputType.OUTPUT_SUM, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 2);
							break;
							
						case OutputType.OUTPUT_MIN_MAX_AVG_COUNT:
							computeAggregates(OutputType.OUTPUT_MIN, queryPartAggvWithTotallingAtTop, foundAggregates,resultBunch, 0);
							computeAggregates(OutputType.OUTPUT_MAX, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 1);
							computeAggregates(OutputType.OUTPUT_AVG, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 2);
							computeAggregates(OutputType.OUTPUT_COUNT, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 3);
							break;
						case OutputType.OUTPUT_MIN_MAX_SUM_AVG:
							computeAggregates(OutputType.OUTPUT_MIN, queryPartAggvWithTotallingAtTop, foundAggregates,resultBunch, 0);
							computeAggregates(OutputType.OUTPUT_MAX, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 1);
							computeAggregates(OutputType.OUTPUT_SUM, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 2);
							computeAggregates(OutputType.OUTPUT_AVG, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 3);
							break;
						case OutputType.OUTPUT_MIN_MAX_SUM_COUNT:
							computeAggregates(OutputType.OUTPUT_MIN, queryPartAggvWithTotallingAtTop, foundAggregates,resultBunch, 0);
							computeAggregates(OutputType.OUTPUT_MAX, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 1);
							computeAggregates(OutputType.OUTPUT_SUM, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 2);
							computeAggregates(OutputType.OUTPUT_COUNT, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 3);
							break;
						case OutputType.OUTPUT_MIN_MAX_AVG_SUM_COUNT:
							computeAggregates(OutputType.OUTPUT_MIN, queryPartAggvWithTotallingAtTop, foundAggregates,resultBunch, 0);
							computeAggregates(OutputType.OUTPUT_MAX, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 1);
							computeAggregates(OutputType.OUTPUT_AVG, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 2);
							computeAggregates(OutputType.OUTPUT_SUM, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 3);
							computeAggregates(OutputType.OUTPUT_COUNT, queryPartAggvWithTotallingAtTop, foundAggregates, resultBunch, 4);
							break;
						default:
							throw new IOException("HSearchCoprocessor Not a agregate type - " + filter.outputType.toStringHumanReadable());
					}
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
	
	private void computeAggregates(int output, double[] finalOutputValues,
			List<Double> appendValues, int resultBunch, int aggvIndex ) throws IOException {
		
		switch(output) {
			case OutputType.OUTPUT_COUNT:
				for ( int i=0; i<resultBunch; i++) {
					finalOutputValues[i] += appendValues.get(resultBunch * aggvIndex + i).doubleValue();
				}
				break;
			case OutputType.OUTPUT_AVG:
				for ( int i=0; i<resultBunch; i++) {
					finalOutputValues[i] += appendValues.get(resultBunch * aggvIndex + i).doubleValue();
					finalOutputValues[i] = finalOutputValues[i] / 2;
				}
				break;
			case OutputType.OUTPUT_MAX:
				for ( int i=0; i<resultBunch; i++) {
					double d = appendValues.get(resultBunch * aggvIndex + i).doubleValue();
					if ( finalOutputValues[i] < d )  finalOutputValues[i] = d;
				}
				break;
				
			case OutputType.OUTPUT_MIN:
				for ( int i=0; i<resultBunch; i++) {
					double d = appendValues.get(resultBunch * aggvIndex + i).doubleValue();
					if ( finalOutputValues[i] > d )  finalOutputValues[i] = d;
				}
				break;
			case OutputType.OUTPUT_SUM:
				for ( int i=0; i<resultBunch; i++) {
					finalOutputValues[i]  += appendValues.get(resultBunch * aggvIndex + i).doubleValue(); 
				}
				break;
				
			default:
				throw new IOException("Not able to process the aggv type - Generic Coprocessor" +  output);
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
			scan.setMaxVersions(1);
			int familiesT = families.length;
			
			for (int i=0; i<familiesT; i++) {
				if ( DEBUG_ENABLED ) HbaseLog.l.debug( Thread.currentThread().getName() + 
					" @ adding family " + families[i]);
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
					
					collection.add(kv.getKey());
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
	
}
