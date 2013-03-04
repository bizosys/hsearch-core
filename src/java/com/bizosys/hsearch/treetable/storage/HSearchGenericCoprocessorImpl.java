package com.bizosys.hsearch.treetable.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

public class HSearchGenericCoprocessorImpl extends BaseEndpointCoprocessor
		implements HSearchGenericCoprocessor {
	
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
	public long[] getCount(byte[][] families, byte[][] cols, HSearchGenericFilter filter) throws IOException {
		
		System.out.println("> " + Thread.currentThread().getName() + "@ coprocessor : getCount");
		InternalScanner scanner = null;

		try {

			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(500);
			scan.setMaxVersions(1);
			int familiesT = families.length;
			
			for (int i=0; i<familiesT; i++) {
				System.out.println("> " + Thread.currentThread().getName() + "@ adding family " + families[i]);
				scan = scan.addColumn(families[i], cols[i]);
			}
			
			if ( null != filter) scan = scan.setFilter(filter);

			RegionCoprocessorEnvironment environment = (RegionCoprocessorEnvironment) getEnvironment();

			scanner = environment.getRegion().getScanner(scan);
			
			List<KeyValue> curVals = new ArrayList<KeyValue>();
			boolean done = false;
			
			long[] queryPartCountsWithTotallingAtTop = new long[filter.getTotalQueryParts() + 1];
			Arrays.fill(queryPartCountsWithTotallingAtTop, 0);
			
			do {
				curVals.clear();
				done = scanner.next(curVals);
				for (KeyValue kv : curVals) {
					byte[] input = kv.getValue();
					if ( null == input) continue;
					@SuppressWarnings("rawtypes")
					List foundCounts = filter.deSerializeOutput(input);
					if ( null == foundCounts) continue;
					int i=0;
					for (Object countV : foundCounts) {
						Long aCount = (Long) countV;
						System.out.println( i + "A count :" + aCount);
						queryPartCountsWithTotallingAtTop[i] = queryPartCountsWithTotallingAtTop[i] + aCount.longValue();
						i++;
					}
				}
				
			} while (done);
			
			for (long l : queryPartCountsWithTotallingAtTop) 
				System.out.println("Final Counting > Value" + l);

			return queryPartCountsWithTotallingAtTop;
			
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
	 * For Multi query, it sends the sum of all matched values
	 * double1 - Multi query values summation
	 * double2 - Part query1 summation
	 * double3 - Part query2 summation
	 * @param filter
	 * @return
	 * @throws IOException
	 */
	public double[] getSum(byte[][] family, byte[][] cols, HSearchGenericFilter filter) throws IOException {
		return null;
	}
	
	/**
	 * For Multi query, it sends the sum of all matched values
	 * double1 - double2 : Multi query MAX-MIN range
	 * double3 - double4 : Part query1 range
	 * double5 - double6 : Part query2 range
	 * @param filter
	 * @return
	 * @throws IOException
	 */
	public double[] getMaxMin(byte[][] family, byte[][] cols, HSearchGenericFilter filter) throws IOException {
		return null;
	}
	
    /**
     * Get Matching rows - All columns are included 
     * @param filter
     * @return
     * @throws IOException
     */
	public byte[][] getRows(byte[][] family, byte[][] cols, HSearchGenericFilter filter) throws IOException {
		return null;
	}

	/**
     * Get Matching Ids - Keys only included 
	 * @param filter
	 * @return
	 * @throws IOException
	 */
	public byte[][] getIds(byte[][] family, byte[][] cols, HSearchGenericFilter filter) throws IOException {
		return null;
	}
    
	/**
	 * Get Matching Values - Values only included
	 * @param filter
	 * @return
	 * @throws IOException
	 */
	public byte[][] getValues(byte[][] family, byte[][] cols, HSearchGenericFilter filter) throws IOException {
    	return null;
	}

	
}
