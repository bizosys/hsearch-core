package com.bizosys.hsearch.treetable.storage;

import java.io.IOException;
import java.util.ArrayList;
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

		Scan scan = new Scan();
		scan.setCacheBlocks(true);
		scan.setCaching(500);
		scan.setMaxVersions(1);
		int familiesT = families.length;
		for (int i=0; i<familiesT; i++) {
			System.out.println("> " + Thread.currentThread().getName() + "@ adding family " + families[i]);
			scan = scan.addColumn(families[i], cols[i]);
		}
		
		if ( null != filter) {
			System.out.println("> " + Thread.currentThread().getName() + "@ setting filter");
			scan = scan.setFilter(filter);
		} else {
			System.out.println("> " + Thread.currentThread().getName() + "@ filter is not set");
		}

		RegionCoprocessorEnvironment environment = (RegionCoprocessorEnvironment) getEnvironment();

		System.out.println("> " + Thread.currentThread().getName() + "@ use an internal scanner to perform scanning.");
		InternalScanner scanner = environment.getRegion().getScanner(scan);
		System.out.println("> " + Thread.currentThread().getName() + "@ Got scanner.");
		int result = 0;
		try {
			List<KeyValue> curVals = new ArrayList<KeyValue>();
			boolean done = false;
			do {
				curVals.clear();
				done = scanner.next(curVals);
				
				System.out.println("> " + Thread.currentThread().getName() + "@ " + curVals.size());
				result = result + curVals.size();
				
			} while (done);
			
			System.out.println("> " + Thread.currentThread().getName() + "@ Total matching : " + result);
			return new long[] {result};
		} finally {
			if ( null != scanner) scanner.close();
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
