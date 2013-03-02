package com.bizosys.hsearch.treetable.storage;

import java.io.IOException;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface  HSearchGenericCoprocessor extends CoprocessorProtocol {
    
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
	long[] getCount(byte[][] family, byte[][] cols, HSearchGenericFilter filter) throws IOException;

	/**
	 * For Multi query, it sends the sum of all matched values
	 * double1 - Multi query values summation
	 * double2 - Part query1 summation
	 * double3 - Part query2 summation
	 * @param filter
	 * @return
	 * @throws IOException
	 */
	double[] getSum(byte[][] family, byte[][] cols, HSearchGenericFilter filter) throws IOException;
	
	/**
	 * For Multi query, it sends the sum of all matched values
	 * double1 - double2 : Multi query MAX-MIN range
	 * double3 - double4 : Part query1 range
	 * double5 - double6 : Part query2 range
	 * @param filter
	 * @return
	 * @throws IOException
	 */
	double[] getMaxMin(byte[][] family, byte[][] cols, HSearchGenericFilter filter) throws IOException;
	
    /**
     * Get Matching rows - All columns are included 
     * @param filter
     * @return
     * @throws IOException
     */
	byte[][] getRows(byte[][] family, byte[][] cols, HSearchGenericFilter filter) throws IOException;

	/**
     * Get Matching Ids - Keys only included 
	 * @param filter
	 * @return
	 * @throws IOException
	 */
	byte[][] getIds(byte[][] family, byte[][] cols, HSearchGenericFilter filter) throws IOException;
    
	/**
	 * Get Matching Values - Values only included
	 * @param filter
	 * @return
	 * @throws IOException
	 */
	byte[][] getValues(byte[][] family, byte[][] cols, HSearchGenericFilter filter) throws IOException;
    
}
