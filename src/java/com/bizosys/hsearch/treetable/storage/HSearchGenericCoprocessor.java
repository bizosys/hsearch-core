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
	long[] getCount(byte[][] families, byte[][] cols, HSearchGenericFilter filter) throws IOException;

	/**
	 * For Multi query, it sends the sum of all matched values
	 * double1 - Multi query values summation
	 * double2 - Part query1 summation
	 * double3 - Part query2 summation
	 * @param filter
	 * @return
	 * @throws IOException
	 */
	double[] getAggregates(byte[][] families, byte[][] cols, HSearchGenericFilter filter) throws IOException;
	
    /**
     * Get Matching rows - Ids or ID-VAL or VAL or All columns are included 
     * @param filter
     * @return
     * @throws IOException
     */
	byte[] getRows(byte[][] families, byte[][] cols, HSearchGenericFilter filter) throws IOException;

	byte[] getFacets(byte[][] families, byte[][] cols, HSearchGenericFilter filter) throws IOException;

}
