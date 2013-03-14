package com.bizosys.hsearch.treetable.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import com.bizosys.hsearch.PerformanceLogger;
import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.functions.HSearchReducer;
import com.bizosys.hsearch.hbase.HbaseLog;

public class HSearchGenericCoprocessorImpl extends BaseEndpointCoprocessor
		implements HSearchGenericCoprocessor {
	
	public static boolean DEBUG_ENABLED = HbaseLog.l.isDebugEnabled();
	public static boolean INFO_ENABLED = PerformanceLogger.l.isInfoEnabled(); 
	
	
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
			scan.setCaching(500);
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
			
			Collection<byte[]> merged = new ArrayList<byte[]>(1024);
			Collection<byte[]> append = new ArrayList<byte[]>(1024);
			
			HSearchReducer reducer = filter.getReducer();
			do {
				curVals.clear();
				done = scanner.next(curVals);
				for (KeyValue kv : curVals) {
					byte[] input = kv.getValue();
					if ( null == input) continue;
					
					if ( null != reducer) {
						filter.deserialize(input, append);
						System.out.println(append.size() + "|" + merged.size());
						reducer.appendRows(merged, kv.getRow(), append);
					}
				}
				
			} while (done);
			
			return SortedBytesArray.getInstance().toBytes(merged);
			
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
