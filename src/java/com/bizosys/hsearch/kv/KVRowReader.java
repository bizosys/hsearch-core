package com.bizosys.hsearch.kv;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.kv.impl.ScalarFilter;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;

public class KVRowReader {

	
	public static final String COL_FAM = "1";
	public static final String INSTRUCTION = "kv";
	
	public static final byte[] getAllValues(final String tableName, final byte[] row, final String query, final int callBackType,  final int outputType) throws IOException {
		
		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;
		try {
			facade = HBaseFacade.getInstance();
			
			table = facade.getTable(tableName);
			
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(1);
			scan.setMaxVersions(1);
			
			scan.setStartRow(row);
			scan.setStopRow(row);

			HSearchProcessingInstruction outputTypeCode = new HSearchProcessingInstruction(callBackType, outputType, INSTRUCTION);
			ScalarFilter sf = new ScalarFilter(outputTypeCode, query);
	    	scan.setFilter(sf);
	    	
			scanner = table.getScanner(scan);
			
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				
				byte[] storedBytes = r.getValue(COL_FAM.getBytes(), new byte[]{0});
				if ( null == storedBytes) continue;
				return storedBytes;
			}
			return null;
		} catch ( IOException ex) {
			throw ex;
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
		}
	}

	public static void main(String[] args) throws Exception {
	}

}
