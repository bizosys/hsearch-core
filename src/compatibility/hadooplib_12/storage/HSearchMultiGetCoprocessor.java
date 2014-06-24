/*
* Copyright 2010 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.bizosys.hsearch.treetable.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.util.HSearchConfig;
import com.bizosys.hsearch.util.HSearchLog;
import com.bizosys.hsearch.util.conf.Configuration;

public final class HSearchMultiGetCoprocessor extends BaseEndpointCoprocessor implements HSearchMultiGetCoprocessorI {
	
	public static boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();
	
	public boolean scannerBlockCaching = true;
	public int scannerBlockCachingLimit = 1;
	
	public HSearchMultiGetCoprocessor() {
		Configuration config = HSearchConfig.getInstance().getConfiguration(); 
		this.scannerBlockCaching = config.getBoolean("scanner.block.caching", true);
		this.scannerBlockCachingLimit = config.getInt("scanner.block.caching.amount", 1);
	}
	
    /**
     * Get Matching rows 
     * @param filter
     * @return
     * @throws IOException
     */
	@Override
	public byte[] getRows(final byte[][] families, final byte[][] cols, final Filter filter, final byte[][] rows) throws IOException {
		if ( DEBUG_ENABLED ) HSearchLog.l.debug( Thread.currentThread().getName() + " @ coprocessor : getRows");
		InternalScanner scanner = null;

		try {
			Scan scan = new Scan();
			scan.setCacheBlocks(scannerBlockCaching);
			scan.setCaching(scannerBlockCachingLimit);
			scan.setMaxVersions(1);
			int familiesT = families.length;
			
			for (int i=0; i<familiesT; i++) {
				if ( DEBUG_ENABLED ) HSearchLog.l.debug( Thread.currentThread().getName() + 
					" @ adding family " + new String(families[i]) + "_" + new String(cols[i]));
				scan = scan.addColumn(families[i], cols[i]);
			}
			
			RegionCoprocessorEnvironment environment = (RegionCoprocessorEnvironment) getEnvironment();
			List<KeyValue> finalVals = new ArrayList<KeyValue>();
			List<KeyValue> curVals = new ArrayList<KeyValue>();
			
			if ( null != filter) {
				scan = scan.setFilter(filter);
			}

			boolean done = false;
			for (byte[] row : rows) {
				done = false;
				scan.setStartRow(row);
				scan.setStopRow(row);

				scanner = environment.getRegion().getScanner(scan);
				do {
					curVals.clear();
					done = scanner.next(curVals);
					if ( curVals.size() == 0 ) continue;
					finalVals.addAll(curVals);
					
					KeyValue keyValue = curVals.get(0);
					System.out.println( new String ( keyValue.getRow() ));
				} while (done);
				scanner.close();
				scanner = null;
			}
			
			Cell2<byte[], byte[]> container = new Cell2<byte[], byte[]>(byte[].class, byte[].class);
			for (KeyValue keyValue : finalVals) {
				byte[] key = keyValue.getRow();
				byte[] val = keyValue.getValue();
				
				if ( null == key || null == val) continue;
				if ( key.length == 0 || val.length == 0 ) continue;
				container.add(key, val);
			}

			byte[] data = container.toBytesOnSortedData();
			
			
        	return data;
			
		} finally {
			if ( null != scanner) {
				try {
					scanner.close();
				} catch (Exception ex) {
					ex.printStackTrace(System.err);
				}
			}
		}
	}
}
