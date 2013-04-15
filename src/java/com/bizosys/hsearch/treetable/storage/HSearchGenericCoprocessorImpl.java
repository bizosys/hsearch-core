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
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.functions.HSearchReducer;
import com.bizosys.hsearch.hbase.HbaseLog;
import com.bizosys.hsearch.util.HSearchConfig;
import com.bizosys.hsearch.util.conf.Configuration;

public final class HSearchGenericCoprocessorImpl extends BaseEndpointCoprocessor
		implements HSearchGenericCoprocessor {
	
	public static boolean DEBUG_ENABLED = HbaseLog.l.isDebugEnabled();
	public static boolean INFO_ENABLED = HbaseLog.l.isInfoEnabled();
	
	private Configuration config = HSearchConfig.getInstance().getConfiguration(); 

	private boolean internalScannerBlockCaching = true;
	private int internalScannerBlockCachingAmount = 1;
	
	public HSearchGenericCoprocessorImpl() {
		this.internalScannerBlockCaching = config.getBoolean("internal.scanner.block.caching", true); 
		this.internalScannerBlockCachingAmount = config.getInt("internal.scanner.block.caching.amount", 1); 
	}
	
    /**
     * Get Matching rows 
     * @param filter
     * @return
     * @throws IOException
     */
	public byte[] getRows(final byte[][] families, final byte[][] cols, final HSearchGenericFilter filter) throws IOException {
		if ( DEBUG_ENABLED ) HbaseLog.l.debug( Thread.currentThread().getName() + " @ coprocessor : getRows");
		InternalScanner scanner = null;
		long monitorStartTime = 0L; 
		long overallStartTime = System.currentTimeMillis(); 

		try {
			Scan scan = new Scan();
			scan.setCacheBlocks(internalScannerBlockCaching);
			scan.setCaching(internalScannerBlockCachingAmount);
			scan.setMaxVersions(1);
			int familiesT = families.length;
			
			for (int i=0; i<familiesT; i++) {
				if ( DEBUG_ENABLED ) HbaseLog.l.debug( Thread.currentThread().getName() + 
					" @ adding family " + new String(families[i]) + "_" + new String(cols[i]));
				//scan = scan.addColumn(families[i], cols[i]);
				scan = scan.addFamily(families[i]);
			}
			
			if ( null != filter) {
				FilterList filterL = filter.getFilters();
				if ( null != filterL) scan = scan.setFilter(filterL);
				else scan = scan.setFilter(filter);
			}

			RegionCoprocessorEnvironment environment = (RegionCoprocessorEnvironment) getEnvironment();

			scanner = environment.getRegion().getScanner(scan);
			
			List<KeyValue> curVals = new ArrayList<KeyValue>();
			boolean done = false;
			
			Collection<byte[]> finalOutput = new ArrayList<byte[]>();
			Collection<byte[]> partOutput = new ArrayList<byte[]>();
			
			HSearchReducer reducer = filter.getReducer();
			filter.configure();
			do {
				curVals.clear();
				partOutput.clear();
				
				done = scanner.next(curVals);
				for (KeyValue kv : curVals) {
					byte[] input = kv.getValue();
					if ( null == input) continue;
					
					if ( null != reducer) {
						filter.deserialize(input, partOutput);
						
						if ( INFO_ENABLED ) {
							monitorStartTime = System.currentTimeMillis();
						}	
						
						reducer.appendRows(finalOutput, kv.getRow(), partOutput);
						
						if ( INFO_ENABLED ) {
							filter.pluginExecutionTime += System.currentTimeMillis() - monitorStartTime;
						}
						
					}
				}
				
			} while (done);
			
			if ( INFO_ENABLED ) HbaseLog.l.info(
				"**** Time spent on Overall : Scanner : Plugin Code = " + 
					( System.currentTimeMillis() - overallStartTime) + ":" + 
					filter.overallExecutionTime + ":" + 
					filter.pluginExecutionTime  + " in ms.");
			
			byte[] data = SortedBytesArray.getInstance().toBytes(finalOutput);
			
        	return data;
			
		} finally {
			if ( null != filter) filter.close();
			
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
