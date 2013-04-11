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
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import com.bizosys.hsearch.PerformanceLogger;
import com.bizosys.hsearch.hbase.HbaseLog;

public final class HSearchBytesCoprocessor extends BaseEndpointCoprocessor {
	
	public static boolean DEBUG_ENABLED = HbaseLog.l.isDebugEnabled();
	public static boolean INFO_ENABLED = PerformanceLogger.l.isInfoEnabled(); 
	
    /**
     * Get Matching rows 
     * @param filter
     * @return
     * @throws IOException
     */
	public byte[] getRows(final byte[][] families, final byte[][] cols, final HSearchBytesFilter filter) throws IOException {
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
					" @ adding family " + new String(families[i]) + "_" + new String(cols[i]));
				scan = scan.addColumn(families[i], cols[i]);
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
			do {
				done = scanner.next(curVals);
			} while (done);
			
			byte[] data = filter.processRows();
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
