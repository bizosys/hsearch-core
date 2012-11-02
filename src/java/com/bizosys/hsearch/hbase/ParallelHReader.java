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
package com.bizosys.hsearch.hbase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.client.ParallelHTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

public class ParallelHReader {
	
	public ExecutorService execService = null;
	
	public ParallelHReader(int fixedThreads) {
		this.execService = Executors.newFixedThreadPool(fixedThreads);
	}

	public void getAllValues(String tableName, List<ColumnFamName> columns, 
		Filter filter, IScanCallBack callback) throws IOException {
		
		ResultScanner scanner = null;
		List<byte[]> matched = null;
		try {
			ParallelHTable table = new ParallelHTable(tableName, execService);
			
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(500);
			scan.setMaxVersions(1);
			for (ColumnFamName aColFamilyName : columns) {
				scan = scan.addColumn(aColFamilyName.family, aColFamilyName.name);
			}
			
			if ( null != filter) scan = scan.setFilter(filter);
			
			scanner = table.getScanner(scan);
			
			long timeS = System.currentTimeMillis();
			
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				
				for (ColumnFamName aColFamilyName : columns) {
					byte[] storedBytes = r.getValue(aColFamilyName.family, aColFamilyName.name);
					if ( null == storedBytes) continue;
					callback.process(r.getRow(), aColFamilyName, storedBytes);
				}
			}
			
			if ( HbaseLog.l.isDebugEnabled()) {
				long timeE = System.currentTimeMillis();
				HbaseLog.l.debug("HReader.getAllValues (" + tableName + ") execution time = " + 
					(timeE - timeS) );
			}
			
		} catch ( IOException ex) {
			throw ex;
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != matched) matched.clear();
		}
	}	
}
