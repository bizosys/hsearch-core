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
package com.bizosys.hsearch.treetable.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.HSearchTableMultiQueryExecutor;
import com.bizosys.hsearch.treetable.client.HSearchTableParts;
import com.bizosys.hsearch.treetable.client.HSearchTableResourcesDefault;
import com.bizosys.hsearch.treetable.client.IHSearchTableCombiner;
import com.bizosys.hsearch.treetable.client.L;

public abstract class HSearchTableCombiner implements IHSearchTableCombiner {

	static boolean DEBUG_ENABLED = true;
	
	public void concurrentDeser(String aStmtOrValue, Map<String, Object> stmtParams) throws Exception {
		
		if ( DEBUG_ENABLED ) L.getInstance().logDebug( "> concurrentDeser Enter");
		
		HSearchTableParts tableParts = (HSearchTableParts) stmtParams.get(HSearchTableMultiQueryExecutor.TABLE_PARTS);
		IHSearchPlugin plugin = (IHSearchPlugin) stmtParams.get(HSearchTableMultiQueryExecutor.PLUGIN);
		Object outputTypeO = stmtParams.get(HSearchTableMultiQueryExecutor.OUTPUT_TYPE);
		Integer outputType = ( null != outputTypeO) ? (Integer) outputTypeO : HSearchTableMultiQueryExecutor.OUTPUT_COLS;
		
		HSearchQuery hQuery = new HSearchQuery(aStmtOrValue);		
		
		List<TableDeserExecutor> tasks = new ArrayList<TableDeserExecutor>();
		for ( byte[] tableSer : tableParts.allParts) {
			IHSearchTable t = buildTable();
			TableDeserExecutor deserTask = new TableDeserExecutor(t, tableSer, plugin, hQuery, outputType);
			tasks.add(deserTask);
		}
		if ( tasks.size() > 1 ) {
			HSearchTableResourcesDefault.getInstance().cpuIntensiveJobExecutor.invokeAll(tasks);
		} else {
			int i = 0;
			for ( TableDeserExecutor deserExec : tasks) {
				if ( DEBUG_ENABLED ) L.getInstance().logDebug( (i++) + "concurrentDeser Exit");
				deserExec.call(); 
			}
		}
	}
	
	public static class TableDeserExecutor implements Callable<Integer> {

		byte[] tableSer = null; 
		IHSearchPlugin plugin = null;
		HSearchQuery hQuery = null;
		int outputType = HSearchTableMultiQueryExecutor.OUTPUT_COLS;
		IHSearchTable t = null;

		public TableDeserExecutor(IHSearchTable t, byte[] tableSer, IHSearchPlugin plugin, HSearchQuery hQuery, int outputType) {
			this.t = t;
			this.tableSer = tableSer;
			this.plugin = plugin;
			this.hQuery = hQuery;
			this.outputType = outputType;
		}
		
		@Override
		public Integer call() throws Exception {
			try {
				switch ( this.outputType) {
					case HSearchTableMultiQueryExecutor.OUTPUT_COLS:
						t.get(tableSer, hQuery, plugin);
						break;
					case HSearchTableMultiQueryExecutor.OUTPUT_ID:
						t.keySet(tableSer, hQuery, plugin);
						break;
					case HSearchTableMultiQueryExecutor.OUTPUT_VAL:
						t.values(tableSer, hQuery, plugin);
						break;
					case HSearchTableMultiQueryExecutor.OUTPUT_IDVAL:
						t.keyValues(tableSer, hQuery, plugin);
						break;
				}
				
				return 0;
			} catch (Exception ex) {
				throw new Exception(ex);
			}
		}
	}
	
	public abstract IHSearchTable buildTable();
}
