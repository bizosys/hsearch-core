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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.bizosys.hsearch.hbase.HbaseLog;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.HSearchTableMultiQueryExecutor;
import com.bizosys.hsearch.treetable.client.HSearchTableParts;
import com.bizosys.hsearch.treetable.client.HSearchTableResourcesDefault;
import com.bizosys.hsearch.treetable.client.IHSearchTableCombiner;
import com.bizosys.hsearch.treetable.client.L;

public abstract class HSearchTableCombiner implements IHSearchTableCombiner {

	public static boolean DEBUG_ENABLED = HbaseLog.l.isDebugEnabled();
	
	@Override
	public void concurrentDeser(String aStmtOrValue, OutputType outputType, Map<String, Object> stmtParams, String tableType) throws Exception {
		
		if ( DEBUG_ENABLED ){
			String keys = ( null != stmtParams) ? stmtParams.keySet().toString() : "No Keys";
			HbaseLog.l.debug( Thread.currentThread().getName() +  "> concurrentDeser Enter - stmt params keys : " + keys);
		}
		
		Object tablePartsO = stmtParams.get(HSearchTableMultiQueryExecutor.TABLE_PARTS);
		
		if ( null == tablePartsO) {
			System.err.println("Warning : Null Column data for > " + tableType + " For Query " + aStmtOrValue);
			for (String key : stmtParams.keySet()) {
				System.err.println("Info : For Key > " + key + "  , Value " + stmtParams.get(key));
			}
		}
		HSearchTableParts tableParts = (HSearchTableParts) tablePartsO ;
		
		IHSearchPlugin plugin = (IHSearchPlugin) stmtParams.get(HSearchTableMultiQueryExecutor.PLUGIN);
		if ( null == plugin) {
			System.err.println("Warning : Null plugin for > " + tableType + ":" + aStmtOrValue);
			for (String key : stmtParams.keySet()) {
				System.err.println("Info : For Key > " + key + "  , Value " + stmtParams.get(key));
			}
			return;
		}
		plugin.cleanupValuesFromLastRun();

		HSearchQuery hQuery = new HSearchQuery(aStmtOrValue);		
		
		List<TableDeserExecutor> tasks = new ArrayList<TableDeserExecutor>();
		
		for ( byte[] tableSer : tableParts.allParts) {
			IHSearchTable t = buildTable(tableType);
			TableDeserExecutor deserTask = new TableDeserExecutor(t, tableSer, plugin, hQuery, outputType);
			tasks.add(deserTask);
		}
		if ( tasks.size() > 1 ) {
			if ( DEBUG_ENABLED ) HbaseLog.l.debug(Thread.currentThread().getName() + " > " + tasks.size() + " HSearchTableCombiner Processing in parallel.");
			HSearchTableResourcesDefault.getInstance().cpuIntensiveJobExecutor.invokeAll(tasks);
		} else {
			if ( DEBUG_ENABLED ) HbaseLog.l.debug(Thread.currentThread().getName() + " > " + tasks.size() + " HSearchTableCombiner Processing in sequence.");
			for ( TableDeserExecutor deserExec : tasks) {
				deserExec.call(); 
			}
		}
	}
	
	public static class TableDeserExecutor implements Callable<Integer> {

		byte[] tableSer = null; 
		IHSearchPlugin plugin = null;
		HSearchQuery hQuery = null;
		OutputType outputType = new OutputType();
		IHSearchTable t = null;

		public TableDeserExecutor(IHSearchTable t, byte[] tableSer, IHSearchPlugin plugin, HSearchQuery hQuery, OutputType outputType) {
			this.t = t;
			this.tableSer = tableSer;
			this.plugin = plugin;
			this.hQuery = hQuery;
			this.outputType = outputType;
		}
		
		@Override
		public Integer call() throws Exception {
			System.out.println(Thread.currentThread().getName() + " HSearch Table Processing - ENTER");
			try {
				switch ( this.outputType.typeCode) {
					case OutputType.OUTPUT_COLS:
						t.get(tableSer, hQuery, plugin);
						break;
					case OutputType.OUTPUT_ID:
						t.keySet(tableSer, hQuery, plugin);
						break;
					case OutputType.OUTPUT_VAL:
						t.values(tableSer, hQuery, plugin);
						break;
					case OutputType.OUTPUT_IDVAL:
						t.keyValues(tableSer, hQuery, plugin);
						break;
					default:
						throw new IOException("Unknown output type:" + this.outputType);
				}
				
				System.out.println(Thread.currentThread().getName() + " HSearch Table Processing - SUCESS");
				return 0;
			} catch (Exception ex) {
				throw new Exception(ex);
			} finally {
				System.out.println(Thread.currentThread().getName() + " HSearch Table Processing - EXIT");
			}
		}
	}
	
	public abstract IHSearchTable buildTable(String tableType) throws IOException ;
}
