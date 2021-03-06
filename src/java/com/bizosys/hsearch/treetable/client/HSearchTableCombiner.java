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

import com.bizosys.hsearch.util.HSearchLog;

public abstract class HSearchTableCombiner implements IHSearchTableCombiner {

	public static boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();
	public static boolean INFO_ENABLED = HSearchLog.l.isInfoEnabled();

	@Override
	public final void concurrentDeser(final String aStmtOrValue, final HSearchProcessingInstruction outputType,
		final Map<String, Object> stmtParams, final String tableType) throws Exception {
		
		if ( DEBUG_ENABLED ){
			String keys = ( null != stmtParams) ? stmtParams.keySet().toString() : "No Keys";
			HSearchLog.l.debug( Thread.currentThread().getName() +  "> concurrentDeser Enter - stmt params keys : " + keys);
		}
		
		Object tablePartsO = stmtParams.get(HSearchTableMultiQueryExecutor.TABLE_PARTS);
		
		if ( null == tablePartsO) {
			HSearchLog.l.warn("Warning : Null Column data for > " + tableType + " For Query " + aStmtOrValue);
			for (String key : stmtParams.keySet()) {
				HSearchLog.l.warn("Warning : For Key > " + key + "  , Value " + stmtParams.get(key));
			}
			return;
		}
		HSearchTableParts tableParts = (HSearchTableParts) tablePartsO ;
		if ( null == tableParts.allParts ) return; 
		
		IHSearchPlugin plugin = (IHSearchPlugin) stmtParams.get(HSearchTableMultiQueryExecutor.PLUGIN);
		if ( null == plugin) {
			HSearchLog.l.error("Warning : Null plugin for > " + tableType + ":" + aStmtOrValue);
			for (String key : stmtParams.keySet()) {
				HSearchLog.l.error("Info : For Key > " + key + "  , Value " + stmtParams.get(key));
			}
			return;
		}
		plugin.clear();

		HSearchQuery hQuery = new HSearchQuery(aStmtOrValue);		
		
		List<TableDeserExecutor> tasks = new ArrayList<TableDeserExecutor>();
		
		for ( byte[] tableSer : tableParts.allParts) {
			if ( null == tableSer) continue;
			IHSearchTable t = buildTable(tableType);
			TableDeserExecutor deserTask = new TableDeserExecutor(t, tableSer, plugin, hQuery, outputType);
			tasks.add(deserTask);
		}
		
		if ( tasks.size() > 1 ) {
			if ( DEBUG_ENABLED ) HSearchLog.l.debug(Thread.currentThread().getName() + " > " + tasks.size() + " HSearchTableCombiner Processing in parallel.");
			HSearchTableResourcesDefault.getInstance().multiPartsThreadExecutor.invokeAll(tasks);
		} else {
			if ( DEBUG_ENABLED ) HSearchLog.l.debug(Thread.currentThread().getName() + " > " + tasks.size() + " HSearchTableCombiner Processing in sequence.");
			for ( TableDeserExecutor deserExec : tasks) {
				deserExec.call(); 
			}
		}
		
	}
	
	public static final class TableDeserExecutor implements Callable<Integer> {

		byte[] tableSer = null; 
		IHSearchPlugin plugin = null;
		HSearchQuery hQuery = null;
		HSearchProcessingInstruction outputType = null;
		IHSearchTable t = null;

		public TableDeserExecutor(final IHSearchTable t, final byte[] tableSer, final IHSearchPlugin plugin, 
				final HSearchQuery hQuery, final HSearchProcessingInstruction outputType) {
			this.t = t;
			this.tableSer = tableSer;
			this.plugin = plugin;
			this.hQuery = hQuery;
			this.outputType = outputType;
		}
		
		@Override
		public final Integer call() throws Exception {
			if ( DEBUG_ENABLED ) {
				HSearchLog.l.debug(Thread.currentThread().getName() + " HSearch Table Processing - ENTER");
			}
			try {
				switch ( this.outputType.getCallbackType()) {
					case HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS:
						t.get(tableSer, hQuery, plugin);
						break;
					case HSearchProcessingInstruction.PLUGIN_CALLBACK_ID:
						t.keySet(tableSer, hQuery, plugin);
						break;
					case HSearchProcessingInstruction.PLUGIN_CALLBACK_VAL:
						t.values(tableSer, hQuery, plugin);
						break;
					case HSearchProcessingInstruction.PLUGIN_CALLBACK_IDVAL:
						t.keyValues(tableSer, hQuery, plugin);
						break;
					default:
						throw new IOException("Unknown output type:" + this.outputType);
				}
				
				if ( DEBUG_ENABLED ) {
					HSearchLog.l.debug(Thread.currentThread().getName() + " HSearch Table Processing - SUCESS");
				}
				return 0;
			} catch (Exception ex) {
				HSearchLog.l.error(Thread.currentThread().getName(), ex);
				throw new Exception(ex);
			} finally {
				if ( DEBUG_ENABLED ) {
					HSearchLog.l.debug(Thread.currentThread().getName() + " HSearch Table Processing - EXIT");
				}
			}
		}
	}
	
	public abstract IHSearchTable buildTable(String tableType) throws IOException ;
}
