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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.federate.FederatedFacade;

public abstract class HSearchTableMultiQueryProcessor implements IHSearchTableMultiQueryProcessor {

	boolean DEBUG_ENABLED = true;
	
	public final static List<com.bizosys.hsearch.federate.FederatedFacade<Long, Integer>.IRowId> noIdsFound = 
			new ArrayList<com.bizosys.hsearch.federate.FederatedFacade<Long, Integer>.IRowId>(0);
	
	public abstract IHSearchTableCombiner getCombiner();
	
	private FederatedFacade<Long, Integer> processor = null;
	
	public HSearchTableMultiQueryProcessor() {
		processor = build();
	}
	
	public FederatedFacade<Long, Integer> getProcessor() { 
		return processor;
	}
	
	private FederatedFacade<Long, Integer> build() {

		return new FederatedFacade<Long, Integer>(0, 
				HSearchTableResourcesDefault.getInstance().multiQueryIdObjectInitialCache, HSearchTableResourcesDefault.getInstance().multiQueryPartsThreads) {
			
			@Override
			public List<FederatedFacade<Long, Integer>.IRowId> populate(
					String type, String multiQueryPartId, String aStmtOrValue, Map<String, Object> stmtParams) throws IOException {

				long startTime = System.currentTimeMillis();
				long endTime = -1L;
				try {

					if ( type == "text") {
						
					} else {
						getCombiner().concurrentDeser(aStmtOrValue, stmtParams);
					}

					IHSearchPlugin plugin = (IHSearchPlugin) stmtParams.get(HSearchTableMultiQueryExecutor.PLUGIN);
					Collection<Integer> keys = plugin.getUniqueRowKeys();
					if ( keys.size() == 0) {
						if ( DEBUG_ENABLED ) L.getInstance().logDebug(  "> " + "No Records found :");
						return noIdsFound;
					}
					if ( DEBUG_ENABLED ) L.getInstance().logDebug(  "> " + "Total Ids found :" + keys.size());
	
					List<com.bizosys.hsearch.federate.FederatedFacade<Long, Integer>.IRowId> results = 
							new ArrayList<com.bizosys.hsearch.federate.FederatedFacade<Long, Integer>.IRowId>(keys.size());
					
					for (Integer id : keys) {
						IRowId primary = objectFactory.getPrimaryKeyRowId(id);
						results.add(primary);
					}
					return results;
				} catch (Exception ex) {
					throw new IOException(ex);
				} finally {
					if ( DEBUG_ENABLED ) {
						endTime = System.currentTimeMillis();
						L.getInstance().logDebug( Thread.currentThread().getName() + "> " + "Deserialization Time :" + (endTime - startTime));
					}
				}
			}
		};
	}
}
