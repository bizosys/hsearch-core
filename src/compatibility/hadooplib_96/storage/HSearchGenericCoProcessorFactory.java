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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.hbase.ColumnFamName;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.treetable.cache.CacheService;
import com.bizosys.hsearch.treetable.storage.protobuf.generated.HSearchCoprocessorProtos.Column;
import com.bizosys.hsearch.treetable.storage.protobuf.generated.HSearchCoprocessorProtos.HSearchGenericCoprocessorService;
import com.bizosys.hsearch.treetable.storage.protobuf.generated.HSearchCoprocessorProtos.RowRequest;
import com.bizosys.hsearch.treetable.storage.protobuf.generated.HSearchCoprocessorProtos.RowResponse;
import com.bizosys.hsearch.treetable.storage.protobuf.generated.HSearchFilterProtos.HSearchGenericFilterMessage;
import com.bizosys.hsearch.util.HSearchLog;
import com.google.protobuf.ByteString;

public final class HSearchGenericCoProcessorFactory {
	
	public static boolean INFO_ENABLED = HSearchLog.l.isInfoEnabled();
	boolean cacheEnabled = false;
	
	HSearchGenericFilter filter = null;
	List<ColumnFamName> family_cols = null;
	
	public HSearchGenericCoProcessorFactory(final List<ColumnFamName> family_cols , final HSearchGenericFilter filter) throws IOException {

		if (null == family_cols) throw new IOException("Please provide family details. Scan on all cols are not allowed");
		this.family_cols = family_cols;
		this.filter = filter;
		this.cacheEnabled = CacheService.getInstance().isCacheEnable();
		if ( INFO_ENABLED) {
			HSearchLog.l.info("Cache Storage Enablement :" + cacheEnabled );
		}
	}
	
	public final Collection<byte[]> execCoprocessorRows(final HTableWrapper table) throws IOException, Throwable  {

		String singleQuery = null;
		
		/**
		 * Check for already cached result
		 */
		if ( null != filter) {
			if ( filter.clientSideAPI_IsSingleQuery() ) {
				singleQuery = filter.clientSideAPI_getSingleQueryWithScope();
				if ( cacheEnabled ) {
					byte[] singleQueryResultB = CacheService.getInstance().get(singleQuery);
					if( null != singleQueryResultB) {
						return SortedBytesArray.getInstance().parse(singleQueryResultB).values();
					}
				}
			}
		}
		
		RowRequest.Builder requestBuilder = RowRequest.newBuilder();
		
		for (ColumnFamName familyNqualifier : this.family_cols) {
			Column.Builder columnBuilder = Column.newBuilder();
			columnBuilder.setFamily(ByteString.copyFrom(familyNqualifier.family));
			columnBuilder.setQualifier(ByteString.copyFrom(familyNqualifier.name));
			Column column = columnBuilder.build();
			requestBuilder.addFamilyWithQualifier(column);
		}
		
		if(null != filter){
			HSearchGenericFilterMessage filterMessage = HSearchGenericFilter.getGenericFilterMessage(filter);
			requestBuilder.setFilter(filterMessage);
		}
			
	    final RowRequest request = requestBuilder.build();
		
	    Batch.Call<HSearchGenericCoprocessorService, byte[]> onComplete = new Batch.Call<HSearchGenericCoprocessorService, byte[]>(){

			@Override
			public byte[] call(HSearchGenericCoprocessorService instance)throws IOException {
	    
				ServerRpcController controller = new ServerRpcController();
	            BlockingRpcCallback<RowResponse> rpcCallback = new BlockingRpcCallback<RowResponse>();
				
	            instance.getRows(controller, request, rpcCallback);
				RowResponse response = rpcCallback.get();
	            
				if (controller.failedOnException()) {
	              throw controller.getFailedOn();
	            }
	            
	            ByteString result = response.hasResult() ? response.getResult() : null; 
	            
	            if (null != result) {
	              return result.toByteArray();
	            }
	            
	            return null;
			}
		};
         
		Map<byte[], byte[]> output = table.tableInterface.coprocessorService(
											HSearchGenericCoprocessorService.class,//coprocessor pf service class 
											null,//Start Row Key 
											null,//End Row key 
											onComplete);//CallBack
		
		Collection<byte[]> result = output.values();
		
		try {
			if ( null != singleQuery) {
				if ( cacheEnabled ) {
					byte[] dataPack = SortedBytesArray.getInstance().toBytes(result);
					CacheService.getInstance().put(singleQuery, dataPack);
				}
			}
		} catch (Exception ex) {
			HSearchLog.l.warn("Cache Service Failure.", ex);
		}

		return result;
	}
}
