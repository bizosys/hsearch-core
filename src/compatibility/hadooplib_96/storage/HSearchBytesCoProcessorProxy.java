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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import com.bizosys.hsearch.hbase.ColumnFamName;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.treetable.storage.protobuf.generated.HSearchCoprocessorProtos.BytesRowRequest;
import com.bizosys.hsearch.treetable.storage.protobuf.generated.HSearchCoprocessorProtos.Column;
import com.bizosys.hsearch.treetable.storage.protobuf.generated.HSearchCoprocessorProtos.HSearchBytesCoprocessorProxyService;
import com.bizosys.hsearch.treetable.storage.protobuf.generated.HSearchCoprocessorProtos.RowResponse;
import com.bizosys.hsearch.treetable.storage.protobuf.generated.HSearchFilterProtos.HSearchBytesFilterMessage;
import com.bizosys.hsearch.util.HSearchLog;
import com.google.protobuf.ByteString;

public final class HSearchBytesCoProcessorProxy {

	public static boolean INFO_ENABLED = HSearchLog.l.isInfoEnabled();

	HSearchBytesFilter filter = null;
	List<ColumnFamName> family_cols = null;

	public HSearchBytesCoProcessorProxy(final List<ColumnFamName> family_cols , final HSearchBytesFilter filter) throws IOException {
		this.filter = filter;
		if (null == family_cols) throw new IOException("Please provide family details. Scan on all cols are not allowed");
		this.family_cols = family_cols;
	}

	public final Map<byte[], byte[]> execCoprocessorRows(final HTableWrapper table) throws IOException, Throwable  {

		BytesRowRequest.Builder requestBuilder = BytesRowRequest.newBuilder();
		for (ColumnFamName familyNqualifier : this.family_cols) {
			Column.Builder columnBuilder = Column.newBuilder();
			columnBuilder.setFamily(ByteString.copyFrom(familyNqualifier.family));
			columnBuilder.setQualifier(ByteString.copyFrom(familyNqualifier.name));
			Column column = columnBuilder.build();
			requestBuilder.addFamilyWithQualifier(column);
		}
		if(null != filter){
			HSearchBytesFilterMessage filterMessage = HSearchBytesFilter.getBytesFilterMessage(filter);
			requestBuilder.setFilter(filterMessage);
		}

		final BytesRowRequest request = requestBuilder.build();

		Batch.Call<HSearchBytesCoprocessorProxyService, byte[]> callable = 
				new Batch.Call<HSearchBytesCoprocessorProxyService, byte[]>() {

					@Override
					public final byte[] call(HSearchBytesCoprocessorProxyService instance) throws IOException {
						ServerRpcController controller = new ServerRpcController();
						BlockingRpcCallback<RowResponse> rpcCallback = new BlockingRpcCallback<RowResponse>();
						instance.getRows(controller, request, rpcCallback);
						RowResponse response = rpcCallback.get();
						if (controller.failedOnException()) {
							throw controller.getFailedOn();
						}

						ByteString result = response.getResult(); 
						if (!result.isEmpty()) {
							return result.toByteArray();
						}

						return null;
					}
				};

				Map<byte[], byte[]> output = table.tableInterface.coprocessorService(
						HSearchBytesCoprocessorProxyService.class,null,null,callable);
				
				return output;
	}
}