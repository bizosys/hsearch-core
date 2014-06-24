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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import com.bizosys.hsearch.treetable.storage.protobuf.generated.HSearchCoprocessorProtos.BytesRowRequest;
import com.bizosys.hsearch.treetable.storage.protobuf.generated.HSearchCoprocessorProtos.Column;
import com.bizosys.hsearch.treetable.storage.protobuf.generated.HSearchCoprocessorProtos.HSearchBytesCoprocessorProxyService;
import com.bizosys.hsearch.treetable.storage.protobuf.generated.HSearchCoprocessorProtos.RowResponse;
import com.bizosys.hsearch.treetable.storage.protobuf.generated.HSearchFilterProtos.HSearchBytesFilterMessage;
import com.bizosys.hsearch.util.HSearchConfig;
import com.bizosys.hsearch.util.HSearchLog;
import com.bizosys.hsearch.util.conf.Configuration;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

public final class HSearchBytesCoprocessor extends HSearchBytesCoprocessorProxyService 
implements CoprocessorService, Coprocessor {
	
	public static boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();
	
	public boolean scannerBlockCaching = true;
	public int scannerBlockCachingLimit = 1;
	private RegionCoprocessorEnvironment env = null;
	
	public HSearchBytesCoprocessor() {
		Configuration config = HSearchConfig.getInstance().getConfiguration(); 
		this.scannerBlockCaching = config.getBoolean("scanner.block.caching", true);
		this.scannerBlockCachingLimit = config.getInt("scanner.block.caching.amount", 1);
	}
	
	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		if (env instanceof RegionCoprocessorEnvironment) {
			this.env = (RegionCoprocessorEnvironment)env;
		} else {
			throw new CoprocessorException("Must be loaded on a table region!");
		}
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
	}

	@Override
	public Service getService() {
		return this;
	}

	
    /**
     * Get Matching rows 
     * @param filter
     * @return
     */
	@Override
	public void getRows(RpcController controller, BytesRowRequest request, RpcCallback<RowResponse> callBack) {
		if ( DEBUG_ENABLED ) HSearchLog.l.debug( Thread.currentThread().getName() + " @ coprocessor : getRows");

		InternalScanner scanner = null;
		RowResponse response = null;
		HSearchBytesFilter filter = null;

		try {
			Scan scan = new Scan();
			scan.setCacheBlocks(scannerBlockCaching);
			scan.setCaching(scannerBlockCachingLimit);
			scan.setMaxVersions(1);

			List<Column> familyWithQualifierL = request.getFamilyWithQualifierList();
			for (Column column : familyWithQualifierL) {
				if ( DEBUG_ENABLED ) HSearchLog.l.debug( Thread.currentThread().getName() + 
						" @ adding family " + column.getFamily().toString() + "_" + column.getQualifier().toString());
				scan.addColumn(column.getFamily().toByteArray(), column.getQualifier().toByteArray());
			}
			
			HSearchBytesFilterMessage filterMessage = request.getFilter();
			filter = HSearchBytesFilter.getBytesFilter(filterMessage);
			
			if ( null != filter) {
				FilterList filterL = filter.getFilters();
				if ( null != filterL) scan = scan.setFilter(filterL);
				else scan = scan.setFilter(filter);
			}

			RegionCoprocessorEnvironment environment = env;
			scanner = environment.getRegion().getScanner(scan);
			
			List<Cell> curVals = new ArrayList<Cell>();
			boolean hasMoreRows = false;
			do {
				hasMoreRows = scanner.next(curVals);
			} while (hasMoreRows);
			
			byte[] data = filter.processRows();
			if (data != null) {
				RowResponse.Builder builder = RowResponse.newBuilder();
				builder.setResult(ByteString.copyFrom(data));
				response = builder.build();

				if(DEBUG_ENABLED)
					HSearchLog.l.debug("Row response length from " + env.getRegion().getRegionNameAsString() + ": " + data.length);
			}

			callBack.run(response);
			
		} catch (Exception e) {
			
			HSearchLog.l.fatal("Error fetching rows in coprocessor : " + e.getLocalizedMessage());  
			ResponseConverter.setControllerException(controller, new IOException(e));
			
		}finally {
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
