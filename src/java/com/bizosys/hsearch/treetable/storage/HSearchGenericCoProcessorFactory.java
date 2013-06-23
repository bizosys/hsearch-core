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

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.hbase.ColumnFamName;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.treetable.cache.CacheService;
import com.bizosys.hsearch.util.HSearchLog;

public final class HSearchGenericCoProcessorFactory {
	
	public static boolean INFO_ENABLED = HSearchLog.l.isInfoEnabled();
	
	HSearchGenericFilter filter = null;
	byte[][] families = null;
	byte[][] cols = null;
	
	public HSearchGenericCoProcessorFactory(final List<ColumnFamName> family_cols , final HSearchGenericFilter filter) throws IOException {
		this.filter = filter;
		
		if (null == family_cols) throw new IOException("Please provide family details. Scan on all cols are not allowed");
		this.families = new byte[family_cols.size()][];
		this.cols = new byte[family_cols.size()][];
		
		int seq = -1;
		for (ColumnFamName columnFamName : family_cols) {
			seq++;
			this.families[seq] = columnFamName.family;
			this.cols[seq] = columnFamName.name;
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
				byte[] singleQueryResultB = CacheService.getInstance().get(singleQuery);
				if( null != singleQueryResultB) {
					return SortedBytesArray.getInstance().parse(singleQueryResultB).values();
				}
			}
		}
		
		Map<byte[], byte[]> output = table.table.coprocessorExec(
                HSearchGenericCoprocessor.class, null, null,
                
                
                new Batch.Call<HSearchGenericCoprocessor, byte[]>() {
                    @Override
                    public final byte[] call(HSearchGenericCoprocessor counter) throws IOException {
                        return counter.getRows(families, cols, filter);
                 }
         } );
		
		Collection<byte[]> result = output.values();
		
		try {
			if ( null != singleQuery) {
				byte[] dataPack = SortedBytesArray.getInstance().toBytes(result);
				CacheService.getInstance().put(singleQuery, dataPack);
			}
		} catch (Exception ex) {
			HSearchLog.l.warn(ex);
		}

		return result;
	}
}
