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
import java.util.Map;

import org.apache.hadoop.hbase.client.coprocessor.Batch;

import com.bizosys.hsearch.hbase.ColumnFamName;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.treetable.BytesSection;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.CellKeyValue;
import com.bizosys.hsearch.util.HSearchLog;

public final class HSearchMultiGetCoProcessorProxy {
	
	public static boolean INFO_ENABLED = HSearchLog.l.isInfoEnabled();
	
	HSearchScalarFilter filter = null;
	byte[][] families = null;
	byte[][] cols = null;
	byte[][] rows = null;
	
	public HSearchMultiGetCoProcessorProxy(final ColumnFamName columnFamName , 
		final HSearchScalarFilter filter, byte[][] rows) throws IOException {
		
		this.filter = filter;
		if (null == columnFamName) throw new IOException("Please provide family details. Scan on all cols are not allowed");
		this.families = new byte[][]{columnFamName.family};
		this.cols = new byte[][]{columnFamName.name};
		this.rows = rows;
	}
	
	public final void execCoprocessorRows( Map<String, byte[]> kvs,
		final HTableWrapper table, final byte[] row) throws IOException, Throwable  {

		Map<byte[], byte[]> output = table.table.coprocessorExec(
                HSearchMultiGetCoprocessorI.class, row, row, 
                
                new Batch.Call<HSearchMultiGetCoprocessorI, byte[]>() {
                    @Override
                    public final byte[] call(HSearchMultiGetCoprocessorI counter) throws IOException {
                        return counter.getRows(families, cols, filter, rows);
                 }
         } );
		

		for (byte[] bs : output.keySet()) {
			Cell2<byte[], byte[]> cell2 = new Cell2<byte[], byte[]>(byte[].class, byte[].class);
			cell2.data = new BytesSection(output.get(bs) );
			cell2.parseElements();
			for (CellKeyValue<byte[], byte[]> kv: cell2.sortedList) {
				kvs.put(new String(kv.getKey()), kv.getValue());
			}
		}		
	}
}
