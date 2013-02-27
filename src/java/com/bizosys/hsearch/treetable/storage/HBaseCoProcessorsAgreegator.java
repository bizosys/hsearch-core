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
import java.util.Vector;

import com.bizosys.hsearch.hbase.ColumnFamName;
import com.bizosys.hsearch.hbase.IScanCallBack;

public class HBaseCoProcessorsAgreegator implements IScanCallBack {
	
	public class HSearchTablerRow {
		
		public byte[] pk;
		public byte[] row;
		ColumnFamName fn;
		
		public HSearchTablerRow(byte[] pk, ColumnFamName fn, byte[] row) {
			this.pk = pk;
			this.row = row;
			this.fn = fn;
		}
	}
	
	public List<HSearchTablerRow> records = null; 

	public HBaseCoProcessorsAgreegator() {
		this.records = new Vector<HSearchTablerRow>();
	}
	
	public HBaseCoProcessorsAgreegator(List<HSearchTablerRow> records) {
		if (null == records) this.records = new Vector<HSearchTablerRow>();
		this.records = records;
	}
	
	@Override
	public void process(byte[] pk, ColumnFamName fn,  byte[] storedBytes) throws IOException {
		int length = ( null == storedBytes ) ? 0 : storedBytes.length;
		if ( length == 0 ) return;
		records.add(new HSearchTablerRow(pk, fn, storedBytes));
	}
	
}
