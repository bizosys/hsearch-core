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

package com.bizosys.hsearch.treetable.cache;

import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.HbaseLog;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.hbase.RecordScalar;
import com.bizosys.hsearch.treetable.storage.CacheStorage;
import com.bizosys.hsearch.util.BatchException;
import com.bizosys.hsearch.util.BatchTask;

public class CacheStoreClientAsync implements BatchTask {

	private static boolean DEBUG_ENABLED = HbaseLog.l.isDebugEnabled(); 
	
	String jobName = "CacheAsync";
	
	String scopedSingleQuery = null;
	byte[] output = null;
	
	public CacheStoreClientAsync(final String scopedSingleQuery, final byte[] output) {
		this.scopedSingleQuery = scopedSingleQuery;
		this.output = output;
	}
	
	@Override
	public String getJobName() {
		return jobName;
	}

	@Override
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	@Override
	public boolean process() throws BatchException {
		if ( DEBUG_ENABLED ) {
			HbaseLog.l.debug("Caching query:" + scopedSingleQuery);
		}
		try {
			RecordScalar record = new RecordScalar(scopedSingleQuery.getBytes(), 
				new NV(CacheStorage.CACHE_COLUMN_BYTES, CacheStorage.CACHE_COLUMN_BYTES, output));
			HWriter.getInstance(true).insertScalar(CacheStorage.TABLE_NAME, record);
			return true;
		} catch (Exception ex) {
			HbaseLog.l.warn("Error while saving cache objects:" , ex);
			throw new BatchException(ex);
			
		}
		
	}

}
