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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.bizosys.hsearch.util.HSearchConfig;
import com.bizosys.hsearch.util.conf.Configuration;

public class HSearchTableResourcesDefault {

	private static HSearchTableResourcesDefault singleton = null;
	
	public static HSearchTableResourcesDefault getInstance() {
		if ( null != singleton ) return singleton;
		synchronized (HSearchTableResourcesDefault.class) {
			if ( null != singleton ) return singleton;
			singleton = new HSearchTableResourcesDefault();
		}
		return singleton;
	}

	public int multiPartsThreadLimit = Runtime.getRuntime().availableProcessors();
	public int multiQueryThreadsLimit = -1;

	/**
	 * Processing threads.. No I/O wait. Please never make it more than the # of CPUs 
	 */
	public ExecutorService multiPartsThreadExecutor = null;
	
	private HSearchTableResourcesDefault() {
		Configuration config = HSearchConfig.getInstance().getConfiguration(); 
		
		this.multiPartsThreadLimit = config.getInt("query.parts.threads.limit", -1);
		if ( -1 == this.multiPartsThreadLimit) 
			multiPartsThreadLimit = Runtime.getRuntime().availableProcessors();
		this.multiPartsThreadExecutor = Executors.newFixedThreadPool(multiPartsThreadLimit);
		
		this.multiQueryThreadsLimit = config.getInt("query.multi.threads.limit", -1);
		if ( -1 == this.multiQueryThreadsLimit) 
			multiQueryThreadsLimit = Runtime.getRuntime().availableProcessors();
	}
	
}
