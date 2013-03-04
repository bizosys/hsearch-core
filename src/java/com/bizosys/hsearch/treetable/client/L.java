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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class L {
	
	private static L singleton = null;
	
	public static L getInstance() {
		if ( null != singleton ) return singleton;
		synchronized (L.class) {
			if ( null != singleton ) return singleton;
			singleton = new L();
		}
		return singleton;
	}
	
	Map<String, StringBuilder> loggings = new ConcurrentHashMap<String, StringBuilder>();
	
	public void logDebug(String msg) {
		String threadName = Thread.currentThread().getName();
		if (loggings.containsKey(threadName)) {
			loggings.get(threadName).append(msg).append('\n');
		} else {
			StringBuilder sb = new StringBuilder(65536);
			sb.append(msg).append('\n');
			loggings.put(threadName, sb);
		}
	}
	
	public void logWarning(String msg) {
		logError(msg, "Warning", null);
	}

	public void logWarning(String msg, Exception ex) {
		logError(msg, "Warning", ex);
	}

	public void logError(String msg) {
		logError(msg, "Error", null);
	}
	
	public void logError(String msg, Exception ex) {
		logError(msg, "Error", ex);
	}

	private void logError(String msg, String level, Exception ex) {
		String threadName = Thread.currentThread().getName();
		
		System.err.println(threadName + " > " + level + " > " +  msg );
		if ( null != ex) ex.printStackTrace(System.err);
		
		if (loggings.containsKey(threadName)) {
			loggings.get(threadName).append(msg).append('\n');
		} else {
			StringBuilder sb = new StringBuilder(65536);
			sb.append(msg).append('\n');
			loggings.put(threadName, sb);
		}
	}

	public void flush() {
		for (String threadName : loggings.keySet()) {
			StringBuilder sb = loggings.get(threadName);
			System.out.println("****** Thread :" + threadName + "\n" + loggings.get(threadName).toString());
			sb.delete(0, sb.capacity());
		}
		loggings.clear();
	}

	public void clear() {
		loggings.clear();
	}

	public void flush(Exception ex) {
		for (String threadName : loggings.keySet()) {
			StringBuilder sb = loggings.get(threadName);
			System.out.println("****** Thread :" + threadName + "\n" + loggings.get(threadName).toString());
			sb.delete(0, sb.capacity());
		}
		ex.printStackTrace(System.err);
	}

}
