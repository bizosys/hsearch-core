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
package com.bizosys.hsearch.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A Record 
 * @author bizosys
 *
 */
public class Record {
	
	public static final List<NV> KEYVALUE_EMPTY = new ArrayList<NV>(0);
	
	public byte[] pk = null;
	protected List<NV> nvs = KEYVALUE_EMPTY;

	public Record(byte[] pk) {
		this.pk = pk;
	}
	
	public Record(byte[] pk, List<NV> kvs ) {
		this.pk = pk;
		if ( null != nvs) this.nvs = kvs;
	}
	
	/**
	 * @param fam	Column Family
	 * @param name	Column Name
	 * @param data	Existing Column Data
	 * @return Should the data be merged or not
	 */
	public void merge(byte[] fam, byte[] name, byte[] data) throws IOException {
		return;
	}
	
	public List<NV> getBlankNVs() throws IOException {
		return this.nvs;
	}
	
	public List<NV> getNVs() throws IOException {
		return this.nvs;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("  PK :").append(this.pk);
		if ( null == nvs) return sb.toString();
		for (NV nv : nvs) {
			sb.append(nv.toString()).append('\n');
		}
		return sb.toString();
	}

}
