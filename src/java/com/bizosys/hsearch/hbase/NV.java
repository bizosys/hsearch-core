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

/**
 * Serialized name value objects
 * @author karan
 *
 */
public class NV {

	/**
	 * Column Family
	 */
	public byte[] family = null;
	
	/**
	 * Column name
	 */
	public byte[] name = null;
	
	/**
	 * Column data
	 */
	public byte[] data = null;
	
	public boolean isDataUnchanged = false;

	/**
	 * Constructor
	 * @param family	Column Family
	 * @param name	Column Name / Qualifier
	 */
	public NV(String family, String name) throws IOException {
		if ( null == family || null == name) throw new IOException("Family or Name is null");
		if ( family.length() == 0 || name.length() == 0 ) throw new IOException("Family or Name is Empty");
		
		this.family = family.getBytes("UTF-8");
		this.name = name.getBytes("UTF-8");
	}
	
	/**
	 * Constructor
	 * @param family	Column Family
	 * @param name	Column Name / Qualifier
	 */
	public NV(byte[] family, byte[] name) throws IOException {
		if ( null == family || null == name) throw new IOException("Family or Name is null");
		
		this.family = family;
		this.name = name;
	}
	
	/**
	 * Constructor
	 * @param family	Column Family
	 * @param name	Column Name / Qualifier
	 * @param data	Column Value
	 */
	public NV(byte[] family, byte[] name, byte[] data) throws IOException {
		if ( null == family || null == name) throw new IOException("Family or Name is null");
		
		this.family = family;
		this.name = name;
		this.data = data;
	}
	
	public NV(byte[] family, byte[] name, byte[] data, boolean isDataUnchanged)  throws IOException{
		this(family,name,data);
		this.isDataUnchanged = isDataUnchanged;
	}
	
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(50);
		sb.append("  F:[").append(new String(family)).
		append("] N:[").append(new String(name)).
		append("] D:").append(data.toString());
		return sb.toString();
	}
}
