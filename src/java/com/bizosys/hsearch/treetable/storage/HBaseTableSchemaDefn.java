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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBaseTableSchemaDefn {
	
	private static HBaseTableSchemaDefn singleton = new HBaseTableSchemaDefn();
	
	public static HBaseTableSchemaDefn getInstance() {
		return singleton;
	}
	
	private HBaseTableSchemaDefn() {
		
	}

	public String tableName = "htable";
	
	//FamilyName_partition is how the column families are created.
	public Map<String, List<String>> familyNames = new HashMap<String, List<String>>();
	public Map<String, List<Double>> distributionPoint = new HashMap<String, List<Double>>();
	
	public static final String COL_NAME = "1";
	public static final byte[] COL_NAME_BYTES = COL_NAME.getBytes();
	public static final byte COL_NAME_BYTE = COL_NAME_BYTES[0];

}
