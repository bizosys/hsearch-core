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
import java.util.Map;

import com.bizosys.hsearch.treetable.client.partition.IPartition;

public class HBaseTableSchemaDefn {
	
	private static HBaseTableSchemaDefn singleton = new HBaseTableSchemaDefn();
	
	public static HBaseTableSchemaDefn getInstance() {
		return singleton;
	}
	
	private HBaseTableSchemaDefn() {
		
	}

	public String tableName = "htable";
	
	//FamilyName_partition is how the column families are created.
	public Map<String, IPartition> columnPartions = new HashMap<String, IPartition>();

	public static char getColumnName() {
		return getColumnName(1);
	}
	
	public static char getColumnName(int token) {
		String tokenStr  = new Integer(token).toString();
		return tokenStr.charAt(tokenStr.length() - 1);
	}	
	

}