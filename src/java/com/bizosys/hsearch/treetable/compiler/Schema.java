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
package com.bizosys.hsearch.treetable.compiler;

import java.util.ArrayList;
import java.util.List;

public class Schema {
	public String table = "";
	public String module = "";
	public List<Column> columns = new ArrayList<Column>();
	
	Schema() {
	}
	
	public static class Column {
		public Column() {}
		public String name = "";
		public Field mergeId = null;
		public List<Field> indexes = new ArrayList<Schema.Field>();
		public Field key = null;
		public Field value = new Schema.Field();
		public Partitions partitions = new Partitions();
		
		
		public String toString() {
			String text = name + "\n" + mergeId.toString();
			return text;
		}
	}
	
	public static class Partitions {
		public int parts = 0;
		public String values = "";
		
		public Partitions() {}
		public Partitions(int parts, String values ) {
			this.parts = parts;
			this.values = values;
		}
	}
	
	public static class Field {
		public String name = "";
		public String datatype = "";
		public double minValue = -1;
		public List<String> allowedValues = new ArrayList<String>();

		public Field() {}
		
		public Field(String name, String datatype) {
			this.name = name;
			this.datatype = datatype;
		}
		
		public Field(String name, String datatype, double minValue) {
			this.name = name;
			this.datatype = datatype;
			this.minValue = minValue;
		}

		public Field(String name, String datatype, String[] allowedValues) {
			this.name = name;
			this.datatype = datatype;
			for (String value : allowedValues) {
				this.allowedValues.add(value);
			}
		}

		public String toString() {
			String text = name + "/" + datatype;
			return text;
		}
	}	
	
	public String toString() {
		String text = table;
		
		if ( null != columns) {
			for (Column col : columns) {
				if ( null == col) continue;
				text = text + "\n" + col.toString();
			}
		}
		return text;
		
	}
}
