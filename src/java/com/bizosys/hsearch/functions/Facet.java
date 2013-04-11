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
package com.bizosys.hsearch.functions;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.CellKeyValue;


public class Facet<T> {

	public static class Total {
		public int counter = 1;
	}
	private HashMap<String, Total> elements = new HashMap<String, Total>();
	
	public void add(T elem) {
		String elemStr = elem.toString();
		if ( elements.containsKey(elemStr)) {
			elements.get(elemStr).counter++;
		} else {
			elements.put(elemStr, new Total());
		}
	}
	
	public HashMap<String, Total> getFacets() {
		return this.elements;
	}
	
	public void clear() {
		this.clear();
	}
	
	public byte[] toBytes() throws IOException {
		
		HashMap<String, Integer> facets = new HashMap<String, Integer>();
		for (String key : this.elements.keySet()) {
			facets.put(key, this.elements.get(key).counter);
		}
		
		Cell2<String, Integer> cell2 = new Cell2<String, Integer>(
			SortedBytesString.getInstance(), SortedBytesInteger.getInstance());
		return cell2.toBytesOnSortedData(facets);
	}

	public static Map<String, Integer> fromBytes(byte[] data) throws IOException {
		
		Cell2<String, Integer> cell2 = new Cell2<String, Integer>(
			SortedBytesString.getInstance(), SortedBytesInteger.getInstance(), data);
		List<CellKeyValue<String, Integer> >  cellL = cell2.getMap(data);
		
		
		HashMap<String, Integer> facets = new HashMap<String, Integer>();
		for (CellKeyValue<String, Integer> cellKeyValue : cellL) {
			facets.put(cellKeyValue.key, cellKeyValue.value);
		}
		return facets;
	}
	
	public static void main(String[] args) throws Exception {
		Facet<String> ser = new Facet<String>();
		ser.add("abinash");
		ser.add("abinash");
		ser.add("abinash");

		ser.add("karan");
		ser.add("karan");

		ser.add("bizosys");
		
		byte[] data = ser.toBytes();
		
		Map<String, Integer> deser = Facet.fromBytes(data);
		System.out.println(deser);
		
		
		
		

	}

}
