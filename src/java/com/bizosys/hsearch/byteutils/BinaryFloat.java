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

package com.bizosys.hsearch.byteutils;

import java.util.ArrayList;
import java.util.List;

public class BinaryFloat {
	
	public static final List<Float> emptyList = new ArrayList<Float>(0);
	
	private boolean isEmpty = true;
	public List<Float> values = emptyList;
	
	public BinaryFloat() {
		
	}
	
	public void add(float aFloat) {
		if ( isEmpty ) {
			isEmpty = false;
			values = new ArrayList<Float>(0);
		}
		
		values.add(aFloat);
	}
	
	public byte[] toBytes() {
		int val = -1;
		int valuesT = values.size();
		byte[] b = new byte[4 * valuesT];
		int arrII = 0;

		for (int arrI=0 ; arrI< valuesT; arrI++) {
			float f = values.get(arrI);
			arrII = arrI * 4;
			
			val = Float.floatToRawIntBits(f);
			for(int i = 3; i > 0; i--) {
				b[arrII + i] = (byte)(val);
				val >>>= 8;
			}
			b[arrII] = (byte)(val);
		}
		
		return b;
	}


}
