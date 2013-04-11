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

public class Count implements IFunction{


	long count = 0;
	
	
	@Override
	public void eval(boolean input) {
	}

	@Override
	public void eval(short input) {
		count = count + input;
	}

	@Override
	public void eval(int input) {
		count = count + input;
	}

	@Override
	public void eval(float input) {
	}

	@Override
	public void eval(long input) {
		count = count + input;
	}

	@Override
	public void eval(double input) {
	}

	@Override
	public void eval(String input) {
	}

	@Override
	public void eval(byte[] input) {
	}

	@Override
	public Object getValue() {
		return this.count;
	}

}
