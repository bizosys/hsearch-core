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
package com.bizosys.hsearch.distribution;

import java.util.Collection;

public class Distribution {

	public static final double[] distributesDouble(Collection<Double> inputs, int parts) {
		return new DistributionPartionDouble().distributes(inputs, parts);
	}

	public static final short[] distributesShort(Collection<Short> inputs, int parts) {
		return new DistributionPartionShort().distributes(inputs, parts);
	}

	public static int[] distributesInteger(Collection<Integer> inputs, int parts) {
		return new DistributionPartionInt().distributes(inputs, parts);
	}

	public static long[] distributesLong(Collection<Long> inputs, int parts) {
		return new DistributionPartionLong().distributes(inputs, parts);
	}
	
	public static float[] distributesFloat(Collection<Float> inputs, int parts) {
		return new DistributionPartionFloat().distributes(inputs, parts);
	}
	
}
