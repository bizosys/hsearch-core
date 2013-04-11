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

package com.bizosys.hsearch.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class EmptyMap<K, V> implements Map<K, V> {

	@Override
	public final int size() {
		return 0;
	}

	@Override
	public final boolean isEmpty() {
		return false;
	}

	@Override
	public final boolean containsKey(Object key) {
		return false;
	}

	@Override
	public final boolean containsValue(Object value) {
		return false;
	}

	@Override
	public final V get(Object key) {
		return null;
	}

	@Override
	public V put(K key, V value) {
		return null;
	}

	@Override
	public final V remove(Object key) {
		return null;
	}

	@Override
	public final void putAll(Map<? extends K, ? extends V> m) {
	}

	@Override
	public final void clear() {
	}

	@Override
	public final Set<K> keySet() {
		return null;
	}

	@Override
	public final Collection<V> values() {
		return null;
	}

	@Override
	public final Set<java.util.Map.Entry<K, V>> entrySet() {
		return null;
	}

}
