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

package com.bizosys.hsearch.treetable;

import java.util.Collection;
import java.util.Iterator;

import com.bizosys.hsearch.byteutils.ISortedByte;

public class Cell2FoundIndex<K1, V> implements Collection<Integer>{
	
	private ISortedByte<K1> k1Sorter = null;
	private ISortedByte<V> vSorter = null;
	private Cell2Visitor<K1,V> visitor = null;

	public Cell2FoundIndex(ISortedByte<K1> k1Sorter, ISortedByte<V> vSorter, Cell2Visitor<K1,V> visitor) {
		this.k1Sorter = k1Sorter;
		this.vSorter = vSorter;
		this.visitor = visitor;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public boolean contains(Object o) {
		return false;
	}

	@Override
	public Iterator<Integer> iterator() {
		return null;
	}

	@Override
	public Object[] toArray() {
		return null;
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return null;
	}

	@Override
	public final boolean add(Integer i)  {
		visitor.visit(k1Sorter.getValueAt(i), vSorter.getValueAt(i));
		return true;
	}

	@Override
	public boolean remove(Object o) {
		return false;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return false;
	}

	@Override
	public boolean addAll(Collection<? extends Integer> c) {
		return false;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		return false;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		return false;
	}

	@Override
	public void clear() {
	}
}
