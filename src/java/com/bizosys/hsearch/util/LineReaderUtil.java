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

public final class LineReaderUtil {
	
	public static final void fastSplit(final Collection<String> result,
			final String text, final char separator) {

		if (text == null)
			return;
		if (text.length() == 0)
			return;

		int index1 = 0;
		int index2 = text.indexOf(separator);

		if (index2 >= 0) {
			String token = null;
			while (index2 >= 0) {
				token = text.substring(index1, index2);
				result.add(token);
				index1 = index2 + 1;
				index2 = text.indexOf(separator, index1);
				if (index2 < 0) index1--;
			}

			if (index1 <= text.length() - 1) {
				result.add(text.substring(index1 + 1));
			}

		} else {
			result.add(text);
		}
	}	
	
	public static final void fastSplit(final String[] result,
			final int[] positions, final String text, final char separator) {

		if (text == null)
			return;
		if (text.length() == 0)
			return;

		int index1 = 0;
		int index2 = text.indexOf(separator);

		int pos = -1;
		int resultSeq = 0;
		if (index2 >= 0) {
			String token = null;
			while (index2 >= 0) {
				pos++;
				for (int aPos : positions) {
					if (pos != aPos) continue;
					token = text.substring(index1, index2);
					result[resultSeq++] = token;
					break;
				}
				index1 = index2 + 1;
				index2 = text.indexOf(separator, index1);
				if (index2 < 0) index1--;
			}

			if (index1 <= text.length() - 1) {
				pos++;
				for (int aPos : positions) {
					if (pos != aPos) continue;
					result[resultSeq++] = text.substring(index1 + 1);
					break;
				}
			}

		} else {
			pos++;
			for (int aPos : positions) {
				if (pos != aPos) continue;
				result[resultSeq++] = text;
				break;
			}
		}
	}

	public static final void fastSplit(final String[] result,
			final String text, final char separator) {

		if (text == null)
			return;
		if (text.length() == 0)
			return;

		int index1 = 0;
		int index2 = text.indexOf(separator);

		int resultSeq = 0;
		if (index2 >= 0) {
			String token = null;
			while (index2 >= 0) {
				token = text.substring(index1, index2);
				result[resultSeq++] = token;
				index1 = index2 + 1;
				index2 = text.indexOf(separator, index1);
				if (index2 < 0)
					index1--;
			}

			if (index1 <= text.length() - 1) {
				if ( result.length > resultSeq)  
					result[resultSeq++] = text.substring(index1 + 1);
			}

		} else {
			result[resultSeq++] = text;
		}
	}

	private StringBuilder appender = new StringBuilder(65536);

	public final String append(final String[] cols, final char separator) {
		boolean isFirst = true;
		for (String col : cols) {
			if (isFirst)
				isFirst = false;
			else
				appender.append(separator);

			appender.append(col);
		}
		String value = appender.toString();
		appender.setLength(0);
		return value;

	}

}
