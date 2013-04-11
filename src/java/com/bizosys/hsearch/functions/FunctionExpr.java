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

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.util.LineReaderUtil;

public class FunctionExpr {
	public int cellNo;
	public int functionCode;
	
	public FunctionExpr(int cellNo, int functionCode) {
		this.cellNo = cellNo;
		this.functionCode = functionCode;
	}
	
	public static FunctionExpr[] deser(String text) throws ParseException {
		List<String> cellNoAndFunc = new ArrayList<String>();
		LineReaderUtil.fastSplit(cellNoAndFunc, text, ',');
		FunctionExpr[] funcs = new FunctionExpr[cellNoAndFunc.size()];
		
		int seq = 0;
		for (String cellNoAndFuncStr : cellNoAndFunc) {
			int fDivider = cellNoAndFuncStr.indexOf(':');
			if ( fDivider < 0 ) {
				throw new ParseException("Improper function serilization: " + cellNoAndFuncStr, 0);
			} 
			
			funcs[seq] = new FunctionExpr(
				new Integer(cellNoAndFuncStr.substring(0, fDivider)),
				new Integer(cellNoAndFuncStr.substring(fDivider + 1))
				);
			seq++;
		}
		return funcs;
	}
	
	public static String ser(FunctionExpr[] functions) {
		StringBuilder sb = null;
		
		for (FunctionExpr functionExpr : functions) {
			if ( null == sb) {
				sb = new StringBuilder();
			} else {
				sb.append(',');
			}
			sb.append(functionExpr.cellNo);
			sb.append(':');
			sb.append(functionExpr.functionCode);
		}
		return sb.toString();
	}
}
