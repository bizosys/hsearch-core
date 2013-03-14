package com.bizosys.hsearch.functions;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

public class FunctionRepos {
	
	public final static int COUNT = 0;
	
	public final static Map<Integer, IFunction> repos = new HashMap<Integer, IFunction>();

	public static IFunction getFunction(int funcCode) throws ParseException{
		switch (funcCode) {
			case COUNT:
				return new Count();
		}
		
		if (repos.containsKey(funcCode)) {
			return repos.get(funcCode); 
		}
		
		throw new ParseException("Unknown function code", funcCode);
	}
	
}
