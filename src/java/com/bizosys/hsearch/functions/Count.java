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
