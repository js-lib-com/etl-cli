package com.jslib.etl.impl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Patterns {
	private static final Pattern SQL_PATTERN = Pattern.compile("^SELECT\\s+.+FROM\\s+.+$");
	
	public boolean isSql(String expression) {
		Matcher matcher = SQL_PATTERN.matcher(expression);
		return matcher.find();
	}
}
