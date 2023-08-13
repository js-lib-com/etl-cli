package com.jslib.etl.impl;

import java.util.Map;

import com.jslib.etl.IOutputRecord;

public class OutputRecord implements IOutputRecord {
	private Map<String, Object> values;

	@Override
	public void setValue(String[] columnNames, Object[] values) {
		assert columnNames.length == values.length;
		for (int i = 0; i < columnNames.length; ++i) {
			this.values.put(columnNames[i], values[i]);
		}
	}
}
