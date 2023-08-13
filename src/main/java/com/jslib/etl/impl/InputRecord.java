package com.jslib.etl.impl;

import java.util.Map;

import com.jslib.etl.IInputRecord;

public class InputRecord implements IInputRecord {
	private final Map<String, Object> values;

	public InputRecord(Map<String, Object> values) {
		this.values = values;
	}

	@Override
	public Object[] getValues(String[] columnNames) {
		Object[] values = new Object[columnNames.length];
		for (int i = 0; i < columnNames.length; ++i) {
			values[i] = this.values.get(columnNames[i]);
		}
		return values;
	}
}
