package com.jslib.etl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.jslib.util.Types;

/**
 * Data record is a row from data table. It is a dictionary of column names used as key and related table row values.
 * 
 * @author Iulian Rotaru
 */
public class DataRecord implements IQueueElement {
	private final Map<String, Object> values = new HashMap<>();

	public void put(IDataReference reference, IDataValue value) {
		if (!reference.isArray()) {
			this.values.put(reference.columnName(), object(value));
			return;
		}

		String[] columnNames = reference.columnNames();
		Object[] values = array(value);
		if (columnNames.length != values.length) {
			throw new IllegalStateException("Data reference not consistent with data value.");
		}
		for (int i = 0; i < columnNames.length; ++i) {
			this.values.put(columnNames[i], values[i]);
		}
	}

	public IDataValue get(IDataReference reference) {
		if (!reference.isArray()) {
			return value(values.get(reference.columnName()));
		}

		String[] columnNames = reference.columnNames();
		Object[] values = new Object[columnNames.length];
		for (int i = 0; i < columnNames.length; ++i) {
			values[i] = this.values.get(columnNames[i]);
		}
		return value(values);
	}

	public void put(String columnName, Object value) {
		values.put(columnName, value);
	}

	public Object get(String columnName) {
		return values.get(columnName);
	}

	public Collection<String> columnNames() {
		return values.keySet();
	}

	@Override
	public String toString() {
		return "DataRecord [values=" + values + "]";
	}

	public static IDataValue value(Object value) {
		if (Types.isArray(value)) {
			return new DataArray((Object[]) value);
		}
		return new DataObject(value);
	}

	public static Object object(IDataValue value) {
		if (value instanceof DataArray) {
			throw new IllegalArgumentException("Bad data value type: expected data object but got data array.");
		}
		return ((DataObject) value).value();
	}

	public static Object[] array(IDataValue value) {
		if (value instanceof DataObject) {
			throw new IllegalArgumentException("Bad data value type: expected data array but got data object.");
		}
		return ((DataArray) value).value();
	}

	private static class DataObject implements IDataValue {
		private final Object value;

		public DataObject(Object value) {
			this.value = value;
		}

		@Override
		public Object value() {
			return value;
		}
	}

	private static class DataArray implements IDataValue {
		private final Object[] value;

		public DataArray(Object[] value) {
			this.value = value;
		}

		@Override
		public Object[] value() {
			return value;
		}
	}
}
