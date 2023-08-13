package com.jslib.etl.impl;

import com.jslib.converter.Converter;
import com.jslib.converter.ConverterException;
import com.jslib.util.Params;
import com.jslib.util.Strings;

public class TableName implements Converter {
	private final String dataSourceName;
	private final String simpleName;

	public TableName() {
		this.dataSourceName = null;
		this.simpleName = null;
	}

	public TableName(String qualifiedTableName) {
		Params.notNullOrEmpty(qualifiedTableName, "Qualified table name");
		String[] parts = qualifiedTableName.split("\\.");
		if (parts.length != 2) {
			throw new IllegalArgumentException("Not a qualified table name: " + qualifiedTableName);
		}
		this.dataSourceName = parts[0];
		this.simpleName = parts[1];
	}

	public String getDataSourceName() {
		return dataSourceName;
	}

	public String getSimpleName() {
		return simpleName;
	}

	@Override
	public String toString() {
		return Strings.concat(dataSourceName, '.', simpleName);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T asObject(String string, Class<T> valueType) throws IllegalArgumentException, ConverterException {
		return (T) new TableName(string);
	}

	@Override
	public String asString(Object object) throws ConverterException {
		return object.toString();
	}
}
