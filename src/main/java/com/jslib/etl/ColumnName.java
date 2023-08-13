package com.jslib.etl;

public class ColumnName {
	private String dataSourceName;
	private String tableName;
	private String columnName;

	public String dataSource() {
		return dataSourceName;
	}

	public String table() {
		return tableName;
	}

	public String column() {
		return columnName;
	}
}
