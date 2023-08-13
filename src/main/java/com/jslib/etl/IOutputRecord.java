package com.jslib.etl;

public interface IOutputRecord {

	void setValue(String[] columnNames, Object[] values);

}