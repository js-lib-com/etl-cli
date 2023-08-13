package com.jslib.etl;

public interface IInputRecord {

	Object[] getValues(String[] columnNames);

}