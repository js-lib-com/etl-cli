package com.jslib.etl;

/**
 * A reference to {@link IDataValue} that could designate a single or an array of values.
 * 
 * @author Iulian Rotaru
 */
public interface IDataReference {

	boolean isArray();

	boolean isObject();

	boolean isVariable();
	
	String columnName();

	String[] columnNames();

}
