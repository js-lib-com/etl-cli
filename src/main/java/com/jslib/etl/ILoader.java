package com.jslib.etl;

import java.sql.SQLException;

public interface ILoader extends AutoCloseable {

	/**
	 * Implementation should optimize writing to output data source(s) using batches of records.
	 * 
	 * @param record
	 * @throws SQLException 
	 */
	void write(DataRecord record) throws SQLException;
	
}
