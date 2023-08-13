package com.jslib.etl;

import java.sql.SQLException;
import java.util.List;

import com.jslib.etl.meta.ServiceType;

public interface IDataSource extends AutoCloseable {

	ServiceType getType();

	String getName();

	IExtractor extractor(String tableName, List<String> columnNames, String... whereClause);

	ILoader loader(String tableName);

	ILoader loader(String tableName, boolean speedOptimization);

	String testConnection();
	
	Object lastInsertedId(String tableName);
	
	Object execute(String query) throws SQLException;

	char getEscapeChar();
	
}
