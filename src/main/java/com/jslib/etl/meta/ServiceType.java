package com.jslib.etl.meta;

import java.util.HashMap;
import java.util.Map;

import com.jslib.etl.EtlException;

public enum ServiceType {
	MYSQL, MSSQL, SQLITE, POSTGRESQL, MONGODB, CSV, JSON, LOG4J, GRAYLOG;

	private static final Map<String, ServiceType> SERVICES = new HashMap<>();
	static {
		SERVICES.put("MYSQL", MYSQL);
		SERVICES.put("MICROSOFTSQLSERVER", MSSQL);
		SERVICES.put("MICROSOFTSQL", MSSQL);
		SERVICES.put("MSSQLSERVER", MSSQL);
		SERVICES.put("MSSQL", MSSQL);
		SERVICES.put("SQLITE", SQLITE);
		SERVICES.put("POSTGRESQL", POSTGRESQL);
		SERVICES.put("POSTGRES", POSTGRESQL);
	}

	public static ServiceType forType(String type) {
		ServiceType service = SERVICES.get(type.toUpperCase().replaceAll("\\s+", ""));
		if (service == null) {
			throw new EtlException("Unknown or misspelled service type |%s|.", type);
		}
		return service;
	}
}
