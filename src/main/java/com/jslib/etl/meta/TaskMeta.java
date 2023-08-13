package com.jslib.etl.meta;

import java.util.ArrayList;
import java.util.List;

import com.jslib.etl.EtlException;
import com.jslib.util.Params;
import com.jslib.util.Strings;

/**
 * ETL task meta.
 * 
 * @author Iulian Rotaru
 */
public class TaskMeta {
	private final String name;
	private final MetaProperties properties;
	private final TransferMeta transferMeta;

	/** Extractor data source name, possible null if this task extract data only from variables. */
	private String extractorDataSourceName;
	/** Extractor table name, relative to extractor data source, possible null if this task extract data only from variables. */
	private String extractorTableName;
	private final List<String> extractorColumnNames;
	
	private String loaderDataSourceName;
	private String loaderTableName;

	public TaskMeta(String sectionName, MetaProperties sectionProperties) {
		Params.notNull(sectionName, "Section name");
		this.name = sectionName;
		this.properties = sectionProperties != null ? sectionProperties : new MetaProperties();
		this.transferMeta = new TransferMeta();
		
		this.extractorColumnNames = new ArrayList<>();
	}

	public String getName() {
		return name;
	}

	public MetaProperties getProperties() {
		return properties;
	}

	public String getExtractorDataSourceName() {
		return extractorDataSourceName;
	}

	public String getExtractorTableName() {
		return extractorTableName;
	}

	public List<String> getExtractorColumnNames() {
		return extractorColumnNames;
	}

	public String getLoaderDataSourceName() {
		return loaderDataSourceName;
	}

	public String getLoaderTableName() {
		return loaderTableName;
	}

	public TransferMeta getTransferMeta() {
		return transferMeta;
	}

	// --------------------------------------------------------------------------------------------

	void setExtractorTable(String extractorTable) {
		Params.notNullOrEmpty(extractorTable, "Extractor table");
		String[] parts = extractorTable.split("\\.");
		this.extractorDataSourceName = parts[0].trim();
		this.extractorTableName = parts[1].trim();
	}

	void setLoaderTable(String loaderTable) {
		Params.notNullOrEmpty(loaderTable, "Loader table");
		String[] parts = loaderTable.split("\\.");
		this.loaderDataSourceName = parts[0].trim();
		this.loaderTableName = parts[1].trim();
	}

	void addTransferRule(String sourceReference, String transformerName, String destinationReference) {
		TransferRule transferRule = new TransferRule();

		if (sourceReference.startsWith("$")) {
			transferRule.setSourceReference(sourceReference);
		} else {
			if (extractorDataSourceName == null) {
				String[] parts = sourceReference.split("\\.");
				if (parts.length != 3) {
					throw new EtlException("Invalid source reference " + sourceReference);
				}
				extractorDataSourceName = parts[0].trim();
				extractorTableName = parts[1].trim();
				transferRule.setSourceReference(parts[2].trim());
				extractorColumnNames.add(parts[2].trim());
			} else {
				String columnName = Strings.last(sourceReference, '.');
				transferRule.setSourceReference(columnName);
				extractorColumnNames.add(columnName);
			}
		}

		if (loaderDataSourceName == null) {
			String[] parts = destinationReference.split("\\.");
			if (parts.length != 3) {
				throw new EtlException("Invalid destination reference " + destinationReference);
			}
			loaderDataSourceName = parts[0].trim();
			loaderTableName = parts[1].trim();
			transferRule.setDestinationReference(parts[2].trim());
		} else {
			transferRule.setDestinationReference(Strings.last(destinationReference, '.'));
		}

		transferRule.setTransformerName(transformerName);
		transferMeta.addTransferRule(transferRule);
	}

	@Override
	public String toString() {
		return "TaskMeta [transferMeta=" + transferMeta + ", extractorDataSourceName=" + extractorDataSourceName + ", extractorTableName=" + extractorTableName + ", loaderDataSourceName=" + loaderDataSourceName + ", loaderTableName=" + loaderTableName + "]";
	}
}
