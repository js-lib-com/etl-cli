package com.jslib.etl.meta;

import java.util.Arrays;

import com.jslib.etl.IDataReference;
import com.jslib.util.Params;

/**
 * Transfer rule is defined by {@link TransferMeta}.
 * 
 * @author Iulian Rotaru
 */
public class TransferRule {
	private IDataReference sourceReference;
	private IDataReference destinationReference;
	/** Optional unique name for transformer function. If this name is null Identity function is assumed. */
	private String transformerName;

	public IDataReference getSourceReference() {
		return sourceReference;
	}

	public IDataReference getDestinationReference() {
		return destinationReference;
	}

	public String getTransformerName() {
		return transformerName;
	}

	void setSourceReference(String sourceReference) {
		Params.notNullOrEmpty(sourceReference, "Source reference");
		this.sourceReference = new DataReference(sourceReference.split(","));
	}

	void setDestinationReference(String destinationReference) {
		Params.notNullOrEmpty(destinationReference, "Destination reference");
		this.destinationReference = new DataReference(destinationReference.split(","));
	}

	void setTransformerName(String transformerName) {
		this.transformerName = transformerName;
	}

	private static class DataReference implements IDataReference {
		private final String[] columnNames;

		public DataReference(String[] columnNames) {
			this.columnNames = columnNames;
		}

		@Override
		public boolean isArray() {
			return columnNames.length > 1;
		}

		@Override
		public boolean isObject() {
			return columnNames.length == 1;
		}

		@Override
		public boolean isVariable() {
			// TODO: for now is not allowed to mix variables with column names
			return columnNames.length == 1 && columnNames[0].startsWith("$");
		}

		@Override
		public String columnName() {
			if (columnNames.length != 1) {
				throw new UnsupportedOperationException("Attempt to get single column name from an array reference.");
			}
			return columnNames[0];
		}

		@Override
		public String[] columnNames() {
			if (columnNames.length <= 1) {
				throw new UnsupportedOperationException("Attempt to get multiple column names from an object reference.");
			}
			return columnNames;
		}

		@Override
		public String toString() {
			return "DataReference [columnNames=" + Arrays.toString(columnNames) + "]";
		}
	}
}
