package com.jslib.etl.meta;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.jslib.etl.IDataReference;

/**
 * Transfer meta defines transfer rules - {@link TransferRule}. An ETL task has a transfer meta that define source values
 * processing.
 * 
 * @author Iulian Rotaru
 */
public class TransferMeta implements Iterable<TransferRule> {
	private final List<TransferRule> rules;

	public TransferMeta() {
		this.rules = new ArrayList<>();
	}

	@Override
	public Iterator<TransferRule> iterator() {
		return rules.iterator();
	}

	public void addTransferRule(TransferRule rule) {
		rules.add(rule);
	}

	public List<String> getSourceColumnNames() {
		List<String> columnNames = new ArrayList<>();

		for (TransferRule rule : rules) {
			IDataReference reference = rule.getSourceReference();
			if (reference.isObject()) {
				columnNames.add(reference.columnName());
				continue;
			}
			assert reference.isArray();
			for (String columnName : reference.columnNames()) {
				columnNames.add(columnName);
			}
		}

		return columnNames;
	}
}
