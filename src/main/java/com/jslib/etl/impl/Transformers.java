package com.jslib.etl.impl;

import com.jslib.etl.DataRecord;
import com.jslib.etl.EtlException;
import com.jslib.etl.IDataReference;
import com.jslib.etl.IDataValue;
import com.jslib.etl.ITransformer;
import com.jslib.etl.meta.TransferRule;
import com.jslib.util.Types;

public class Transformers {
	private final Functions functions;

	public Transformers(Functions functions) {
		this.functions = functions;
	}

	public ITransformer getTransformers(TransferRule rule) {
		if (rule.getTransformerName().isEmpty()) {
			return this::identity;
		}
		return new Proxy(rule);
	}

	private class Proxy implements ITransformer {
		private final TransferRule rule;

		public Proxy(TransferRule rule) {
			this.rule = rule;
		}

		@Override
		public IDataValue transform(IDataValue sourceValue) {
			Object destinationValue = functions.invoke(rule.getTransformerName(), sourceValue.value());

			IDataReference destinationReference = rule.getDestinationReference();
			if (destinationReference.isArray()) {
				if (!Types.isArray(destinationValue)) {
					throw new EtlException("Invalid response type. Expected array but got object.");
				}
			}
			if (destinationReference.isObject()) {
				if (Types.isArray(destinationValue)) {
					throw new EtlException("Invalid response type. Expected object but got array.");
				}
			}
			return DataRecord.value(destinationValue);
		}
	}

	private IDataValue identity(IDataValue value) {
		return value;
	}
}