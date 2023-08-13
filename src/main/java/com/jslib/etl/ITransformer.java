package com.jslib.etl;

@FunctionalInterface
public interface ITransformer {

	IDataValue transform(IDataValue value);
	
}
