package com.jslib.etl.meta;

public class ConstantMeta {
	/** Unique constant name. */
	private final String name;
	/** Constant value. */
	private final String value;
	/** Name of the section from ETL execution plan on which constant is defined. */
	private final String sectionName;

	public ConstantMeta(String name, String value, String sectionName) {
		this.name = name;
		this.value = value;
		this.sectionName = sectionName;
	}

	public String getName() {
		return name;
	}

	public String getValue() {
		return value;
	}

	public String getSectionName() {
		return sectionName;
	}
}
