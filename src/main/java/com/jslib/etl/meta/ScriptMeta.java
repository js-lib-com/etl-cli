package com.jslib.etl.meta;

public class ScriptMeta {
	private final String language;
	private final StringBuilder code;
	/** Name of the section from ETL execution plan on which variable is defined. */
	private final String sectionName;
	/** Whether or not a task definition exists into parent section at the point variable is defined. */
	private final boolean taskExists;

	public ScriptMeta(String language, String sectionName, boolean taskExists) {
		super();
		this.language = language;
		this.code = new StringBuilder();
		this.sectionName = sectionName;
		this.taskExists = taskExists;
	}

	public String getLanguage() {
		return language;
	}

	public void addLine(String line) {
		code.append(line);
		code.append(System.lineSeparator());
	}

	public String getCode() {
		return code.toString();
	}

	public String getSectionName() {
		return sectionName;
	}

	public boolean isTaskExists() {
		return taskExists;
	}
}
