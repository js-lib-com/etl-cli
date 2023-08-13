package com.jslib.etl.meta;

public class VariableMeta {
	/** Unique variable name. */
	private final String name;
	/** Variable expression. Project decides processing logic based on this expression pattern . */
	private final String expression;
	/** Name of the section from ETL execution plan on which variable is defined. */
	private final String sectionName;
	/** Whether or not a task definition exists into parent section at the point variable is defined. */
	private final boolean taskExists;

	public VariableMeta(String name, String expression, String sectionName, boolean taskExists) {
		this.name = name;
		this.expression = expression;
		this.sectionName = sectionName;
		this.taskExists = taskExists;
	}

	public String getName() {
		return name;
	}

	public String getExpression() {
		return expression;
	}

	public String getSectionName() {
		return sectionName;
	}

	public boolean isTaskExists() {
		return taskExists;
	}

	@Override
	public String toString() {
		return "VariableMeta [name=" + name + ", expression=" + expression + ", sectionName=" + sectionName + ", taskExists=" + taskExists + "]";
	}
}
