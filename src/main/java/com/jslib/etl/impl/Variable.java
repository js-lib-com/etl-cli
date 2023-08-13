package com.jslib.etl.impl;

// TODO: maybe rename to property?
public class Variable {
	private final String name;
	/** Optional expression, evaluated for variable value loading. Null if value is a constant. */
	private final String expression;
	private final Type type;
	/** Optional name of the task owner. Null if this variable is global. */
	private final String taskName;

	private Object value;

	public Variable(String name, String expression, Type type, String taskName) {
		this.name = name;
		this.expression = expression;
		this.type = type;
		this.taskName = taskName;
	}

	public Variable(String name, Object value) {
		this.name = name;
		this.expression = null;
		this.type = Type.CONSTANT;
		this.taskName = null;
		this.value = value;
	}

	public String getName() {
		return name;
	}

	public String getExpression() {
		return expression;
	}

	public Type getType() {
		return type;
	}

	public String getTaskName() {
		return taskName;
	}

	public boolean isGlobal() {
		return type == Type.GLOBAL;
	}

	public boolean isPreTask(String taskName) {
		return type == Type.PRE_TASK && taskName.equals(this.taskName);
	}

	public boolean isPostTask(String taskName) {
		return type == Type.POST_TASK && taskName.equals(this.taskName);
	}

	public boolean isConstant() {
		return type == Type.CONSTANT;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "Variable [name=" + name + ", expression=" + expression + ", type=" + type + ", taskName=" + taskName + ", value=" + value + "]";
	}

	public static enum Type {
		GLOBAL, PRE_TASK, POST_TASK, CONSTANT
	}
}
