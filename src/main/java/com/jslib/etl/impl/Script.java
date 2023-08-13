package com.jslib.etl.impl;

public class Script {
	private final Language language;
	private final String code;
	private final Scope scope;
	/** Optional name of the task owner. Null if this script is global. */
	private final String taskName;

	public Script(Language language, String code, Scope scope, String taskName) {
		this.language = language;
		this.code = code;
		this.scope = scope;
		this.taskName = taskName;
	}

	public Language getLanguage() {
		return language;
	}

	public String getCode() {
		return code;
	}

	public Scope getScope() {
		return scope;
	}

	public String getTaskName() {
		return taskName;
	}

	public boolean isGlobal() {
		return scope == Scope.GLOBAL;
	}

	public boolean isPreTask(String taskName) {
		return scope == Scope.PRE_TASK && taskName.equals(this.taskName);
	}

	public boolean isPostTask(String taskName) {
		return scope == Scope.POST_TASK && taskName.equals(this.taskName);
	}

	public enum Language {
		SQL, UNKNOWN;

		public static Language forName(String language) {
			try {
				return valueOf(language.toUpperCase());
			} catch (Exception e) {
				return UNKNOWN;
			}
		}
	}
}
