package com.jslib.etl.meta;

public class ArgumentMeta {
	private final String shortOption;
	private final String longOption;
	private final String parameter;
	private final String description;

	public ArgumentMeta(String shortOption, String longOption, String parameter, String description) {
		this.shortOption = shortOption;
		this.longOption = longOption;
		this.parameter = parameter;
		this.description = description;
	}

	public String getShortOption() {
		return shortOption;
	}

	public String getLongOption() {
		return longOption;
	}

	public String getParameter() {
		return parameter;
	}

	public String getDescription() {
		return description;
	}

	@Override
	public String toString() {
		return "ArgumentMeta [shortOption=" + shortOption + ", longOption=" + longOption + ", parameter=" + parameter + ", description=" + description + "]";
	}
}
