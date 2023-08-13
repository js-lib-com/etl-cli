package com.jslib.etl.meta;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class MarkdownParser implements IParser {
	private static final LinePattern SECTION_PATTERN = LinePattern.compile("^(?:#{1,3}\\s*)(.+)\\s*$");
	private static final LinePattern ARGUMENTS_PATTERN = LinePattern.compile("^\\| Short\\s+\\| Long\\s+\\| Parameter\\s+\\| Description\\s+\\|$");
	private static final LinePattern ARGUMENTS_OPTION = LinePattern.compile("^\\| (\\-[^ ]+)\\s+\\| (\\-\\-[^ ]+)?\\s+\\| ([^ ]+)\\s+\\| (.+)?\\s+\\|$");
	private static final LinePattern CONSTANT_PATTERN = LinePattern.compile("^(?:- )?([^ ]+)\\s*=\\s*(.+)$");
	private static final LinePattern VARIABLE_PATTERN = LinePattern.compile("^(?:- )?([^ ]+)\\s*:=\\s*(.+)$");
	private static final LinePattern SCRIPT_PATTERN = LinePattern.compile("^```(\\w+)$");
	private static final LinePattern SERVICE_PATTERN = LinePattern.compile("^(?:- )?service\\s*:\\s*(.+)$");
	private static final LinePattern PROPERTY = LinePattern.compile("^(?:- )?([^:]+)\\s*:\\s*(.+)$");
	private static final LinePattern TASK_PATTERN = LinePattern.compile("^\\| Extract (?:- (.+))?\\s+\\| Transform\\s+\\| Load (?:- (.+))?\\s+\\|$");
	private static final LinePattern TRANSFER_RULE = LinePattern.compile("^\\| ([^ ]+)\\s+\\| (.+)?\\s+\\| ([^ ]+)\\s+\\|$");

	@Override
	public ProjectMeta parse(Reader reader) throws IOException {
		ProjectMeta project = new ProjectMeta();

		String sectionName = null;
		MetaProperties properties = null;
		ServiceMeta service = null;
		TaskMeta task = null;
		ScriptMeta script = null;

		State state = State.SECTION;
		State outerState = null;

		try (BufferedReader linesReader = new BufferedReader(reader)) {
			String line;
			while ((line = linesReader.readLine()) != null) {
				line = line.trim();
				if (line.startsWith("!") || line.startsWith("TODO")) {
					continue;
				}

				if (CONSTANT_PATTERN.match(line)) {
					project.addConstant(sectionName, CONSTANT_PATTERN.group(1), CONSTANT_PATTERN.group(2));
					continue;
				}
				if (VARIABLE_PATTERN.match(line)) {
					project.addVariable(sectionName, VARIABLE_PATTERN.group(1), VARIABLE_PATTERN.group(2));
					continue;
				}
				if(SCRIPT_PATTERN.match(line)) {
					assert outerState == null;
					outerState = state;
					state = State.SCRIPT;
					script = project.createScript(sectionName, SCRIPT_PATTERN.group(1));
					continue;
				}
				if (SECTION_PATTERN.match(line)) {
					state = State.SECTION;
					sectionName = SECTION_PATTERN.group(1);
					properties = null;
					service = null;
					task = null;
					script = null;
				}

				switch (state) {
				case SECTION:
					if (ARGUMENTS_PATTERN.match(line)) {
						state = State.ARGUMENTS;
					} else if (TASK_PATTERN.match(line)) {
						state = State.TASK;
						task = project.createTask(sectionName, properties, TASK_PATTERN.group(1), TASK_PATTERN.group(2));
					} else if (SERVICE_PATTERN.match(line)) {
						state = State.SERVICE;
						service = project.createService(SERVICE_PATTERN.group(1));
					} else if (PROPERTY.match(line)) {
						state = State.PROPERTIES;
						properties = new MetaProperties();
						properties.put(PROPERTY.group(1), PROPERTY.group(2));
					}
					break;

				case ARGUMENTS:
					if (line.isEmpty()) {
						state = State.SECTION;
						break;
					}
					if (ARGUMENTS_OPTION.match(line)) {
						project.addArgument(ARGUMENTS_OPTION.group(1), ARGUMENTS_OPTION.group(2), ARGUMENTS_OPTION.group(3), ARGUMENTS_OPTION.group(4));
					}
					break;

				case PROPERTIES:
					if (!PROPERTY.match(line)) {
						state = State.SECTION;
						break;
					}
					properties.put(PROPERTY.group(1), PROPERTY.group(2));
					break;

				case SERVICE:
					if (!PROPERTY.match(line)) {
						state = State.SECTION;
						break;
					}
					assert service != null;
					service.putProperty(PROPERTY.group(1), PROPERTY.group(2));
					break;

				case TASK:
					if (TRANSFER_RULE.match(line)) {
						assert task != null;
						task.addTransferRule(TRANSFER_RULE.group(1), TRANSFER_RULE.group(2).trim(), TRANSFER_RULE.group(3));
					}
					break;

				case SCRIPT:
					if(line.equals("```")) {
						assert outerState != null;
						state = outerState;
						outerState = null;
						script = null;
						break;
					}
					script.addLine(line);
					break;
					
				default:

				}
			}
		}

		return project;
	}

	private static class LinePattern {
		public static LinePattern compile(String pattern) {
			return new LinePattern(pattern);
		}

		private final Pattern pattern;
		private Matcher matcher;

		private LinePattern(String pattern) {
			this.pattern = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
		}

		public boolean match(String line) {
			matcher = pattern.matcher(line);
			return matcher.find();
		}

		public String group(int group) {
			return matcher.group(group);
		}
	}

	private enum State {
		SECTION, ARGUMENTS, PROPERTIES, SERVICE, TASK, SCRIPT
	}
}
