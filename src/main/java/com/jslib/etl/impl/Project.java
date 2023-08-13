package com.jslib.etl.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.jslib.api.log.Log;
import com.jslib.api.log.LogFactory;
import com.jslib.converter.Converter;
import com.jslib.converter.ConverterRegistry;
import com.jslib.etl.EtlException;
import com.jslib.etl.IDataSource;
import com.jslib.etl.IProject;
import com.jslib.etl.ITask;
import com.jslib.etl.meta.ArgumentMeta;
import com.jslib.etl.meta.ProjectMeta;
import com.jslib.etl.meta.ServiceMeta;

public class Project implements IProject {
	private static final Log log = LogFactory.getLog(Project.class);

	private final Converter converter;
	private final Functions functions;

	private final LinkedHashMap<String, Variable> variables;
	private final List<Script> scripts;

	private final Map<String, IDataSource> dataSources;
	private final List<ITask> tasks;

	public Project() {
		this.converter = ConverterRegistry.getConverter();
		this.functions = new Functions(this);

		this.variables = new LinkedHashMap<>();
		this.scripts = new ArrayList<>();
		this.dataSources = new HashMap<>();
		this.tasks = new ArrayList<>();
	}

	@Override
	public void close() throws Exception {
		for(IDataSource dataSource:dataSources.values()) {
			dataSource.close();
		}
	}

	@Override
	public void parseArguments(List<ArgumentMeta> argumentsMeta, String... arguments) {
		// etl ngs.etl.md -t=1234567890 -d=/absolute/file/path -a=relative/archive/path
		// etl ngs.etl.md --table 1234567890 --database /absolute/file/path --ack relative/archive/path

		String parameter = null;
		for (String argument : arguments) {
			if (argument.startsWith("--")) {
				ArgumentMeta argumentMeta = argumentsMeta.stream().filter(meta -> meta.getLongOption().equals(argument)).findFirst().get();
				parameter = argumentMeta.getParameter();
			} else if (argument.startsWith("-")) {
				ArgumentMeta argumentMeta = argumentsMeta.stream().filter(meta -> meta.getShortOption().equals(argument)).findFirst().get();
				String name = argumentMeta.getParameter();
				String value = argument.substring(3);
				variables.put(name, new Variable(name, value));
			} else {
				if (parameter != null) {
					variables.put(parameter, new Variable(parameter, argument));
					parameter = null;
				}
			}
		}
	}

	@Override
	public void parseMeta(ProjectMeta projectMeta) {
		projectMeta.getConstants().forEach(constantMeta -> {
			variables.put(constantMeta.getName(), new Variable(constantMeta.getName(), constantMeta.getValue()));
		});

		projectMeta.getServices().forEach(serviceMeta -> serviceFactory(serviceMeta).create(serviceMeta));

		dataSources.forEach((name, dataSource) -> log.debug(dataSource.getName() + " data source: " + dataSource.testConnection()));

		List<String> tasksName = new ArrayList<>();
		projectMeta.getTasksMeta().forEach(taskMeta -> {
			tasksName.add(taskMeta.getName());
			this.tasks.add(new Task(this, functions, taskMeta));
		});

		projectMeta.getVariables().forEach(variableMeta -> {
			String taskName = variableMeta.getSectionName();
			Variable.Type type = Variable.Type.GLOBAL;
			if (tasksName.contains(taskName)) {
				type = variableMeta.isTaskExists() ? Variable.Type.POST_TASK : Variable.Type.PRE_TASK;
			} else {
				taskName = null;
			}
			if (variables.put(variableMeta.getName(), new Variable(variableMeta.getName(), variableMeta.getExpression(), type, taskName)) != null) {
				throw new EtlException("Variable |%s| redefinition on section |%s|.", variableMeta.getName(), variableMeta.getSectionName());
			}
		});
		evaluateVariables(variable -> variable.isGlobal());
		variables.forEach((n, v) -> log.debug("{}: {}", n, v));

		projectMeta.getScripts().forEach(scriptMeta -> {
			String taskName = scriptMeta.getSectionName();
			Scope scope = Scope.GLOBAL;
			if (tasksName.contains(taskName)) {
				scope = scriptMeta.isTaskExists() ? Scope.POST_TASK : Scope.PRE_TASK;
			} else {
				taskName = null;
			}
			Script.Language language = Script.Language.forName(scriptMeta.getLanguage());
			scripts.add(new Script(language, scriptMeta.getCode(), scope, taskName));
		});
		runScripts(script -> script.isGlobal());
	}

	@Override
	public void onPreExecuteTask(String taskName) {
		evaluateVariables(variable -> variable.isPreTask(taskName));
		runScripts(script -> script.isPreTask(taskName));
	}

	@Override
	public void onPostExecuteTask(String taskName) {
		evaluateVariables(variable -> variable.isPostTask(taskName));
		runScripts(script -> script.isPostTask(taskName));
	}

	// --------------------------------------------------------------------------------------------

	private ServiceFactory serviceFactory(ServiceMeta serviceMeta) {
		// takes care to inject variables into service properties before using them
		serviceMeta.getProperties().forEach((name, value) -> serviceMeta.getProperties().put(name, injectVariables(value)));

		switch (serviceMeta.getType()) {
		case GRAYLOG:
		case LOG4J:
			return this::createLogger;

		case MYSQL:
		case MSSQL:
		case SQLITE:
		case POSTGRESQL:
			return this::createSqlDataSource;

		case MONGODB:
			return this::createMongoDataSource;

		case CSV:
			return this::createCsvDataSource;

		case JSON:
			return this::createJsonDataSource;

		default:
			throw new IllegalStateException("Not handled service type " + serviceMeta.getType());
		}

	}

	private void createLogger(ServiceMeta serviceMeta) {

	}

	private void createSqlDataSource(ServiceMeta serviceMeta) {
		IDataSource dataSource = new SqlDataSource(serviceMeta);
		dataSources.put(serviceMeta.getProperties().get("name", "").toLowerCase(), dataSource);
	}

	private void createMongoDataSource(ServiceMeta serviceMeta) {

	}

	private void createCsvDataSource(ServiceMeta serviceMeta) {

	}

	private void createJsonDataSource(ServiceMeta serviceMeta) {

	}

	// --------------------------------------------------------------------------------------------

	@Override
	public IDataSource getDataSource(String name) {
		IDataSource dataSource = dataSources.get(name.toLowerCase());
		if (dataSource == null) {
			throw new IllegalArgumentException("Data source not found: " + name);
		}
		return dataSource;
	}

	@Override
	public List<ITask> getTasks() {
		return tasks;
	}

	// --------------------------------------------------------------------------------------------

	private void evaluateVariables(Predicate<Variable> filter) {
		for (Variable variable : variables.values().stream().filter(filter).collect(Collectors.toList())) {
			String expression = injectVariables(variable.getExpression());
			Object value = functions.invoke(expression);
			if (value != null) {
				log.debug("Evaluate variable {} value to {}.", variable.getName(), value);
				variables.get(variable.getName()).setValue(value);
			}
		}
	}

	@Override
	public Object getVariableValue(String name) {
		// on source code, variable name starts with dollar
		Variable variable = variables.get(name.substring(1));
		if (variable == null) {
			throw new EtlException("Variable %s not defined.", name);
		}
		return variable.getValue();
	}

	private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$(\\w+)");

	public String injectVariables(String text) {
		if (text == null) {
			return null;
		}
		StringBuffer buffer = new StringBuffer();

		Matcher matcher = VARIABLE_PATTERN.matcher(text);
		while (matcher.find()) {
			Variable variable = variables.get(matcher.group(1));
			if (variable != null) {
				String replacement = converter.asString(variable.getValue());
				matcher.appendReplacement(buffer, "");
				buffer.append(replacement);
			}
		}

		matcher.appendTail(buffer);
		return buffer.toString();
	}

	// --------------------------------------------------------------------------------------------

	private void runScripts(Predicate<Script> filter) {
		for (Script script : scripts.stream().filter(filter).collect(Collectors.toList())) {
			String code = injectVariables(script.getCode());
			if (script.getLanguage() != Script.Language.SQL) {
				throw new EtlException("Not supported script language |%s|.", script.getLanguage());
			}
			try (BufferedReader reader = new BufferedReader(new StringReader(code))) {
				functions.executeSql(reader.readLine());
			} catch (IOException e) {
				throw new EtlException("Error reading script code.");
			}
		}
	}
}
