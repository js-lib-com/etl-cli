package com.jslib.etl.meta;

import java.util.ArrayList;
import java.util.List;

import com.jslib.etl.EtlException;

public class ProjectMeta {
	private final List<ArgumentMeta> arguments;
	private final List<ConstantMeta> constants;
	private final List<VariableMeta> variables;
	private final List<ServiceMeta> services;
	private final List<TaskMeta> tasks;
	private final List<ScriptMeta> scripts;

	public ProjectMeta() {
		this.arguments = new ArrayList<>();
		this.constants = new ArrayList<>();
		this.variables = new ArrayList<>();
		this.services = new ArrayList<>();
		this.tasks = new ArrayList<>();
		this.scripts = new ArrayList<>();
	}

	public List<ArgumentMeta> getArguments() {
		return arguments;
	}

	public List<ConstantMeta> getConstants() {
		return constants;
	}

	public List<VariableMeta> getVariables() {
		return variables;
	}

	public List<ServiceMeta> getServices() {
		return services;
	}

	public List<TaskMeta> getTasksMeta() {
		return tasks;
	}

	public List<ScriptMeta> getScripts() {
		return scripts;
	}

	// --------------------------------------------------------------------------------------------

	void addArgument(String shortOption, String longOption, String parameter, String description) {
		arguments.add(new ArgumentMeta(shortOption, longOption, parameter, description));
	}

	void addConstant(String sectionName, String name, String value) {
		constants.add(new ConstantMeta(name, value, sectionName));
	}

	void addVariable(String sectionName, String name, String expression) {
		variables.add(new VariableMeta(name, expression, sectionName, taskExists(sectionName)));
	}

	ServiceMeta createService(String type) {
		ServiceMeta service = new ServiceMeta(ServiceType.forType(type));
		services.add(service);
		return service;
	}

	TaskMeta createTask(String sectionName, MetaProperties sectionProperties, String extractorDefaultTable, String loaderDefaultTable) {
		if (taskExists(sectionName)) {
			throw new EtlException("Task section %s already defined.", sectionName);
		}

		TaskMeta task = new TaskMeta(sectionName, sectionProperties);
		if (extractorDefaultTable != null) {
			task.setExtractorTable(extractorDefaultTable);
		}
		if (loaderDefaultTable != null) {
			task.setLoaderTable(loaderDefaultTable);
		}
		tasks.add(task);
		return task;
	}

	ScriptMeta createScript(String sectionName, String language) {
		ScriptMeta script = new ScriptMeta(language, sectionName, taskExists(sectionName));
		scripts.add(script);
		return script;
	}
	
	private boolean taskExists(String name) {
		for (TaskMeta task : tasks) {
			if (task.getName().equals(name)) {
				return true;
			}
		}
		return false;
	}
}
