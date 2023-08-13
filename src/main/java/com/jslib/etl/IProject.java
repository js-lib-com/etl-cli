package com.jslib.etl;

import java.util.List;

import com.jslib.etl.meta.ArgumentMeta;
import com.jslib.etl.meta.ProjectMeta;

public interface IProject extends AutoCloseable {

	void parseArguments(List<ArgumentMeta> argumentsMeta, String... arguments);

	void parseMeta(ProjectMeta projectMeta);
	
	IDataSource getDataSource(String name);
	
	Object getVariableValue(String name);
	
	List<ITask> getTasks();
	
	void onPreExecuteTask(String taskName);

	void onPostExecuteTask(String taskName);
		
	String injectVariables(String text);

}
