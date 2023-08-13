package com.jslib.etl.impl;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import com.jslib.api.log.Log;
import com.jslib.api.log.LogFactory;
import com.jslib.etl.DataRecord;
import com.jslib.etl.IDataReference;
import com.jslib.etl.IDataSource;
import com.jslib.etl.IDataValue;
import com.jslib.etl.IExtractor;
import com.jslib.etl.ILoader;
import com.jslib.etl.IProject;
import com.jslib.etl.ITask;
import com.jslib.etl.ITransformer;
import com.jslib.etl.meta.TaskMeta;
import com.jslib.etl.meta.TransferRule;

class Task implements ITask {
	private static final Log log = LogFactory.getLog(Task.class);

	private final IProject project;
	private final TaskMeta taskMeta;
	private final Transformers transformers;

	public Task(IProject project, Functions functions, TaskMeta taskMeta) {
		this.project = project;
		this.taskMeta = taskMeta;
		this.transformers = new Transformers(functions);
	}

	@Override
	public String getName() {
		return taskMeta.getName();
	}

	@Override
	public boolean execute() {
		project.onPreExecuteTask(taskMeta.getName());

		long startTimeMillis = System.currentTimeMillis();
		boolean speedOptimization = taskMeta.getProperties().get("speedOptimization", boolean.class, true);
		String extractorFilter = project.injectVariables(taskMeta.getProperties().get("extractorFilter"));

		// if task extracts data ONLY from variables extractor data source is not configured
		IExtractor extractor = null;
		if (taskMeta.getExtractorDataSourceName() != null) {
			IDataSource sourceDataSource = project.getDataSource(taskMeta.getExtractorDataSourceName());
			if (extractorFilter != null) {
				extractor = sourceDataSource.extractor(taskMeta.getExtractorTableName(), taskMeta.getExtractorColumnNames(), extractorFilter);
			} else {
				extractor = sourceDataSource.extractor(taskMeta.getExtractorTableName(), taskMeta.getExtractorColumnNames());
			}
		} else {
			extractor = new VariablesExtractor();
		}

		final AtomicInteger records = new AtomicInteger();
		IDataSource destinationDataSource = project.getDataSource(taskMeta.getLoaderDataSourceName());
		try (ILoader loader = destinationDataSource.loader(taskMeta.getLoaderTableName(), speedOptimization)) {
			for(DataRecord sourceRecord: extractor) {
				records.incrementAndGet();
				DataRecord destinationRecord = new DataRecord();
				for (TransferRule transferRule : taskMeta.getTransferMeta()) {
					IDataReference sourceReference = transferRule.getSourceReference();
					IDataValue sourceValue;
					if (sourceReference.isVariable()) {
						// is not allowed to mix variables with column names
						sourceValue = DataRecord.value(project.getVariableValue(sourceReference.columnName()));
					} else {
						assert sourceRecord != null;
						sourceValue = sourceRecord.get(transferRule.getSourceReference());
					}

					ITransformer transformer = transformers.getTransformers(transferRule);
					IDataValue destinationValue = transformer.transform(sourceValue);

					destinationRecord.put(transferRule.getDestinationReference(), destinationValue);
				}
				loader.write(destinationRecord);
			};
		} catch (Throwable e) {
			log.error("Task '{task_name}' error: {exception}", taskMeta.getName(), e);
			return false;
		}

		log.info("Task '{task_name}' processed {} records in {processing_time} seconds.", taskMeta.getName(), records.get(), (System.currentTimeMillis() - startTimeMillis) / 1000.0);
		project.onPostExecuteTask(taskMeta.getName());
		return true;
	}

	/**
	 * Special case extractor used only when ETL task reads all values from variables and, therefore, has no data source
	 * configured. In this care extractor returns a single null data record. Null record is acceptable because task processing
	 * loop does not attempt to use source data record if rule source reference is a variable.
	 * 
	 * @author Iulian Rotaru
	 */
	private static class VariablesExtractor implements IExtractor {
		private int index = 1;

		@Override
		public Iterator<DataRecord> iterator() {
			return new Iterator<DataRecord>() {
				@Override
				public boolean hasNext() {
					return index > 0;
				}

				@Override
				public DataRecord next() {
					--index;
					return null;
				}
			};
		}
	}
}
