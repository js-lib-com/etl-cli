package com.jslib.etl.impl;

import java.util.ArrayList;
import java.util.List;

import com.jslib.etl.ITask;

public class ForEachLoop implements ITask {
	private final List<ITask> tasks;

	public ForEachLoop() {
		this.tasks = new ArrayList<>();
	}

	public void addTask(ITask task) {
		tasks.add(task);
	}
	
	@Override
	public String getName() {
		return null;
	}

	@Override
	public boolean execute() {
		for (ITask task : tasks) {
			if (!task.execute()) {
				return false;
			}
		}
		return true;
	}
}
