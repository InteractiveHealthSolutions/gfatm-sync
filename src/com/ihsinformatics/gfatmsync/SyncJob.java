package com.ihsinformatics.gfatmsync;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class SyncJob implements Job {

	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		System.out.println("Hello Quartz!");

	}
}
