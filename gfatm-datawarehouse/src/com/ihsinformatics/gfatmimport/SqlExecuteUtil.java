package com.ihsinformatics.gfatmimport;

import java.io.File;
import java.sql.SQLException;

import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.SQLExec;

public class SqlExecuteUtil extends SQLExec {

	public SqlExecuteUtil(String url, String driver, String username,
			String password) throws SQLException {
		setDriver(driver);
		setUserid(username);
		setPassword(password);
		setUrl(url);
		Project project = new Project();
		project.init();
		setProject(project);
		setTaskType("sql");
		setTaskName("sql");
	}

	/**
	 * Input SQL script file and execute all commands in a batch
	 * 
	 * @param filePath
	 */
	public void execute(String filePath) {
		setSrc(new File(filePath));
		execute();
	}
}
