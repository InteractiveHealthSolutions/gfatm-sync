/*
Copyright(C) 2016 Interactive Health Solutions, Pvt. Ltd.

This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation; either version 3 of the License (GPLv3), or any later version.
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with this program; if not, write to the Interactive Health Solutions, info@ihsinformatics.com
You can also access the license on the internet at the address: http://www.gnu.org/licenses/gpl-3.0.html
Interactive Health Solutions, hereby disclaims all copyright interest in this program written by the contributors. */
package com.ihsinformatics.gfatmimport;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.EventQueue;
import java.awt.FlowLayout;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.ComboBoxModel;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.JToggleButton;
import javax.swing.UIManager;
import javax.swing.text.BadLocationException;
import javax.swing.text.JTextComponent;
import javax.swing.text.SimpleAttributeSet;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyledDocument;

import org.apache.commons.lang3.ArrayUtils;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import com.ihsinformatics.util.DatabaseUtil;
import com.ihsinformatics.util.DateTimeUtil;
import com.ihsinformatics.util.RegexUtil;
import com.ihsinformatics.util.VersionUtil;
import com.jgoodies.forms.layout.ColumnSpec;
import com.jgoodies.forms.layout.FormLayout;
import com.jgoodies.forms.layout.FormSpecs;
import com.jgoodies.forms.layout.RowSpec;

/**
 * @author owais.hussain@ihsinformatics.com
 *
 */
public class GfatmImportMain implements ActionListener, KeyListener {

	private static final Logger log = Logger.getLogger(Class.class.getName());
	private static final VersionUtil version = new VersionUtil();
	private static String propertiesFile = "res/gfatm-import.properties";
	private static Properties props;
	private static String title = "GFATM Import";
	private static Scheduler scheduler;
	private DatabaseUtil serverDb;
	private DatabaseUtil localDb;

	public static GfatmImportMain gfatmImport = new GfatmImportMain();
	private final JFrame mainFrame = new JFrame();
	private final JTabbedPane tabbedPane = new JTabbedPane(JTabbedPane.TOP);
	private final JPanel serverPanel = new JPanel();
	private final JPanel clientPanel = new JPanel();
	private final JScrollPane scrollPane = new JScrollPane();
	private final JPanel progressPanel = new JPanel();
	private final JPanel optionsPanel = new JPanel();

	private final JLabel lblServerAddress = new JLabel("Server Address:");
	private final JLabel lblServerDriver = new JLabel("Driver:");
	private final JLabel lblServerDatabase = new JLabel("Database:");
	private final JLabel lblServerUsername = new JLabel("Username:");
	private final JLabel lblServerPassword = new JLabel("Password:");
	private final JLabel lblLocalAddress = new JLabel("Local Address:");
	private final JLabel lblLocalDriver = new JLabel("Driver:");
	private final JLabel lblLocalDatabase = new JLabel("Database:");
	private final JLabel lblLocalUsername = new JLabel("Username:");
	private final JLabel lblLocalPassword = new JLabel("Password:");
	private final JLabel lblDataToImport = new JLabel("Data to Import:");
	private final JLabel lblImportOption = new JLabel("Import Option:");
	private final JLabel lblProgress = new JLabel("Progress:");

	private final JTextField serverUrlTextField = new JTextField();
	private final JTextField serverDriverTextField = new JTextField();
	private final JTextField serverDatabaseTextField = new JTextField();
	private final JTextField serverUsernameTextField = new JTextField();
	private final JPasswordField serverPasswordField = new JPasswordField();
	private final JTextField localUrlTextField = new JTextField();
	private final JTextField localDriverTextField = new JTextField();
	private final JTextField localDatabaseTextField = new JTextField();
	private final JTextField localUsernameTextField = new JTextField();
	private final JPasswordField localPasswordField = new JPasswordField();
	private final JTextPane logTextPane = new JTextPane();

	private final JComboBox<String> importOptionComboBox = new JComboBox<String>();

	private final JCheckBox usersCheckBox = new JCheckBox(
			"Users and Attributes");
	private final JCheckBox locationsCheckBox = new JCheckBox(
			"Locations and Attributes");
	private final JCheckBox conceptsCheckBox = new JCheckBox(
			"Concepts and Form Types");
	private final JCheckBox otherMetadataCheckBox = new JCheckBox(
			"Other Metadata");

	private final JProgressBar progressBar = new JProgressBar();

	private final JToggleButton importButton = new JToggleButton("Import");

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		try {
			for (UIManager.LookAndFeelInfo info : UIManager
					.getInstalledLookAndFeels()) {
				if ("Nimbus".equals(info.getName())) {
					UIManager.setLookAndFeel(info.getClassName());
					break;
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					gfatmImport = new GfatmImportMain();
					gfatmImport.readProperties(propertiesFile);
					gfatmImport.mainFrame.setTitle(title + version.toString());
					gfatmImport.mainFrame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Record log in log file as well as text pane in colour coded form
	 * 
	 * @param message
	 * @param level
	 */
	public void log(String message, Level level) {
		log.log(level, message);
		if (logTextPane != null) {
			StyledDocument styledDoc = logTextPane.getStyledDocument();
			SimpleAttributeSet attrs = new SimpleAttributeSet();
			StyleConstants.setFontSize(attrs, 12);
			if (level.getName().equalsIgnoreCase(Level.WARNING.getName())) {
				StyleConstants.setForeground(attrs, Color.ORANGE);
			} else if (level.getName().equalsIgnoreCase(Level.SEVERE.getName())) {
				StyleConstants.setForeground(attrs, Color.RED);
			} else if (level.getName().equalsIgnoreCase(Level.FINE.getName())) {
				StyleConstants.setForeground(attrs, Color.GREEN);
			} else {
				StyleConstants.setForeground(attrs, Color.BLUE);
			}
			try {
				styledDoc.insertString(styledDoc.getLength(),
						DateTimeUtil.getSqlDateTime(new Date()) + ":\t"
								+ message + "\n", attrs);
			} catch (BadLocationException e) {
			}
			logTextPane.setCaretPosition(styledDoc.getLength() - 1);
		}
	}

	public void resetProgressBar(int min, int max) {
		progressBar.setMinimum(min);
		progressBar.setMaximum(max);
		progressBar.setValue(0);
	}

	public void updateProgress() {
		progressBar.setValue(progressBar.getValue() + 1);
	}

	/**
	 * Create the application.
	 */
	private GfatmImportMain() {
		initialize();
		log("Application initialized.", Level.INFO);
	}

	/**
	 * Initialize the contents of the mainFrame.
	 */
	private void initialize() {
		mainFrame
				.setIconImage(Toolkit
						.getDefaultToolkit()
						.getImage(
								GfatmImportMain.class
										.getResource("/com/sun/java/swing/plaf/windows/icons/Computer.gif")));
		mainFrame.setBounds(100, 100, 635, 425);
		mainFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		mainFrame.getContentPane().add(tabbedPane, BorderLayout.NORTH);
		tabbedPane.addTab("Server Connection", null, serverPanel, null);
		serverPanel.setLayout(new FormLayout(new ColumnSpec[] {
				FormSpecs.RELATED_GAP_COLSPEC, ColumnSpec.decode("87px"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("76px:grow"), FormSpecs.RELATED_GAP_COLSPEC,
				FormSpecs.DEFAULT_COLSPEC,
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("104px"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("80px"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("174px:grow"), }, new RowSpec[] {
				FormSpecs.LINE_GAP_ROWSPEC, RowSpec.decode("28px"),
				FormSpecs.RELATED_GAP_ROWSPEC, FormSpecs.DEFAULT_ROWSPEC, }));
		serverPanel.add(lblServerAddress, "2, 2, left, center");
		serverPanel.add(serverUrlTextField, "4, 2, 5, 1, fill, top");
		serverUrlTextField.setColumns(24);
		serverPanel.add(lblServerDriver, "10, 2, left, default");
		serverPanel.add(serverDriverTextField, "12, 2, left, default");
		serverPanel.add(lblServerDatabase, "2, 4, left, default");
		serverPanel.add(serverDatabaseTextField, "4, 4, fill, default");
		serverPanel.add(lblServerUsername, "6, 4, left, center");
		serverPanel.add(serverUsernameTextField, "8, 4, fill, top");
		serverUsernameTextField.setColumns(10);
		serverPanel.add(lblServerPassword, "10, 4, left, center");
		serverPasswordField.setColumns(10);
		serverPanel.add(serverPasswordField, "12, 4, left, top");
		tabbedPane.addTab("Local Connection", null, clientPanel, null);
		clientPanel.setLayout(new FormLayout(new ColumnSpec[] {
				FormSpecs.RELATED_GAP_COLSPEC, ColumnSpec.decode("87px"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("30px:grow"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("91px"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("90px"), FormSpecs.RELATED_GAP_COLSPEC,
				FormSpecs.DEFAULT_COLSPEC,
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("108px:grow"), }, new RowSpec[] {
				FormSpecs.LINE_GAP_ROWSPEC, RowSpec.decode("28px"),
				FormSpecs.RELATED_GAP_ROWSPEC, FormSpecs.DEFAULT_ROWSPEC, }));
		clientPanel.add(lblLocalAddress, "2, 2, left, center");
		localUrlTextField.setColumns(24);
		clientPanel.add(localUrlTextField, "4, 2, 5, 1, fill, top");
		clientPanel.add(lblLocalDriver, "10, 2, left, default");
		clientPanel.add(localDriverTextField, "12, 2, left, default");
		clientPanel.add(lblLocalDatabase, "2, 4, left, default");
		clientPanel.add(localDatabaseTextField, "4, 4, fill, default");
		clientPanel.add(lblLocalUsername, "6, 4, left, center");
		localUsernameTextField.setColumns(8);
		clientPanel.add(localUsernameTextField, "8, 4, fill, top");
		clientPanel.add(lblLocalPassword, "10, 4, left, center");
		localPasswordField.setColumns(10);
		clientPanel.add(localPasswordField, "12, 4, left, top");
		JPanel centerPanel = new JPanel();
		mainFrame.getContentPane().add(centerPanel, BorderLayout.CENTER);
		centerPanel.setLayout(new FormLayout(new ColumnSpec[] {
				FormSpecs.RELATED_GAP_COLSPEC,
				ColumnSpec.decode("max(85dlu;min)"),
				FormSpecs.RELATED_GAP_COLSPEC,
				ColumnSpec.decode("max(120dlu;default):grow"),
				FormSpecs.RELATED_GAP_COLSPEC,
				ColumnSpec.decode("max(140dlu;default):grow"), },
				new RowSpec[] { FormSpecs.RELATED_GAP_ROWSPEC,
						FormSpecs.DEFAULT_ROWSPEC,
						FormSpecs.RELATED_GAP_ROWSPEC, FormSpecs.MIN_ROWSPEC,
						FormSpecs.RELATED_GAP_ROWSPEC,
						FormSpecs.DEFAULT_ROWSPEC,
						FormSpecs.RELATED_GAP_ROWSPEC,
						FormSpecs.DEFAULT_ROWSPEC,
						FormSpecs.RELATED_GAP_ROWSPEC,
						RowSpec.decode("default:grow"), }));
		centerPanel.add(lblDataToImport, "2, 2, 5, 1");
		centerPanel.add(optionsPanel, "2, 4, 5, 1, left, fill");
		usersCheckBox.setSelected(true);
		conceptsCheckBox.setSelected(true);
		locationsCheckBox.setSelected(true);
		otherMetadataCheckBox.setSelected(true);
		optionsPanel.setLayout(new FlowLayout(FlowLayout.CENTER, 5, 5));
		optionsPanel.add(usersCheckBox);
		optionsPanel.add(conceptsCheckBox);
		optionsPanel.add(locationsCheckBox);
		optionsPanel.add(otherMetadataCheckBox);
		centerPanel.add(lblImportOption, "2, 6, 5, 1");
		ComboBoxModel<String> comboBoxModel = new DefaultComboBoxModel<String>(
				new String[] { "Every 6 hours", "Twice a day", "Daily" });
		importOptionComboBox.setModel(comboBoxModel);
		centerPanel.add(importOptionComboBox, "2, 8, fill, default");
		centerPanel.add(importButton, "4, 8, fill, default");
		centerPanel.add(progressPanel, "6, 8, fill, center");
		progressPanel.setLayout(new FormLayout(new ColumnSpec[] {
				FormSpecs.RELATED_GAP_COLSPEC, FormSpecs.DEFAULT_COLSPEC,
				ColumnSpec.decode("center:120px"), },
				new RowSpec[] { FormSpecs.DEFAULT_ROWSPEC, }));
		progressPanel.add(lblProgress, "2, 1");
		progressPanel.add(progressBar, "3, 1, fill, fill");
		progressBar.setStringPainted(true);
		centerPanel.add(scrollPane, "2, 10, 5, 1, fill, fill");
		scrollPane.setViewportView(logTextPane);
		importButton.addActionListener(this);
		serverUrlTextField.addKeyListener(this);
		serverDriverTextField.addKeyListener(this);
		serverDatabaseTextField.addKeyListener(this);
		serverUsernameTextField.addKeyListener(this);
		localUrlTextField.addKeyListener(this);
		localDriverTextField.addKeyListener(this);
		localDatabaseTextField.addKeyListener(this);
		localUsernameTextField.addKeyListener(this);
		;
	}

	/**
	 * Read properties from properties file
	 */
	public void readProperties(String propertiesFile) {
		localDatabaseTextField.setColumns(10);
		localDriverTextField.setColumns(15);
		serverDatabaseTextField.setColumns(10);
		serverDriverTextField.setColumns(15);
		InputStream propFile;
		try {
			propFile = new FileInputStream(propertiesFile);
			if (propFile != null) {
				props = new Properties();
				props.load(propFile);
				String versionStr = props.getProperty("app.version");
				try {
					version.parseVersion(versionStr);
				} catch (NumberFormatException e) {
					log("Invalid version in properties file.", Level.WARNING);
				} catch (NullPointerException e) {
					log("Version not found in properties file.", Level.WARNING);
				} catch (ParseException e) {
					log("Bad version name found in properties file.",
							Level.WARNING);
				}
				String serverUrl = props.getProperty("remote.connection.url",
						"jdbc:mysql://202.141.249.106:6847/openmrs");
				serverUrlTextField.setText(serverUrl);
				String serverUsername = props.getProperty(
						"remote.connection.username", "gfatm_user");
				serverUsernameTextField.setText(serverUsername);
				String serverPassword = props.getProperty(
						"remote.connection.password", "");
				serverPasswordField.setText(serverPassword);
				String localUrl = props.getProperty("localDb.connection.url",
						"jdbc:mysql://localhost:3306/openmrs");
				String serverDriver = props.getProperty(
						"remote.connection.driver", "com.mysql.jdbc.Driver");
				serverDriverTextField.setText(serverDriver);
				String serverDatabase = props.getProperty(
						"remote.connection.database", "openmrs");
				serverDatabaseTextField.setText(serverDatabase);
				localUrlTextField.setText(localUrl);
				String localUsername = props.getProperty(
						"local.connection.username", "root");
				localUsernameTextField.setText(localUsername);
				String localPassword = props.getProperty(
						"local.connection.password", "");
				localPasswordField.setText(localPassword);
				String localDriver = props.getProperty(
						"local.connection.driver", "com.mysql.jdbc.Driver");
				localDriverTextField.setText(localDriver);
				String localDatabase = props.getProperty(
						"local.connection.database", "openmrs");
				localDatabaseTextField.setText(localDatabase);
			}
		} catch (FileNotFoundException e1) {
			log("Properties file not found or is inaccessible.", Level.SEVERE);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Set import mode and enable/disable fields
	 * 
	 * @param importStatus
	 */
	public void setMode(ImportStatus importStatus) {
		Component[] clientComponents = clientPanel.getComponents();
		Component[] serverComponents = serverPanel.getComponents();
		Component[] otherComponents = { importOptionComboBox, usersCheckBox,
				locationsCheckBox, conceptsCheckBox, otherMetadataCheckBox };
		Component[] all = ArrayUtils.addAll(clientComponents, serverComponents);
		all = ArrayUtils.addAll(all, otherComponents);
		switch (importStatus) {
		case STOPPED:
			log(importStatus.toString(), Level.WARNING);
			for (Component field : all) {
				field.setEnabled(true);
			}
			break;
		case IMPORTING:
		case WAITING:
			log(importStatus.toString(), Level.INFO);
			for (Component field : all) {
				field.setEnabled(false);
			}
			break;
		default:
			break;
		}
	}

	/**
	 * Validate fields before execution
	 * 
	 * @return
	 */
	public boolean validate() {
		boolean valid = true;
		StringBuilder error = new StringBuilder();
		String serverUrl = serverUrlTextField.getText().trim();
		String serverDriver = serverDriverTextField.getText().trim();
		String serverDatabase = serverDatabaseTextField.getText().trim();
		String serverUsername = serverUsernameTextField.getText().trim();
		String serverPassword = String.valueOf(serverPasswordField
				.getPassword());
		String localUrl = localUrlTextField.getText().trim();
		String localDatabase = localDatabaseTextField.getText().trim();
		String localDriver = localDriverTextField.getText().trim();
		String localUsername = localUsernameTextField.getText().trim();
		String localPassword = String.valueOf(localPasswordField.getPassword());
		boolean usersChecked = usersCheckBox.isSelected();
		boolean locationsChecked = locationsCheckBox.isSelected();
		boolean conceptsChecked = conceptsCheckBox.isSelected();
		boolean otherMetadataChecked = otherMetadataCheckBox.isSelected();
		// Check mandatory fields
		if (serverUrl.equals("")) {
			error.append("Server Address cannot be empty.\n");
		}
		if (serverUsername.equals("")) {
			error.append("Server Username cannot be empty.\n");
		}
		if (serverPassword.equals("")) {
			error.append("Server Password cannot be empty.\n");
		}
		if (localUrl.equals("")) {
			error.append("Local Address cannot be empty.\n");
		}
		if (localUsername.equals("")) {
			error.append("Local Username cannot be empty.\n");
		}
		if (localPassword.equals("")) {
			error.append("Local Password cannot be empty.\n");
		}
		if ((usersChecked | locationsChecked | conceptsChecked | otherMetadataChecked) == false) {
			error.append("At least one option must be checked to import.\n");
		}
		// Check data types
		if (!RegexUtil.isWord(serverUsername)) {
			error.append("Server Username is invalid.\n");
		}
		if (!RegexUtil.isWord(localUsername)) {
			error.append("Local Username is invalid.\n");
		}
		// Try connection
		try {
			serverDb = new DatabaseUtil();
			serverDb.setUrl(serverUrl);
			serverDb.setDriverName(serverDriver);
			serverDb.setUser(serverUsername, serverPassword);
			serverDb.setDbName(serverDatabase);
			localDb = new DatabaseUtil();
			localDb.setUrl(localUrl);
			localDb.setDriverName(localDriver);
			localDb.setUser(localUsername, localPassword);
			localDb.setDbName(localDatabase);
			if (!serverDb.tryConnection()) {
				error.append("Cannot connect with the Server. Please check serverDb connection URL and credentials.\n");
			}
			if (!localDb.tryConnection()) {
				error.append("Cannot connect with the Local database. Please check localDb connection URL and credentials.\n");
			}
		} catch (Exception e) {
		}
		valid = error.length() == 0;
		if (!valid) {
			JOptionPane.showMessageDialog(null, error.toString(), "Error!",
					JOptionPane.ERROR_MESSAGE);
		}
		return valid;
	}

	/**
	 * Starts import schedule
	 * 
	 * @throws SchedulerException
	 */
	public void importData() throws SchedulerException {
		int interval = -1;
		switch (importOptionComboBox.getSelectedIndex()) {
		case 0:
			interval = 6;
			break;
		case 1:
			interval = 12;
			break;
		default:
			interval = 24;
		}
		scheduler = StdSchedulerFactory.getDefaultScheduler();
		scheduler.start();
		JobDetail job = JobBuilder.newJob(ImportJob.class)
				.withIdentity("importJob", "importGroup").build();
		ImportJob importJobObj = new ImportJob();
		importJobObj.setRemoteDb(serverDb);
		importJobObj.setLocalDb(localDb);
		importJobObj.setImportUsers(usersCheckBox.isSelected());
		importJobObj.setImportLocations(locationsCheckBox.isSelected());
		importJobObj.setImportConcepts(conceptsCheckBox.isSelected());
		importJobObj.setImportOtherMetadata(otherMetadataCheckBox.isSelected());
		job.getJobDataMap().put("importJob", importJobObj);
		SimpleScheduleBuilder scheduleBuilder = SimpleScheduleBuilder
				.simpleSchedule().withIntervalInHours(interval);
		Trigger trigger = TriggerBuilder.newTrigger()
				.withIdentity("importTrigger", "importGroup")
				.withSchedule(scheduleBuilder).build();
		scheduler.scheduleJob(job, trigger);
	}

	public void actionPerformed(ActionEvent event) {
		JComponent source = (JComponent) event.getSource();
		if (source == importButton) {
			JToggleButton button = (JToggleButton) source;
			if (button.isSelected()) {
				setMode(ImportStatus.IMPORTING);
				if (validate()) {
					try {
						importData();
					} catch (SchedulerException e) {
						log(e.getMessage(), Level.SEVERE);
					}
				} else {
					importButton.setSelected(false);
					setMode(ImportStatus.STOPPED);
				}
			} else {
				try {
					setMode(ImportStatus.STOPPED);
					scheduler.shutdown();
					resetProgressBar(0, 100);
					JOptionPane.showMessageDialog(null,
							"Import process was interrupted in the middle.",
							"Warning!", JOptionPane.WARNING_MESSAGE);
				} catch (SchedulerException e) {
					e.printStackTrace();
				}
			}

		}

	}

	public void keyTyped(KeyEvent event) {
		JComponent source = (JComponent) event.getSource();
		JComponent[] keyFields = { serverUrlTextField, serverDriverTextField,
				serverDatabaseTextField, serverUsernameTextField,
				localUrlTextField, localDriverTextField,
				localDatabaseTextField, localUsernameTextField };
		for (JComponent field : keyFields) {
			if (source == field) {
				JTextComponent textField = (JTextComponent) source;
				textField.setText(textField.getText().trim());
			}
		}
	}

	public void keyPressed(KeyEvent event) {
		// TODO Auto-generated method stub
	}

	public void keyReleased(KeyEvent event) {
		// TODO Auto-generated method stub
	}
}
