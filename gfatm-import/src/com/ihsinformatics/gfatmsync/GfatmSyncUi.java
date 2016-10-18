/**
 * 
 */
package com.ihsinformatics.gfatmsync;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.EventQueue;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
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
import javax.swing.JTabbedPane;
import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.JToggleButton;
import javax.swing.UIManager;
import javax.swing.text.BadLocationException;
import javax.swing.text.SimpleAttributeSet;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyledDocument;

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
public class GfatmSyncUi {

	private static final Logger log = Logger.getLogger(Class.class.getName());
	private static final VersionUtil version = new VersionUtil();
	private static String propertiesFile = "res/gfatm-sync.properties";
	private static Properties props;

	private final JFrame mainFrame = new JFrame();
	private final JTabbedPane tabbedPane = new JTabbedPane(JTabbedPane.TOP);
	private final JPanel serverPanel = new JPanel();
	private final JPanel clientPanel = new JPanel();

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
	private final JLabel lblDataToSynchronize = new JLabel(
			"Data to Synchronize:");
	private final JLabel lblSynchronizationOption = new JLabel(
			"Synchronization Option:");
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
	private static final JTextPane logTextPane = new JTextPane();

	private final JComboBox<String> syncOptionComboBox = new JComboBox<String>();

	private final JCheckBox usersCheckBox = new JCheckBox(
			"Users and Attributes");
	private final JCheckBox locationsCheckBox = new JCheckBox(
			"Locations and Attributes");
	private final JCheckBox conceptsCheckBox = new JCheckBox(
			"Concepts and Form Types");
	private final JCheckBox otherMetadataCheckBox = new JCheckBox(
			"Other Metadata");

	private static final JProgressBar progressBar = new JProgressBar();

	private final JToggleButton synchronizeButton = new JToggleButton(
			"Synchronize");

	private Scheduler scheduler;
	private DatabaseUtil serverDb;
	private DatabaseUtil localDb;

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
					GfatmSyncUi window = new GfatmSyncUi();
					window.readProperties(propertiesFile);
					window.mainFrame.setTitle("GFATM Synchronization "
							+ version.toString());
					window.mainFrame.setVisible(true);
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
	public static void log(String message, Level level) {
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

	public static void resetProgressBar(int min, int max) {
		progressBar.setMinimum(min);
		progressBar.setMaximum(max);
	}

	public static void updateProgress() {
		progressBar.setValue(progressBar.getValue() + 1);
	}

	/**
	 * Create the application.
	 */
	public GfatmSyncUi() {
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
								GfatmSyncUi.class
										.getResource("/com/sun/java/swing/plaf/windows/icons/Computer.gif")));
		mainFrame.setBounds(100, 100, 600, 500);
		mainFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		mainFrame.getContentPane().add(tabbedPane, BorderLayout.NORTH);
		tabbedPane.addTab("Server Connection", null, serverPanel, null);
		serverPanel.setLayout(new FormLayout(new ColumnSpec[] {
				FormSpecs.RELATED_GAP_COLSPEC, ColumnSpec.decode("87px"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("30px:grow"), FormSpecs.RELATED_GAP_COLSPEC,
				FormSpecs.DEFAULT_COLSPEC,
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("85px"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("59px"),
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
				FormSpecs.RELATED_GAP_COLSPEC, ColumnSpec.decode("63px:grow"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("62px"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("76px"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("59px"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("32px:grow"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("25px"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("56px"), },
				new RowSpec[] { FormSpecs.RELATED_GAP_ROWSPEC,
						FormSpecs.DEFAULT_ROWSPEC,
						FormSpecs.RELATED_GAP_ROWSPEC,
						FormSpecs.DEFAULT_ROWSPEC,
						FormSpecs.RELATED_GAP_ROWSPEC,
						FormSpecs.DEFAULT_ROWSPEC,
						FormSpecs.RELATED_GAP_ROWSPEC,
						FormSpecs.DEFAULT_ROWSPEC,
						FormSpecs.RELATED_GAP_ROWSPEC,
						FormSpecs.DEFAULT_ROWSPEC,
						FormSpecs.RELATED_GAP_ROWSPEC,
						FormSpecs.DEFAULT_ROWSPEC,
						FormSpecs.RELATED_GAP_ROWSPEC,
						RowSpec.decode("default:grow"), }));
		centerPanel.add(lblDataToSynchronize, "2, 2, 5, 1");
		usersCheckBox.setSelected(true);
		centerPanel.add(usersCheckBox, "2, 4, 5, 1");
		centerPanel.add(conceptsCheckBox, "8, 4, 7, 1");
		locationsCheckBox.setSelected(true);
		centerPanel.add(locationsCheckBox, "2, 6, 5, 1");
		centerPanel.add(otherMetadataCheckBox, "8, 6, 7, 1");
		centerPanel.add(lblSynchronizationOption, "2, 8, 5, 1");
		centerPanel.add(synchronizeButton, "8, 10, 7, 1");
		ComboBoxModel<String> comboBoxModel = new DefaultComboBoxModel<String>(
				new String[] { "Every 6 hours", "Twice a day", "Daily" });
		syncOptionComboBox.setModel(comboBoxModel);
		centerPanel.add(syncOptionComboBox, "2, 10, 5, 1, fill, default");
		centerPanel.add(lblProgress, "2, 12");
		progressBar.setStringPainted(true);
		centerPanel.add(progressBar, "4, 12, 3, 1");
		logTextPane.setEditable(false);
		centerPanel.add(logTextPane, "2, 14, 13, 1, fill, fill");
		synchronizeButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
				JToggleButton button = (JToggleButton) event.getSource();
				if (button.isSelected()) {
					setMode(SyncStatus.SYNCHRONIZING);
					if (validate()) {
						try {
							synchronize();
						} catch (SchedulerException e) {
							log(e.getMessage(), Level.SEVERE);
						}
					} else {
						synchronizeButton.setSelected(false);
						setMode(SyncStatus.STOPPED);
					}
				} else {
					try {
						setMode(SyncStatus.STOPPED);
						scheduler.shutdown();
					} catch (SchedulerException e) {
						e.printStackTrace();
					}
				}
			}
		});
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
	 * Set synchronization mode and enable/disable fields
	 * 
	 * @param syncStatus
	 */
	private void setMode(SyncStatus syncStatus) {
		JComponent[] fields = { serverUrlTextField, serverUsernameTextField,
				serverPasswordField, localUrlTextField, localUsernameTextField,
				localPasswordField, usersCheckBox, locationsCheckBox,
				conceptsCheckBox, otherMetadataCheckBox, syncOptionComboBox };
		switch (syncStatus) {
		case STOPPED:
			for (JComponent field : fields) {
				field.setEnabled(true);
			}
			break;
		case DISCONNECTED:
		case SYNCHRONIZING:
		case WAITING:
			for (JComponent field : fields) {
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
	private boolean validate() {
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
			error.append("At least one option must be checked to synchronize.\n");
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
	 * Starts synchronization schedule
	 * 
	 * @throws SchedulerException
	 */
	private void synchronize() throws SchedulerException {
		int interval = -1;
		switch (syncOptionComboBox.getSelectedIndex()) {
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
		JobDetail job = JobBuilder.newJob(SyncJob.class)
				.withIdentity("syncJob", "syncGroup").build();
		SyncJob syncJobObj = new SyncJob();
		syncJobObj.setRemoteDb(serverDb);
		syncJobObj.setLocalDb(localDb);
		syncJobObj.setSyncUsers(usersCheckBox.isSelected());
		syncJobObj.setSyncLocations(locationsCheckBox.isSelected());
		syncJobObj.setSyncConcepts(conceptsCheckBox.isSelected());
		syncJobObj.setSyncOtherMetadata(otherMetadataCheckBox.isSelected());
		job.getJobDataMap().put("syncJob", syncJobObj);
		SimpleScheduleBuilder scheduleBuilder = SimpleScheduleBuilder
				.simpleSchedule().withIntervalInHours(interval);
		Trigger trigger = TriggerBuilder.newTrigger()
				.withIdentity("syncTrigger", "syncGroup")
				.withSchedule(scheduleBuilder).build();
		scheduler.scheduleJob(job, trigger);
	}
}
