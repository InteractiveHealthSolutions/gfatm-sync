/**
 * 
 */
package com.ihsinformatics.gfatmsync;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.EventQueue;
import java.awt.Toolkit;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
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
import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.JToggleButton;
import javax.swing.UIManager;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.text.AttributeSet;
import javax.swing.text.SimpleAttributeSet;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyleContext;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import com.ihsinformatics.util.DateTimeUtil;
import com.ihsinformatics.util.RegexUtil;
import com.ihsinformatics.util.VersionUtil;
import com.jgoodies.forms.layout.ColumnSpec;
import com.jgoodies.forms.layout.FormLayout;
import com.jgoodies.forms.layout.FormSpecs;
import com.jgoodies.forms.layout.RowSpec;
import javax.swing.JTabbedPane;

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
	private final JLabel lblServerUsername = new JLabel("Username:");
	private final JLabel lblServerPassword = new JLabel("Password:");
	private final JLabel lblLocalAddress = new JLabel("Local Address:");
	private final JLabel lblLocalUsername = new JLabel("Username:");
	private final JLabel lblLocalPassword = new JLabel("Password:");
	private final JLabel lblDataToSynchronize = new JLabel(
			"Data to Synchronize:");
	private final JLabel lblSynchronizationOption = new JLabel(
			"Synchronization Option:");
	private final JLabel lblProgress = new JLabel("Progress:");
	private final JLabel lblStatus = new JLabel("Status:");
	private final JLabel syncStatusLabel = new JLabel("STOPPED");

	private final JTextField serverAddressTextField = new JTextField();
	private final JTextField serverUsernameTextField = new JTextField();
	private final JPasswordField serverPasswordField = new JPasswordField();
	private final JTextField localAddressTextField = new JTextField();
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

	private static SyncStatus currentStatus = SyncStatus.STOPPED;

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

			Color colour = Color.BLUE;
			if (level.getName().equalsIgnoreCase(Level.WARNING.getName())) {
				colour = Color.ORANGE;
			} else if (level.getName().equalsIgnoreCase(Level.SEVERE.getName())) {
				colour = Color.RED;
			} else if (level.getName().equalsIgnoreCase(Level.FINE.getName())) {
				colour = Color.RED;
			}
			StyleContext sc = StyleContext.getDefaultStyleContext();
			AttributeSet aset = sc.addAttribute(SimpleAttributeSet.EMPTY,
					StyleConstants.Foreground, colour);
			aset = sc.addAttribute(aset, StyleConstants.FontFamily,
					"Lucida Console");
			aset = sc.addAttribute(aset, StyleConstants.Alignment,
					StyleConstants.ALIGN_JUSTIFIED);
			int len = logTextPane.getDocument().getLength();
			logTextPane.setCaretPosition(len);
			logTextPane.setCharacterAttributes(aset, false);
			logTextPane.replaceSelection(DateTimeUtil
					.getSqlDateTime(new Date()) + ":\t" + message);
		}
	}

	public static void resetProgressBar(int min, int max) {
		progressBar.setMinimum(min);
		progressBar.setMaximum(max);
		progressBar.updateUI();
	}

	public static void updateProgress() {
		progressBar.setValue(progressBar.getValue() + 1);
		progressBar.updateUI();
	}

	/**
	 * Create the application.
	 */
	public GfatmSyncUi() {
		initialize();
		readProperties(propertiesFile);
		log("Application initialized. Version " + version.toString(), Level.INFO);
	}

	/**
	 * Initialize the contents of the mainFrame.
	 */
	private void initialize() {
		mainFrame.setResizable(false);
		mainFrame
				.setIconImage(Toolkit
						.getDefaultToolkit()
						.getImage(
								GfatmSyncUi.class
										.getResource("/com/sun/java/swing/plaf/windows/icons/Computer.gif")));
		mainFrame.setBounds(100, 100, 400, 435);
		mainFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		mainFrame.getContentPane().add(tabbedPane, BorderLayout.NORTH);

		tabbedPane.addTab("Server Connection", null, serverPanel, null);
		serverPanel.setLayout(new FormLayout(new ColumnSpec[] {
				FormSpecs.RELATED_GAP_COLSPEC, ColumnSpec.decode("87px"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("30px"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("65px"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("59px"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("110px"), }, new RowSpec[] {
				FormSpecs.LINE_GAP_ROWSPEC, RowSpec.decode("28px"),
				FormSpecs.LINE_GAP_ROWSPEC, RowSpec.decode("28px"), }));
		serverPanel.add(lblServerAddress, "2, 2, left, center");
		serverPanel.add(serverAddressTextField, "4, 2, 7, 1, fill, top");
		serverAddressTextField
				.setText("jdbc:mysql://202.141.249.106:6848/openmrs");
		serverAddressTextField.setColumns(24);
		serverPanel.add(lblServerUsername, "2, 4, left, center");
		serverPanel.add(serverUsernameTextField, "4, 4, 3, 1, left, top");
		serverUsernameTextField.setText("gfatm_user");
		serverUsernameTextField.setColumns(8);
		serverPanel.add(lblServerPassword, "8, 4, left, center");
		serverPanel.add(serverPasswordField, "10, 4, fill, top");

		tabbedPane.addTab("Local Connection", null, clientPanel, null);
		clientPanel.setLayout(new FormLayout(new ColumnSpec[] {
				FormSpecs.RELATED_GAP_COLSPEC, ColumnSpec.decode("87px"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("30px"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("72px"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("59px"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("108px"), }, new RowSpec[] {
				FormSpecs.LINE_GAP_ROWSPEC, RowSpec.decode("28px"),
				FormSpecs.LINE_GAP_ROWSPEC, RowSpec.decode("28px"), }));
		clientPanel.add(lblLocalAddress, "2, 2, left, center");
		localAddressTextField.setText("jdbc:mysql://localhost:3306/openmrs");
		localAddressTextField.setColumns(24);
		clientPanel.add(localAddressTextField, "4, 2, 7, 1, fill, top");
		clientPanel.add(lblLocalUsername, "2, 4, left, center");
		localUsernameTextField.setText("openmrs_user");
		localUsernameTextField.setColumns(8);
		clientPanel.add(localUsernameTextField, "4, 4, 3, 1, left, top");
		clientPanel.add(lblLocalPassword, "8, 4, left, center");
		clientPanel.add(localPasswordField, "10, 4, fill, top");
		JPanel centerPanel = new JPanel();
		mainFrame.getContentPane().add(centerPanel, BorderLayout.CENTER);
		centerPanel.setLayout(new FormLayout(new ColumnSpec[] {
				FormSpecs.RELATED_GAP_COLSPEC, ColumnSpec.decode("63px:grow"),
				FormSpecs.LABEL_COMPONENT_GAP_COLSPEC,
				ColumnSpec.decode("19px"),
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
		centerPanel.add(lblStatus, "8, 12");
		centerPanel.add(syncStatusLabel, "10, 12, 5, 1");
		logTextPane.setEditable(false);
		centerPanel.add(logTextPane, "2, 14, 13, 1, fill, fill");
		synchronizeButton.addChangeListener(new ChangeListener() {
			public void stateChanged(ChangeEvent event) {
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
				}
			}
		});
	}

	/**
	 * Read properties from properties file
	 */
	public void readProperties(String propertiesFile) {
		try {
			InputStream propFile = new FileInputStream(propertiesFile);
			if (propFile != null) {
				props = new Properties();
				props.load(propFile);
				String versionStr = props.getProperty("app.version");
				version.parseVersion(versionStr);
			}
		} catch (IOException e) {
			e.printStackTrace();
			log("Properties file not found or is inaccessible.", Level.SEVERE);
		} catch (NumberFormatException e) {
			e.printStackTrace();
			log("Invalid version in properties file.", Level.WARNING);
		} catch (NullPointerException e) {
			e.printStackTrace();
			log("Version not found in properties file.", Level.WARNING);
		} catch (ParseException e) {
			e.printStackTrace();
			log("Bad version name found in properties file.", Level.WARNING);
		}
	}

	/**
	 * Set synchronization mode
	 * 
	 * @param syncStatus
	 */
	private void setMode(SyncStatus syncStatus) {
		JComponent[] fields = { serverAddressTextField,
				serverUsernameTextField, serverPasswordField, usersCheckBox,
				locationsCheckBox, conceptsCheckBox, otherMetadataCheckBox };
		currentStatus = syncStatus;
		syncStatusLabel.setText(currentStatus.toString());
		switch (currentStatus) {
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

	private boolean validate() {
		boolean valid = true;
		StringBuilder error = new StringBuilder();
		String serverUrl = serverAddressTextField.getText();
		String serverUsername = serverUsernameTextField.getText();
		String serverPassword = String.valueOf(serverPasswordField
				.getPassword());
		String localUrl = localAddressTextField.getText();
		String localUsername = localUsernameTextField.getText();
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
			Connection conn = DriverManager.getConnection(serverUrl,
					serverUsername, serverPassword);
			Class.forName("com.mysql.jdbc.Driver");
			Statement stmt = conn.createStatement();
			String sql = "select vesion()";
			ResultSet rs = stmt.executeQuery(sql);
			Object object = rs.getObject(1);
			if (object == null) {
				error.append("Cannot connect with the Server. Please check server connection URL and credentials.\n");
			}
			conn = DriverManager.getConnection(localUrl, localUsername,
					localPassword);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			object = rs.getObject(1);
			if (object == null) {
				error.append("Cannot connect with the Local database. Please check local connection URL and credentials.\n");
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
		Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
		scheduler.start();
		JobDetail job = JobBuilder.newJob(SyncJob.class)
				.withIdentity("syncJob", "syncGroup").build();
		SimpleScheduleBuilder scheduleBuilder = SimpleScheduleBuilder
				.simpleSchedule().withIntervalInHours(interval);
		Trigger trigger = TriggerBuilder.newTrigger()
				.withIdentity("syncTrigger", "syncGroup")
				.withSchedule(scheduleBuilder).build();
		scheduler.scheduleJob(job, trigger);
	}
}
