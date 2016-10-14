/**
 * 
 */
package com.ihsinformatics.gfatmsync;

import java.awt.BorderLayout;
import java.awt.EventQueue;
import java.awt.Toolkit;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

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
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JToggleButton;
import javax.swing.UIManager;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import com.ihsinformatics.util.RegexUtil;
import com.jgoodies.forms.layout.ColumnSpec;
import com.jgoodies.forms.layout.FormLayout;
import com.jgoodies.forms.layout.FormSpecs;
import com.jgoodies.forms.layout.RowSpec;

/**
 * @author owais.hussain@ihsinformatics.com
 *
 */
public class GfatmSyncUi {
	private JFrame mainFrame = new JFrame();

	private JLabel lblServerAddress = new JLabel("Server Address:");
	private JLabel lblUsername = new JLabel("Username:");
	private JLabel lblPassword = new JLabel("Password:");
	private JLabel lblDataToSynchronize = new JLabel("Data to Synchronize:");
	private JLabel lblSynchronizationOption = new JLabel(
			"Synchronization Option:");
	private JLabel lblProgress = new JLabel("Progress:");
	private JLabel lblStatus = new JLabel("Status:");
	private JLabel syncStatusLabel = new JLabel("STOPPED");

	private JTextField serverAddressTextField = new JTextField();
	private JTextField usernameTextField = new JTextField();
	private JPasswordField passwordTextField = new JPasswordField();
	private JTextArea logTextArea = new JTextArea();

	private JComboBox<String> syncOptionComboBox = new JComboBox<String>();

	private JCheckBox usersCheckBox = new JCheckBox("Users and Attributes");
	private JCheckBox locationsCheckBox = new JCheckBox(
			"Locations and Attributes");
	private JCheckBox conceptsCheckBox = new JCheckBox(
			"Concepts and Form Types");
	private JCheckBox otherMetadataCheckBox = new JCheckBox("Other Metadata");

	private JProgressBar progressBar = new JProgressBar();

	private JToggleButton synchronizeButton = new JToggleButton("Synchronize");

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
	 * Create the application.
	 */
	public GfatmSyncUi() {
		initialize();
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
		mainFrame.setBounds(100, 100, 390, 400);
		mainFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

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
				new RowSpec[] { FormSpecs.LINE_GAP_ROWSPEC,
						RowSpec.decode("28px"), FormSpecs.LINE_GAP_ROWSPEC,
						RowSpec.decode("28px"), FormSpecs.RELATED_GAP_ROWSPEC,
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

		centerPanel.add(lblServerAddress, "2, 2, 3, 1, left, center");
		serverAddressTextField.setText("jdbc:mysql://localhost:3306/gfatm_dw");
		centerPanel.add(serverAddressTextField, "6, 2, 9, 1, left, top");
		serverAddressTextField.setColumns(24);
		centerPanel.add(lblUsername, "2, 4, fill, center");
		usernameTextField.setText("gfatm_user");
		centerPanel.add(usernameTextField, "4, 4, 3, 1, left, top");
		usernameTextField.setColumns(8);
		centerPanel.add(lblPassword, "8, 4, right, center");
		centerPanel.add(passwordTextField, "10, 4, 5, 1, fill, default");
		centerPanel.add(lblDataToSynchronize, "2, 6, 5, 1");
		centerPanel.add(usersCheckBox, "2, 8, 5, 1");
		centerPanel.add(conceptsCheckBox, "8, 8, 7, 1");
		centerPanel.add(locationsCheckBox, "2, 10, 5, 1");
		centerPanel.add(otherMetadataCheckBox, "8, 10, 7, 1");
		centerPanel.add(lblSynchronizationOption, "2, 12, 5, 1");
		centerPanel.add(synchronizeButton, "8, 14, 7, 1");
		ComboBoxModel<String> comboBoxModel = new DefaultComboBoxModel<String>(
				new String[] { "Every 6 hours", "Twice a day", "Daily" });
		syncOptionComboBox.setModel(comboBoxModel);
		centerPanel.add(syncOptionComboBox, "2, 14, 5, 1, fill, default");
		centerPanel.add(lblProgress, "2, 16");
		centerPanel.add(progressBar, "4, 16, 3, 1");
		centerPanel.add(lblStatus, "8, 16");
		centerPanel.add(syncStatusLabel, "10, 16, 5, 1");
		logTextArea.setEditable(false);
		centerPanel.add(logTextArea, "2, 18, 13, 1, fill, fill");
		synchronizeButton.addChangeListener(new ChangeListener() {
			public void stateChanged(ChangeEvent event) {
				JToggleButton button = (JToggleButton) event.getSource();
				if (button.isSelected()) {
					setMode(SyncStatus.SYNCHRONIZING);
					if (validate()) {
						try {
							synchronize();
						} catch (SchedulerException e) {
							e.printStackTrace();
						}
					} else {
						synchronizeButton.setSelected(false);
						setMode(SyncStatus.STOPPED);
					}
				}
			}
		});
	}

	private void setMode(SyncStatus syncStatus) {
		JComponent[] fields = { serverAddressTextField, usernameTextField,
				passwordTextField, usersCheckBox, locationsCheckBox,
				conceptsCheckBox, otherMetadataCheckBox };
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
		String server = serverAddressTextField.getText();
		String username = usernameTextField.getText();
		String password = String.valueOf(passwordTextField.getPassword());
		boolean usersChecked = usersCheckBox.isSelected();
		boolean locationsChecked = locationsCheckBox.isSelected();
		boolean conceptsChecked = conceptsCheckBox.isSelected();
		boolean otherMetadataChecked = otherMetadataCheckBox.isSelected();
		// Check mandatory fields
		if (server.equals("")) {
			error.append("Server Address cannot be empty.\n");
		}
		if (username.equals("")) {
			error.append("Username cannot be empty.\n");
		}
		if (password.equals("")) {
			error.append("Password cannot be empty.\n");
		}
		if (usersChecked == locationsChecked == conceptsChecked == otherMetadataChecked == false) {
			error.append("At least one option must be checked to synchronize.\n");
		}
		// Check data types
		if (!RegexUtil.isWord(username)) {
			error.append("Username is invalid.\n");
		}
		// Try connection
		try {
			Connection conn = DriverManager.getConnection(server, username,
					password);
			Class.forName("com.mysql.jdbc.Driver");
			Statement stmt = conn.createStatement();
			String sql = "select vesion()";
			ResultSet rs = stmt.executeQuery(sql);
			Object object = rs.getObject(1);
			if (object == null) {
				error.append("Cannot connect with the Server.\n");
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
		JobDetail job = JobBuilder.newJob(SyncJob.class)
				.withIdentity("syncJob", "group1").build();
		TriggerBuilder<Trigger> triggerBuilder = TriggerBuilder.newTrigger();
		SimpleScheduleBuilder scheduleBuilder = SimpleScheduleBuilder
				.simpleSchedule().withIntervalInHours(interval).repeatForever();
		Trigger trigger = triggerBuilder.withIdentity("syncTrigger", "group1")
				.withSchedule(scheduleBuilder).build();
		Scheduler scheduler = new StdSchedulerFactory().getScheduler();
		scheduler.start();
		scheduler.scheduleJob(job, trigger);
	}
}
