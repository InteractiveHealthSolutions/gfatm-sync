# GFATM Sync
Contributors: IHS
Software Type: Free, Open-source
Requires: Microsoft Windows 7 or higher, Oracle Java Runtime Environment (JRE) v7.0 or higher
License: GPLv3

## Description
This repository contains projects used for synchronization of data/metadata between several components. There are two projects inside:

### 1. gfatm-datawarehouse:
This is Java console project used to create/update data warehouse from transactional databases. The gfatm-sync.properties file should contain credentials to connect with the Data warehouse repository, while the target data sources are defined in a table named _implementation.

#### Configuration
1. Import the project in Eclipse/IntelliJ.
2. Make sure you have necessary Maven plug-ins (or use command prompt).
3. The project requires utilities project in [developer-resources](https://git.ihsinformatics.com/team-leaders/developer-resources.git) Repositry.
4. Import the utilities project in IDE; build and update snapshot.
5. If the gfatm-datawarehouse pom file is unable to find the required version, then update the version according to latest utilities project version in pom file.
6. Once all dependencies are resolved, create schema for Data warehouse in your target MySQL server instance (default name is "gfatm_dw").
7. Execute all SQL statements in res/sql directory to your Data warehouse instance. This will create all requried stored procedures.
8. Once complete, open MySQL server workbench (or command) and insert source databases (OpenMRS) in _implementation table from which the data will be imported. It is highly recommended to refrain from using root user. Instead, you should always create a separate user account with read-only access to data in source database.
For example: 
INSERT INTO _implementation VALUES (1,'jdbc:mysql://localhost:3306/openmrs?autoReconnect=true&useSSL=false','com.mysql.jdbc.Driver','openmrs','read_only_user','password',1,'2017-05-01 00:00:00','STOPPED',NULL,'OpenMRS deployment on local machine');
9. Once done, run the application (DataWarehouseMain.java file) with required arguments (read argument details from main() function)

### 2. gfatm-import:
This is Java swing project used to synchronize metadata between two OpenMRS instances. Once running, the app keeps repeating the import process based on the Import option selected.
This project will not import any Patient-level data.

#### Configuration
1. Import the project in Eclipse/IntelliJ.
2. Make sure you have necessary Maven plug-ins (or use command prompt).
3. The project requires utilities project in [developer-resources](https://git.ihsinformatics.com/team-leaders/developer-resources.git) Repositry.
4. Import the utilities project in IDE; build and update snapshot.
5. If the gfatm-datawarehouse pom file is unable to find the required version, then update the version according to latest utilities project version in pom file.
6. Once all dependencies are resolved, run the application from GfatmImportMain class
7. Source connection is used to define from where the metadata will be imported
8. Target connection is used to define to where the metadata will be exported
