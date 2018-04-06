=== GFATM Sync ===
Contributors: IHS
Software Type: Free, Open-source
Requires: Microsoft Windows 7 or higher, Oracle Java Runtime Environment (JRE) v7.0 or higher
License: GPLv3

== Description ==
This repository contains projects used for synchronization of data/metadata between several components. There are two projects inside:

1. gfatm-datawarehouse:
This is Java console project used to create/update data warehouse from transactional databases. The gfatm-sync.properties file should contain credentials to connect with the Data warehouse repository, while the target data sources are defined in a table named _implementation.

= Configuration =
1. Import the project in Eclipse/IntelliJ
2. Make sure you have necessary Maven plug-ins (or use command prompt)
3. The project requires utilities project in [developer-resources](https://git.ihsinformatics.com/team-leaders/developer-resources.git) Repositry.

2. gfatm-import:

