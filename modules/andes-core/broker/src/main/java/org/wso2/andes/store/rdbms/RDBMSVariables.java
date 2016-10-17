package org.wso2.andes.store.rdbms;

import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;

/**
 * JDBC storage related prepared statements, table names, column names and tasks
 * that changes according to the number of tables are grouped, in this class.
 */

public class RDBMSVariables {

    // Configuration properties
    protected static final String PROP_JNDI_LOOKUP_NAME = "dataSource";

    // Get number of tables from the configurations. Default value is 5.
    private int  numberOfTables;

    protected static final String CONTENT_TABLE = "$MB_CONTENT";
    protected static final String METADATA_TABLE = "$MB_METADATA";


    public RDBMSVariables() {
        numberOfTables = AndesConfigurationManager.readValue(
                AndesConfiguration.PERFORMANCE_TUNING_NUMBER_OF_TABLES);

    String[] psInsertMessagePart = new String[numberOfTables];
    String[] psInsertMetadata = new String[numberOfTables];

    for (int i = 0 ; i<numberOfTables ; i++ ) {

        psInsertMessagePart[i] = ("protected static final String PS_INSERT_MESSAGE_PART"+ i + " =\n" +
                "             \"INSERT INTO \" + CONTENT_TABLE"+ i +" + \"(\"\n" +
                "                     + MESSAGE_ID + \",\"\n" +
                "                     + MSG_OFFSET + \",\"\n" +
                "                     + MESSAGE_CONTENT + \") VALUES (?, ?, ?)\"");

        psInsertMetadata[i] = "protected static final String PS_INSERT_METADATA"+ i +" =\n" +
                " +            \"INSERT INTO \" + METADATA_TABLE2 + \" (\"\n" +
                " +                    + MESSAGE_ID + \",\"\n" +
                " +                    + QUEUE_ID + \",\"\n" +
                " +                    + DLC_QUEUE_ID + \",\"\n" +
                " +                    + METADATA + \")\"\n" +
                " +                    + \" VALUES ( ?,?,-1,? )\"";
    }

    }
}
