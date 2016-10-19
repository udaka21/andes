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
    protected static final String MESSAGE_ID = "MESSAGE_ID";
    protected static final String MSG_OFFSET = "CONTENT_OFFSET";
    protected static final String MESSAGE_CONTENT = "MESSAGE_CONTENT";

    protected static final String METADATA_TABLE = "$MB_METADATA";
    protected static final String QUEUE_ID = "QUEUE_ID";
    protected static final String DLC_QUEUE_ID = "DLC_QUEUE_ID";
    protected static final String METADATA = "MESSAGE_METADATA";


    public RDBMSVariables() {
        numberOfTables = AndesConfigurationManager.readValue(
                AndesConfiguration.PERFORMANCE_TUNING_NUMBER_OF_TABLES);

    String[] psInsertMessagePart = new String[numberOfTables];
    String[] psInsertMetadata = new String[numberOfTables];

    for (int i = 0 ; i<numberOfTables ; i++ ) {

        psInsertMessagePart[i] = (
                "INSERT INTO"  + CONTENT_TABLE + i +" ( " + MESSAGE_ID +  " ," +
                MSG_OFFSET + " ," + MESSAGE_CONTENT + ")" + " VALUES (?, ?, ?)");

        psInsertMetadata[i] = (
                "INSERT INTO" + METADATA_TABLE + i + " ( " + MESSAGE_ID + " ," +
                QUEUE_ID + " ," + DLC_QUEUE_ID + " ," + METADATA + " ) " + "VALUES ( ?,?,-1,? )");
        }

    }
}
