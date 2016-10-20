package org.wso2.andes.store.rdbms;

import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;

/**
 * JDBC storage related prepared statements, table names, column names and tasks
 * that changes according to the number of tables are grouped.
 */

public class RDBMSMultipleTableHandler {

    // Configuration properties
    //protected static final String PROP_JNDI_LOOKUP_NAME = "dataSource";

    protected static final String CONTENT_TABLE = "$MB_CONTENT";
    protected static final String MESSAGE_ID = "MESSAGE_ID";
    protected static final String MSG_OFFSET = "CONTENT_OFFSET";
    protected static final String MESSAGE_CONTENT = "MESSAGE_CONTENT";

    protected static final String METADATA_TABLE = "$MB_METADATA";
    protected static final String QUEUE_ID = "QUEUE_ID";
    protected static final String DLC_QUEUE_ID = "DLC_QUEUE_ID";
    protected static final String METADATA = "MESSAGE_METADATA";
    protected static final String PS_ALIAS_FOR_COUNT = "count";
    protected static final String QUEUE_NAME = "QUEUE_NAME";
    protected static final String QUEUES_TABLE = "MB_QUEUE_MAPPING";

    protected static final String ALIAS_FOR_QUEUES = "QUEUE_COUNT";


    /**
     * Constructor that initialize all the prepared statements, that will change according to the number of tables.
     */
    public RDBMSMultipleTableHandler() {

        // Get number of tables from the configurations. Default value is 5.
        Integer numberOfTables = AndesConfigurationManager.readValue(
                AndesConfiguration.PERFORMANCE_TUNING_NUMBER_OF_TABLES);

        String[] psInsertMessagePart = new String[numberOfTables];
        String[] psInsertMetadata = new String[numberOfTables];
        String[] psRetrieveMessagePart = new String[numberOfTables];
        String[] psSelectQueueMessageCount = new String[numberOfTables];
        String[] psSelectRangedQueueMessageCount = new String[numberOfTables];
        String[] psSelectAllQueueMessageCount = new String[numberOfTables];
        String[] psSelectQueueMessageCountFromDlc = new String[numberOfTables];
        String[] psSelectMessageCountInDlc = new String[numberOfTables];
        String[] psSelectMetadata = new String[numberOfTables];
        String[] psSelectMetadataRangeFromQueue = new String[numberOfTables];
        String[] psSelectMetadataRangeFromQueueInDlc = new String[numberOfTables];
        String[] psSelectMetadataFromQueue = new String[numberOfTables];
        String[] psSelectMessageIdsFromQueue = new String[numberOfTables];
        String[] psSelectMetadataInDlcForQueue = new String[numberOfTables];
        String[] psSelectMetadataInDlc = new String[numberOfTables];
        String[] psSelectMessageIdsFromMetadataForQueue = new String[numberOfTables];
        String[] psDeleteMetadataFromQueue = new String[numberOfTables];
        String[] psDeleteMetadataInDlc = new String[numberOfTables];
        String[] psDeleteMetadata = new String[numberOfTables];
        String[] psClearQueueFromMetadata = new String[numberOfTables];
        String[] psClearDlcQueue = new String[numberOfTables];
        String[] psUpdateMetadataQueue = new String[numberOfTables];
        String[] psUpdateMetadata = new String[numberOfTables];
        String[] psMoveMetadataToDlc = new String[numberOfTables];


        for (int i = 0; i < numberOfTables; i++) {

            psInsertMessagePart[i] = ("INSERT INTO"
                    + CONTENT_TABLE + i + " ( " + MESSAGE_ID + " ,"
                    + MSG_OFFSET + " ," + MESSAGE_CONTENT + ")" + " VALUES (?, ?, ?)");

            psRetrieveMessagePart[i] = ("SELECT " + MESSAGE_CONTENT
                    + " FROM " + CONTENT_TABLE + i
                    + " WHERE " + MESSAGE_ID + "=?"
                    + " AND " + MSG_OFFSET + "=?");

            psInsertMetadata[i] = ("INSERT INTO"
                    + METADATA_TABLE + i + " ( " + MESSAGE_ID + " ,"
                    + QUEUE_ID + " ," + DLC_QUEUE_ID + " ," + METADATA + " ) " + "VALUES ( ?,?,-1,? )");

            psSelectQueueMessageCount[i] = ("SELECT COUNT(" + QUEUE_ID + ") AS " + PS_ALIAS_FOR_COUNT
                    + " FROM " + METADATA_TABLE + i
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=-1");

            psSelectRangedQueueMessageCount[i] = ("SELECT COUNT(" + MESSAGE_ID + ") AS " + PS_ALIAS_FOR_COUNT
                    + " FROM " + METADATA_TABLE + i
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + MESSAGE_ID + " BETWEEN ? AND ?"
                    + " AND " + DLC_QUEUE_ID + "=-1");

            psSelectAllQueueMessageCount[i] = ("SELECT " + QUEUE_NAME + ", " + PS_ALIAS_FOR_COUNT
                    + " FROM " + QUEUES_TABLE + " LEFT OUTER JOIN "
                    + "(SELECT " + QUEUE_ID + ", COUNT(" + QUEUE_ID + ") AS " + PS_ALIAS_FOR_COUNT
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + DLC_QUEUE_ID + "=-1"
                    + " GROUP BY " + QUEUE_ID + " ) " + ALIAS_FOR_QUEUES
                    + " ON " + QUEUES_TABLE + "." + QUEUE_ID + "=" + ALIAS_FOR_QUEUES + "." + QUEUE_ID );

            psSelectQueueMessageCountFromDlc[i] = ("SELECT COUNT(" + MESSAGE_ID + ")"
                    + " AS " + PS_ALIAS_FOR_COUNT
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=?");

            psSelectMessageCountInDlc[i] = ("SELECT COUNT(" + MESSAGE_ID + ")"
                    + " AS " + PS_ALIAS_FOR_COUNT
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + DLC_QUEUE_ID + "=?");

            psSelectMetadata[i] = ("SELECT " + METADATA
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + MESSAGE_ID + "=?");

            psSelectMetadataRangeFromQueue[i] = ("SELECT " + MESSAGE_ID + "," + METADATA
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=-1"
                    + " AND " + MESSAGE_ID + " BETWEEN ? AND ?"
                    + " ORDER BY " + MESSAGE_ID);

            psSelectMetadataRangeFromQueueInDlc[i] = ("SELECT " + MESSAGE_ID + "," + METADATA
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=?"
                    + " AND " + MESSAGE_ID + " BETWEEN ? AND ?"
                    + " ORDER BY " + MESSAGE_ID);

            psSelectMetadataFromQueue[i] = ("SELECT " + MESSAGE_ID + "," + METADATA
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + MESSAGE_ID + ">?"
                    + " AND " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=-1"
                    + " ORDER BY " + MESSAGE_ID);

            psSelectMessageIdsFromQueue[i] = ("SELECT " + MESSAGE_ID
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + MESSAGE_ID + ">?"
                    + " AND " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=-1"
                    + " ORDER BY " + MESSAGE_ID);

            psSelectMetadataInDlcForQueue[i] = ("SELECT " + MESSAGE_ID + "," + METADATA
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + MESSAGE_ID + ">?"
                    + " AND " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=?"
                    + " ORDER BY " + MESSAGE_ID);

            psSelectMetadataInDlc[i] = ("SELECT " + MESSAGE_ID + "," + METADATA
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + MESSAGE_ID + ">?"
                    + " AND " + DLC_QUEUE_ID + "=?"
                    + " ORDER BY " + MESSAGE_ID);

            psSelectMessageIdsFromMetadataForQueue[i] = ("SELECT " + MESSAGE_ID
                    + " FROM " + METADATA_TABLE
                    + " WHERE " + QUEUE_ID + "=?"
                    + " ORDER BY " + MESSAGE_ID);

            psDeleteMetadataFromQueue[i] = ("DELETE  FROM " + METADATA_TABLE
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + MESSAGE_ID + "=?");

            psDeleteMetadataInDlc[i] = ("DELETE  FROM " + METADATA_TABLE
                    + " WHERE " + MESSAGE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "!= -1");

            psDeleteMetadata[i] = ("DELETE  FROM " + METADATA_TABLE
                    + " WHERE " + MESSAGE_ID + "=?");

            psClearQueueFromMetadata[i] = ("DELETE  FROM " + METADATA_TABLE
                    + " WHERE " + QUEUE_ID + "=?");

            psClearDlcQueue[i] = ("DELETE  FROM " + METADATA_TABLE
                    + " WHERE " + DLC_QUEUE_ID + "=?");

            psUpdateMetadataQueue[i] = ("UPDATE " + METADATA_TABLE
                    + " SET " + QUEUE_ID + " = ?"
                    + " WHERE " + MESSAGE_ID + " = ?"
                    + " AND " + QUEUE_ID + " = ?");

            psUpdateMetadata[i] = ("UPDATE " + METADATA_TABLE
                    + " SET " + QUEUE_ID + " = ?," + METADATA + " = ?"
                    + " WHERE " + MESSAGE_ID + " = ?"
                    + " AND " + QUEUE_ID + " = ?");

            psMoveMetadataToDlc[i] = ("UPDATE " + METADATA_TABLE
                    + " SET " + DLC_QUEUE_ID + "=?"
                    + " WHERE " + MESSAGE_ID + "=?");

        }
    }
}
