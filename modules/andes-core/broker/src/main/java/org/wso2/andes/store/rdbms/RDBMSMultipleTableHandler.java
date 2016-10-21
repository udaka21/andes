/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.store.rdbms;

import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;

/**
 * JDBC storage related prepared statements, table names, column names and tasks
 * that changes according to the number of tables that are grouped.
 */

public class RDBMSMultipleTableHandler {

    private static final String CONTENT_TABLE = "$MB_CONTENT";
    private static final String MESSAGE_ID = "MESSAGE_ID";
    private static final String MSG_OFFSET = "CONTENT_OFFSET";
    private static final String MESSAGE_CONTENT = "MESSAGE_CONTENT";

    private static final String METADATA_TABLE = "$MB_METADATA";
    private static final String QUEUE_ID = "QUEUE_ID";
    private static final String DLC_QUEUE_ID = "DLC_QUEUE_ID";
    private static final String METADATA = "MESSAGE_METADATA";
    private static final String PS_ALIAS_FOR_COUNT = "count";
    protected static final String QUEUE_NAME = RDBMSConstants.QUEUE_NAME;
    private static final String QUEUES_TABLE = "MB_QUEUE_MAPPING";

    private static final String ALIAS_FOR_QUEUES = "QUEUE_COUNT";

    // Get number of tables from the configurations. Default value is 5.
    private Integer numberOfTables = AndesConfigurationManager.readValue(
            AndesConfiguration.PERFORMANCE_TUNING_NUMBER_OF_TABLES);

    private String[] psInsertMessagePart = new String[numberOfTables];
    private String[] psInsertMetadata = new String[numberOfTables];
    private String[] psRetrieveMessagePart = new String[numberOfTables];
    private String[] psSelectQueueMessageCount = new String[numberOfTables];
    private String[] psSelectRangedQueueMessageCount = new String[numberOfTables];
    private String[] psSelectAllQueueMessageCount = new String[numberOfTables];
    private String[] psSelectQueueMessageCountFromDlc = new String[numberOfTables];
    private String[] psSelectMessageCountInDlc = new String[numberOfTables];
    private String[] psSelectMetadata = new String[numberOfTables];
    private String[] psSelectMetadataRangeFromQueue = new String[numberOfTables];
    private String[] psSelectMetadataRangeFromQueueInDlc = new String[numberOfTables];
    private String[] psSelectMetadataFromQueue = new String[numberOfTables];
    private String[] psSelectMessageIdsFromQueue = new String[numberOfTables];
    private String[] psSelectMetadataInDlcForQueue = new String[numberOfTables];
    private String[] psSelectMetadataInDlc = new String[numberOfTables];
    private String[] psSelectMessageIdsFromMetadataForQueue = new String[numberOfTables];
    private String[] psDeleteMetadataFromQueue = new String[numberOfTables];
    private String[] psDeleteMetadataInDlc = new String[numberOfTables];
    private String[] psDeleteMetadata = new String[numberOfTables];
    private String[] psClearQueueFromMetadata = new String[numberOfTables];
    private String[] psClearDlcQueue = new String[numberOfTables];
    private String[] psUpdateMetadataQueue = new String[numberOfTables];
    private String[] psUpdateMetadata = new String[numberOfTables];
    private String[] psMoveMetadataToDlc = new String[numberOfTables];


    /**
     * Constructor that initialize all the prepared statements, that will change according to the number of tables.
     */
    public RDBMSMultipleTableHandler() {

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
                    + " FROM " + METADATA_TABLE + i
                    + " WHERE " + DLC_QUEUE_ID + "=-1"
                    + " GROUP BY " + QUEUE_ID + " ) " + ALIAS_FOR_QUEUES
                    + " ON " + QUEUES_TABLE + "." + QUEUE_ID + "=" + ALIAS_FOR_QUEUES + "." + QUEUE_ID);

            psSelectQueueMessageCountFromDlc[i] = ("SELECT COUNT(" + MESSAGE_ID + ")"
                    + " AS " + PS_ALIAS_FOR_COUNT
                    + " FROM " + METADATA_TABLE + i
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=?");

            psSelectMessageCountInDlc[i] = ("SELECT COUNT(" + MESSAGE_ID + ")"
                    + " AS " + PS_ALIAS_FOR_COUNT
                    + " FROM " + METADATA_TABLE + i
                    + " WHERE " + DLC_QUEUE_ID + "=?");

            psSelectMetadata[i] = ("SELECT " + METADATA
                    + " FROM " + METADATA_TABLE + i
                    + " WHERE " + MESSAGE_ID + "=?");

            psSelectMetadataRangeFromQueue[i] = ("SELECT " + MESSAGE_ID + "," + METADATA
                    + " FROM " + METADATA_TABLE + i
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=-1"
                    + " AND " + MESSAGE_ID + " BETWEEN ? AND ?"
                    + " ORDER BY " + MESSAGE_ID);

            psSelectMetadataRangeFromQueueInDlc[i] = ("SELECT " + MESSAGE_ID + "," + METADATA
                    + " FROM " + METADATA_TABLE + i
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=?"
                    + " AND " + MESSAGE_ID + " BETWEEN ? AND ?"
                    + " ORDER BY " + MESSAGE_ID);

            psSelectMetadataFromQueue[i] = ("SELECT " + MESSAGE_ID + "," + METADATA
                    + " FROM " + METADATA_TABLE + i
                    + " WHERE " + MESSAGE_ID + ">?"
                    + " AND " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=-1"
                    + " ORDER BY " + MESSAGE_ID);

            psSelectMessageIdsFromQueue[i] = ("SELECT " + MESSAGE_ID
                    + " FROM " + METADATA_TABLE + i
                    + " WHERE " + MESSAGE_ID + ">?"
                    + " AND " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=-1"
                    + " ORDER BY " + MESSAGE_ID);

            psSelectMetadataInDlcForQueue[i] = ("SELECT " + MESSAGE_ID + "," + METADATA
                    + " FROM " + METADATA_TABLE + i
                    + " WHERE " + MESSAGE_ID + ">?"
                    + " AND " + QUEUE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "=?"
                    + " ORDER BY " + MESSAGE_ID);

            psSelectMetadataInDlc[i] = ("SELECT " + MESSAGE_ID + "," + METADATA
                    + " FROM " + METADATA_TABLE + i
                    + " WHERE " + MESSAGE_ID + ">?"
                    + " AND " + DLC_QUEUE_ID + "=?"
                    + " ORDER BY " + MESSAGE_ID);

            psSelectMessageIdsFromMetadataForQueue[i] = ("SELECT " + MESSAGE_ID
                    + " FROM " + METADATA_TABLE + i
                    + " WHERE " + QUEUE_ID + "=?"
                    + " ORDER BY " + MESSAGE_ID);

            psDeleteMetadataFromQueue[i] = ("DELETE  FROM " + METADATA_TABLE + i
                    + " WHERE " + QUEUE_ID + "=?"
                    + " AND " + MESSAGE_ID + "=?");

            psDeleteMetadataInDlc[i] = ("DELETE  FROM " + METADATA_TABLE + i
                    + " WHERE " + MESSAGE_ID + "=?"
                    + " AND " + DLC_QUEUE_ID + "!= -1");

            psDeleteMetadata[i] = ("DELETE  FROM " + METADATA_TABLE + i
                    + " WHERE " + MESSAGE_ID + "=?");

            psClearQueueFromMetadata[i] = ("DELETE  FROM " + METADATA_TABLE + i
                    + " WHERE " + QUEUE_ID + "=?");

            psClearDlcQueue[i] = ("DELETE  FROM " + METADATA_TABLE + i
                    + " WHERE " + DLC_QUEUE_ID + "=?");

            psUpdateMetadataQueue[i] = ("UPDATE " + METADATA_TABLE + i
                    + " SET " + QUEUE_ID + " = ?"
                    + " WHERE " + MESSAGE_ID + " = ?"
                    + " AND " + QUEUE_ID + " = ?");

            psUpdateMetadata[i] = ("UPDATE " + METADATA_TABLE + i
                    + " SET " + QUEUE_ID + " = ?," + METADATA + " = ?"
                    + " WHERE " + MESSAGE_ID + " = ?"
                    + " AND " + QUEUE_ID + " = ?");

            psMoveMetadataToDlc[i] = ("UPDATE " + METADATA_TABLE + i
                    + " SET " + DLC_QUEUE_ID + "=?"
                    + " WHERE " + MESSAGE_ID + "=?");

        }
    }

    /**
     * get tableID according to the queueID.
     * eg: By default number of tables = 5. If queueID= 1, TableID (for that queue) = 1 % 5 = 1
     * TableIDs are given according to the Round-robin manner.
     *
     * @param queueID that pass.
     * @return tableID relevant to the queueID given.
     */
    private int queueIDtoTableID (int queueID) {
        return (queueID % numberOfTables);
    }

    //Get the String query arrays according to the table id given.
    public String getPsInsertMessagePart(int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psInsertMetadata[tableID];
    }

    public String getPsRetrieveMessagePart(int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psRetrieveMessagePart[tableID];
    }

    public String getPsInsertMetadata(int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psInsertMetadata[tableID];
    }

    public String getPsSelectQueueMessageCount(int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psSelectQueueMessageCount[tableID];
    }

    public String getPsSelectRangedQueueMessageCount(int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psSelectRangedQueueMessageCount[tableID];
    }

    public String getPsSelectAllQueueMessageCount (int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psSelectAllQueueMessageCount[tableID];
    }

    public String getPsSelectQueueMessageCountFromDlc (int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psSelectQueueMessageCountFromDlc[tableID];
    }

    public String getPsSelectMessageCountInDlc (int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psSelectMessageCountInDlc[tableID];
    }

    public String getPsSelectMetadata (int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psSelectMetadata[tableID];
    }

    public String getPsSelectMetadataRangeFromQueue (int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psSelectMetadataRangeFromQueue[tableID];
    }

    public String getPsSelectMetadataRangeFromQueueInDlc (int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psSelectMetadataRangeFromQueueInDlc[tableID];
    }

    public String getPsSelectMetadataFromQueue (int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psSelectMetadataFromQueue[tableID];
    }

    public String getPsSelectMessageIdsFromQueue (int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psSelectMessageIdsFromQueue[tableID];
    }

    public String getPsSelectMetadataInDlcForQueue (int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psSelectMetadataInDlcForQueue[tableID];
    }

    public String getPsSelectMetadataInDlc (int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psSelectMetadataInDlc[tableID];
    }

    public String getPsSelectMessageIdsFromMetadataForQueue (int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psSelectMessageIdsFromMetadataForQueue[tableID];
    }

    public String getPsDeleteMetadataFromQueue (int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psDeleteMetadataFromQueue[tableID];
    }

    public String getPsDeleteMetadataInDlc (int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psDeleteMetadataInDlc[tableID];
    }

    public String getPsDeleteMetadata (int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psDeleteMetadata[tableID];
    }

    public String getPsClearQueueFromMetadata (int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psClearQueueFromMetadata[tableID];
    }

    public String getPsClearDlcQueue (int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psClearDlcQueue[tableID];
    }

    public String getPsUpdateMetadataQueue(int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psUpdateMetadataQueue[tableID];
    }

    public String getPsUpdateMetadata (int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psUpdateMetadata[tableID];
    }

    public String getPsMoveMetadataToDlc (int queueID) {
        int tableID = queueIDtoTableID(queueID);
        return psMoveMetadataToDlc[tableID];
    }
}
