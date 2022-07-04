package io.tapdata.connector.postgres;

import io.tapdata.common.WriteRecorder;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Collectors;

public class PostgresWriteRecorder extends WriteRecorder {

    public PostgresWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

    public PostgresWriteRecorder(Connection connection, TapTable tapTable, String schema, boolean hasUnique) {
        super(connection, tapTable, schema);
        uniqueConditionIsIndex = uniqueConditionIsIndex && hasUnique;
    }

    @Override
    public void addInsertBatch(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        if (EmptyKit.isNotEmpty(uniqueCondition)) {
            if (Integer.parseInt(version) > 90500 && uniqueConditionIsIndex) {
                if (insertPolicy.equals("ignore-on-exists")) {
                    conflictIgnoreInsert(after);
                } else {
                    conflictUpdateInsert(after);
                }
            } else {
                if (insertPolicy.equals("ignore-on-exists")) {
                    notExistsInsert(after);
                } else {
                    withUpdateInsert(after);
                }
            }
        } else {
            justInsert(after);
        }
        preparedStatement.addBatch();
    }

//    @Override
//    public void addInsertBatch(Map<String, Object> after) throws SQLException {
//        //after is empty will be skipped
//        if (EmptyKit.isEmpty(after)) {
//            return;
//        }
//        //insert into all columns, make preparedStatement
//        if (EmptyKit.isNull(preparedStatement)) {
//            String insertHead = "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
//                    + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") ";
//            String insertValue = "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ";
//            String insertSql = insertHead + insertValue;
//            if (EmptyKit.isNotEmpty(uniqueCondition)) {
//                if (Integer.parseInt(version) > 90500 && uniqueConditionIsIndex) {
//                    insertSql += "ON CONFLICT("
//                            + uniqueCondition.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", "))
//                            + ") DO UPDATE SET " + allColumn.stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", "));
//                } else {
//                    if (hasPk) {
//                        insertSql = "WITH upsert AS (UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " + allColumn.stream().map(k -> "\"" + k + "\"=?")
//                                .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "\"" + k + "\"=?")
//                                .collect(Collectors.joining(" AND ")) + " RETURNING *) " + insertHead + " SELECT "
//                                + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
//                    } else {
//                        insertSql = "WITH upsert AS (UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " + allColumn.stream().map(k -> "\"" + k + "\"=?")
//                                .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))")
//                                .collect(Collectors.joining(" AND ")) + " RETURNING *) " + insertHead + " SELECT "
//                                + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
//                    }
//                }
//            }
//            preparedStatement = connection.prepareStatement(insertSql);
//        }
//        preparedStatement.clearParameters();
//        //make params
//        int pos = 1;
//        if ((Integer.parseInt(version) <= 90500 || !uniqueConditionIsIndex) && EmptyKit.isNotEmpty(uniqueCondition)) {
//            for (String key : allColumn) {
//                preparedStatement.setObject(pos++, after.get(key));
//            }
//            if (hasPk) {
//                for (String key : uniqueCondition) {
//                    preparedStatement.setObject(pos++, after.get(key));
//                }
//            } else {
//                for (String key : uniqueCondition) {
//                    preparedStatement.setObject(pos++, after.get(key));
//                    preparedStatement.setObject(pos++, after.get(key));
//                }
//            }
//        }
//        for (String key : allColumn) {
//            preparedStatement.setObject(pos++, after.get(key));
//        }
//        if (EmptyKit.isNotEmpty(uniqueCondition) && Integer.parseInt(version) > 90500 && uniqueConditionIsIndex) {
//            for (String key : allColumn) {
//                preparedStatement.setObject(pos++, after.get(key));
//            }
//        }
//        preparedStatement.addBatch();
//    }

    //on conflict
    private void conflictUpdateInsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            String insertSql = "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                    + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") " +
                    "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ON CONFLICT("
                    + uniqueCondition.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", "))
                    + ") DO UPDATE SET " + allColumn.stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(", "));
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
    }

    private void conflictIgnoreInsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            String insertSql = "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                    + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") " +
                    "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ON CONFLICT("
                    + uniqueCondition.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", "))
                    + ") DO NOTHING ";
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
    }

    //with update
    private void withUpdateInsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            String insertSql;
            if (hasPk) {
                insertSql = "WITH upsert AS (UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " + allColumn.stream().map(k -> "\"" + k + "\"=?")
                        .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "\"" + k + "\"=?")
                        .collect(Collectors.joining(" AND ")) + " RETURNING *) INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                        + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                        + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
            } else {
                insertSql = "WITH upsert AS (UPDATE \"" + schema + "\".\"" + tapTable.getId() + "\" SET " + allColumn.stream().map(k -> "\"" + k + "\"=?")
                        .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))")
                        .collect(Collectors.joining(" AND ")) + " RETURNING *) INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                        + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                        + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
            }
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
        if (hasPk) {
            for (String key : uniqueCondition) {
                preparedStatement.setObject(pos++, after.get(key));
            }
        } else {
            for (String key : uniqueCondition) {
                preparedStatement.setObject(pos++, after.get(key));
                preparedStatement.setObject(pos++, after.get(key));
            }
        }
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
    }

    //insert not exists
    private void notExistsInsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            String insertSql;
            if (hasPk) {
                insertSql = "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                        + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                        + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT 1 FROM \"" + schema + "\".\"" + tapTable.getId()
                        + "\"  WHERE " + uniqueCondition.stream().map(k -> "\"" + k + "\"=?").collect(Collectors.joining(" AND ")) + " )";
            } else {
                insertSql = "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                        + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") SELECT "
                        + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT 1 FROM \"" + schema + "\".\"" + tapTable.getId()
                        + "\"  WHERE " + uniqueCondition.stream().map(k -> "(\"" + k + "\"=? OR (\"" + k + "\" IS NULL AND ?::text IS NULL))").collect(Collectors.joining(" AND ")) + " )";
            }
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
        if (hasPk) {
            for (String key : uniqueCondition) {
                preparedStatement.setObject(pos++, after.get(key));
            }
        } else {
            for (String key : uniqueCondition) {
                preparedStatement.setObject(pos++, after.get(key));
                preparedStatement.setObject(pos++, after.get(key));
            }
        }
    }

    //just insert
    private void justInsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            String insertSql = "INSERT INTO \"" + schema + "\".\"" + tapTable.getId() + "\" ("
                    + allColumn.stream().map(k -> "\"" + k + "\"").collect(Collectors.joining(", ")) + ") " +
                    "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ";
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
    }
}