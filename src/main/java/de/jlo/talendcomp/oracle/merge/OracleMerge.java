package de.jlo.talendcomp.oracle.merge;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import de.jlo.datamodel.SQLDataModel;
import de.jlo.datamodel.SQLField;
import de.jlo.datamodel.SQLSchema;
import de.jlo.datamodel.SQLTable;

/**
 * Builds a merge statement
 * @author jan.lolling@gmail.com
 *
 */
public class OracleMerge {
	
	private Connection connection = null;
	private String targetTableName = null;
	private SQLDataModel model;
	private String sourceSelectCode = null;
	private SQLTable targetTable = null;
	private List<ColumnValue> fixedColumnValueList = new ArrayList<ColumnValue>();
	private List<String> excludeColumnList = new ArrayList<>();
	private boolean allowInsert = true;
	private boolean allowUpdate = true;
	private boolean allowDelete = false;
	private String updateWhereCondition = null;
	private String deleteWhereCondition = null;
	private String currentMergeSQLCode = null;
	private boolean doCommit = true;
	
	public OracleMerge(Connection connection) {
		this.connection = connection;
	}

	public void init() throws Exception {
		if (connection == null) {
			throw new IllegalStateException("Connection not set!");
		}
		if (connection.isClosed()) {
			throw new IllegalStateException("Connection is closed!");
		}
		if (targetTableName == null || targetTableName.trim().isEmpty()) {
			throw new IllegalStateException("Target table name cannot be null or empty!");
		}
		model = new SQLDataModel(connection);
		model.loadCatalogs();
		targetTable = getTargetSQLTable();
	}
	
	protected final String getTableName(String schemaAndTable) {
		int pos = schemaAndTable.indexOf('.');
		if (pos > 0) {
			return schemaAndTable.substring(pos + 1, schemaAndTable.length());
		} else {
			return schemaAndTable;
		}
	}
	
	protected final String getSchemaName(String schemaAndTable) throws Exception {
		final int pos = schemaAndTable.indexOf('.');
		if (pos > 0) {
			return schemaAndTable.substring(0, pos);
		} else {
			return connection.getSchema();
		}
	}
	
	protected final SQLTable getTargetSQLTable() throws Exception {
		final String tableAndSchemaName = this.targetTableName;
		if (targetTable == null || targetTable.getAbsoluteName().equalsIgnoreCase(tableAndSchemaName) == false) {
			String schemaName = getSchemaName(tableAndSchemaName);
			if (schemaName == null) {
				throw new Exception("Schema cannot be resolved");
			}
			SQLSchema schema = model.getSchema(schemaName);
			if (schema == null) {
				throw new Exception("getTargetSQLTable failed: schema " + schemaName + " not available");
			}
			String tableName = getTableName(tableAndSchemaName);
			if (tableName.startsWith("\"")) {
				tableName = tableName.substring(1, tableName.length() - 1);
			}
			targetTable = schema.getTable(tableName);
			if (targetTable == null) {
				throw new Exception("getTargetSQLTable failed: table " + schemaName + "." + tableName + " not available");
			}
			targetTable.loadColumns();
			if (targetTable.getFieldCount() == 0) {
				throw new Exception("Table: " + targetTableName + " does not have any fields!");
			}
			if (targetTable.getPrimaryKeyConstraint() == null) {
				throw new Exception("Table: " + targetTableName + " does not have a primary key!");
			}
		}
		return targetTable;
	}
	
	public String buildMergeStatement() throws Exception {
		if (sourceSelectCode == null || sourceSelectCode.trim().isEmpty()) {
			throw new IllegalStateException("source select not set");
		}
		StringBuilder sb = new StringBuilder();
		sb.append("merge into ");
		sb.append(targetTable.getName());
		sb.append(" t\nusing (\n");
		sb.append(sourceSelectCode);
		sb.append("\n) s");
		sb.append("\non (");
		boolean firstLoop = true;
		for (String pkColumnName : targetTable.getPrimaryKeyFieldNames()) {
			if (isFixedColumn(pkColumnName) == false) {
				if (isExcludedColumn(pkColumnName)) {
					throw new Exception("A primary key column cannot excluded.");
				}
				if (firstLoop) {
					firstLoop = false;
				} else {
					sb.append(",");
				}
				sb.append("t.");
				sb.append(pkColumnName);
				sb.append("=s.");
				sb.append(pkColumnName);
			} else {
				throw new Exception("A primary key column cannot be set or compared with a fixed value.");
			}
		}
		sb.append(")\n");
		if (allowUpdate) {
			buildUpdatePart(sb);
		}
		if (allowInsert) {
			buildInsertPart(sb);
		}
		currentMergeSQLCode = sb.toString();
		return currentMergeSQLCode;
	}
	
	private void buildUpdatePart(StringBuilder sb) {
		sb.append("when matched then\n  update set\n");
		boolean firstLoop = true;
		for (String fieldName : targetTable.getNonPrimaryKeyFieldNames()) {
			if (isFixedColumn(fieldName)) {
				continue;
			}
			if (isExcludedColumn(fieldName)) {
				continue;
			}
			if (firstLoop) {
				firstLoop = false;
				sb.append("    t.");
			} else {
				sb.append(",\n    t.");
			}
			sb.append(fieldName);
			sb.append("=s.");
			sb.append(fieldName);
		}
		for (String fieldName : targetTable.getNonPrimaryKeyFieldNames()) {
			if (isFixedColumn(fieldName)) {
				if (firstLoop) {
					firstLoop = false;
					sb.append("    t.");
				} else {
					sb.append(",\n    t.");
				}
				sb.append(fieldName);
				sb.append("=?");
			}
		}
		if (updateWhereCondition != null) {
			sb.append("\n  where ");
			sb.append(updateWhereCondition);
		}
		if (allowDelete) {
			if (deleteWhereCondition == null) {
				throw new IllegalStateException("Missing delete where condition. Delete option not allowed without where condition!");
			}
			sb.append("\n  delete where ");
			sb.append(deleteWhereCondition);
		}
		sb.append("\n");
	}
	
	private boolean isFixedColumn(String name) {
		for (ColumnValue cv : fixedColumnValueList) {
			if (name.equals(cv.getColumnName())) {
				return true;
			}
		}
		return false;
	}
	
	private boolean isExcludedColumn(String name) {
		for (String s : excludeColumnList) {
			if (name.equals(s)) {
				return true;
			}
		}
		return false;
	}

	private void buildInsertPart(StringBuilder sb) {
		sb.append("when not matched then\n  insert (");
		boolean firstLoop = true;
		for (String fieldName : targetTable.getFieldNames()) {
			if (isFixedColumn(fieldName)) {
				continue;
			}
			if (isExcludedColumn(fieldName)) {
				continue;
			}
			if (firstLoop) {
				firstLoop = false;
				sb.append("t.");
			} else {
				sb.append(",t.");
			}
			sb.append(fieldName);
		}
		for (ColumnValue cv : fixedColumnValueList) {
			if (firstLoop) {
				firstLoop = false;
				sb.append("    t.");
			} else {
				sb.append(",t.");
			}
			sb.append(cv.getColumnName());
		}
		sb.append(")\n  values (");
		firstLoop = true;
		for (String fieldName : targetTable.getFieldNames()) {
			if (isFixedColumn(fieldName)) {
				continue;
			}
			if (isExcludedColumn(fieldName)) {
				continue;
			}
			if (firstLoop) {
				firstLoop = false;
				sb.append("s.");
			} else {
				sb.append(",s.");
			}
			sb.append(fieldName);
		}
		for (int i = 1; i <= fixedColumnValueList.size(); i++) {
			if (firstLoop) {
				firstLoop = false;
				sb.append("?");
			} else {
				sb.append(",?");
			}
		}
		sb.append(")");
	}
	
	private int getSQLTypeForTargetColumn(String columnName) throws Exception {
		SQLField f = targetTable.getField(columnName);
		if (f == null) {
			throw new Exception("Column: " + columnName + " does not exist in the target table: " + targetTable.getAbsoluteName());
		}
		return f.getType();
	}
	
	private int setupPreparedStatement(PreparedStatement ps, int paramIndex) throws Exception {
		for (int i = 0; i < fixedColumnValueList.size(); i++) {
			ColumnValue cv = fixedColumnValueList.get(i);
			if (cv.getValue() instanceof String) {
				ps.setString(paramIndex++, (String) cv.getValue()); 
			} else if (cv.getValue() instanceof Integer) {
				ps.setInt(paramIndex++, (Integer) cv.getValue());
			} else if (cv.getValue() instanceof Long) {
				ps.setLong(paramIndex++, (Long) cv.getValue());
			} else if (cv.getValue() instanceof BigDecimal) {
				ps.setBigDecimal(paramIndex++, (BigDecimal) cv.getValue());
			} else if (cv.getValue() instanceof Short) {
				ps.setShort(paramIndex++, (Short) cv.getValue());
			} else if (cv.getValue() instanceof Double) {
				ps.setDouble(paramIndex++, (Double) cv.getValue());
			} else if (cv.getValue() instanceof Float) {
				ps.setFloat(paramIndex++, (Float) cv.getValue());
			} else if (cv.getValue() instanceof Date) {
				ps.setDate(paramIndex++, new java.sql.Date(((Date) cv.getValue()).getTime()));
			} else if (cv.getValue() instanceof Boolean) {
				ps.setBoolean(paramIndex++, (Boolean) cv.getValue());
			} else if (cv.getValue() != null) {
				ps.setObject(paramIndex++, cv.getValue());
			} else {
				ps.setNull(paramIndex++, getSQLTypeForTargetColumn(cv.getColumnName()));
			}
		}
		return paramIndex;
	}
	
	private PreparedStatement prepareStatement() throws Exception {
		if (currentMergeSQLCode == null) {
			throw new IllegalStateException("Merge SQL statement not created, please call buildMergeStatement() before");
		}
		PreparedStatement ps = connection.prepareStatement(currentMergeSQLCode);
		// set prepared statement parameters
		int paramIndex = 1;
		if (allowUpdate) {
			paramIndex = setupPreparedStatement(ps, paramIndex);
		}
		if (allowInsert) {
			paramIndex = setupPreparedStatement(ps, paramIndex);
		}
		return ps;
	}

	public int execute() throws Exception {
		PreparedStatement ps = prepareStatement();
		int count = 0;
		try {
			count = ps.executeUpdate();
		} catch (SQLException sqle) {
			if (doCommit && connection.getAutoCommit() == false) {
				connection.rollback();
			}
			throw new Exception("Execute merge failed: " + sqle.getMessage() + "\nSQL:\n" + currentMergeSQLCode, sqle);
		}
		if (doCommit && connection.getAutoCommit() == false) {
			connection.commit();
		}
		return count;
	}
	
	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	public String getTargetTableName() {
		return targetTableName;
	}

	public void setTargetTableName(String targetTableName) {
		this.targetTableName = targetTableName;
	}

	public String getSourceSelectCode() {
		return sourceSelectCode;
	}

	public void setSourceSelectCode(String sourceSelectCode) {
		if (sourceSelectCode != null && sourceSelectCode.trim().isEmpty() == false) {
			this.sourceSelectCode = sourceSelectCode.trim();
		}
	}

	public String getUpdateWhereCondition() {
		return updateWhereCondition;
	}

	public void setUpdateWhereCondition(String updateWhereCondition) {
		if (updateWhereCondition != null && updateWhereCondition.trim().isEmpty() == false) {
			this.updateWhereCondition = updateWhereCondition.trim();
		}
	}

	public String getDeleteWhereCondition() {
		return deleteWhereCondition;
	}

	public void setDeleteWhereCondition(String deleteWhereCondition) {
		if (deleteWhereCondition != null && deleteWhereCondition.trim().isEmpty() == false) {
			this.deleteWhereCondition = deleteWhereCondition.trim();
		}
	}

	public boolean isAllowInsert() {
		return allowInsert;
	}

	public void setAllowInsert(boolean allowInsert) {
		this.allowInsert = allowInsert;
	}

	public boolean isAllowUpdate() {
		return allowUpdate;
	}

	public void setAllowUpdate(boolean allowUpdate) {
		this.allowUpdate = allowUpdate;
	}

	public boolean isAllowDelete() {
		return allowDelete;
	}

	public void setAllowDelete(boolean allowDelete) {
		this.allowDelete = allowDelete;
	}
	
	public void setFixedColumnValue(String name, Object value) {
		if (name != null && name.trim().isEmpty() == false) {
			ColumnValue cv = new ColumnValue(name.trim().toUpperCase());
			cv.setValue(value);
			fixedColumnValueList.add(cv);
		}
	}
	
	public void addExcludeColumn(String columnName) {
		if (columnName != null && columnName.trim().isEmpty() == false) {
			if (excludeColumnList.contains(columnName.trim().toUpperCase()) == false) {
				excludeColumnList.add(columnName.trim().toUpperCase());
			}
		}
	}

	public boolean isDoCommit() {
		return doCommit;
	}

	public void setDoCommit(boolean doCommit) {
		this.doCommit = doCommit;
	}
	
}
