package de.jlo.talendcomp.oracle.merge;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.jlo.datamodel.SQLTable;

public class TestOracleMerge {
	
	private Connection connection = null;
	private static final String DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";
	private static final String URL = "jdbc:oracle:thin:@//vmh-lcag-hamosmig-db.dcx.dlh.de:1850/PAMOSDB";
	private static final String USER = "AMOS_TRANSFER_MIG";
	private static final String PW = "AMOS11admin";
	
	@Before
	public void connect() throws Exception {
		Class.forName(DRIVER_CLASS);
		connection = DriverManager.getConnection(URL, USER, PW);
	}
	
	@After
	public void disconnect() throws Exception {
		if (connection != null) {
			connection.close();
		}
	}
	
	@Test
	public void loadTable() throws Exception {
		OracleMerge m = new OracleMerge(connection);
		String expectedTableName = "XTEST";
		m.setTargetTableName(expectedTableName);
		m.init();
		SQLTable t = m.getTargetSQLTable();
		assertTrue(t != null);
		assertEquals("Table name wrong", expectedTableName, t.getName());
		assertEquals(7, t.getFieldCount());
		assertEquals(1, t.getPrimaryKeyFieldNames().size());
	}
	
	@Test
	public void testCreateMergeInsertOnly() throws Exception {
		OracleMerge m = new OracleMerge(connection);
		String expectedTableName = "JOB_INSTANCE_STATUS";
		m.setTargetTableName(expectedTableName);
		m.setAllowInsert(true);
		m.setAllowUpdate(false);
		m.setAllowDelete(false);
		m.addExcludeColumn("JOB_DISPLAY_NAME");
		m.setFixedColumnValue("PROCESS_INSTANCE_ID", 99);
		m.init();
		m.setSourceSelectCode("select * from JOB_INSTANCE_STATUS_TEST");
		String actual = m.buildMergeStatement();
		System.out.println(actual);
		String expected = "merge into JOB_INSTANCE_STATUS t\n"
			    + "using (\n"
			    + "select * from JOB_INSTANCE_STATUS_TEST\n"
			    + ") s\n"
			    + "on (t.JOB_INSTANCE_ID=s.JOB_INSTANCE_ID)\n"
			    + "when not matched then\n"
			    + "  insert (t.JOB_INSTANCE_ID,t.PROCESS_INSTANCE_NAME,t.JOB_NAME,t.JOB_PROJECT,t.JOB_INFO,t.JOB_GUID,t.JOB_EXT_ID,t.ROOT_JOB_GUID,t.WORK_ITEM,t.TIME_RANGE_START,t.TIME_RANGE_END,t.VALUE_RANGE_START,t.VALUE_RANGE_END,t.JOB_STARTED_AT,t.JOB_ENDED_AT,t.JOB_RESULT,t.COUNT_INPUT,t.COUNT_OUTPUT,t.COUNT_UPDATED,t.COUNT_REJECTED,t.COUNT_DELETED,t.RETURN_CODE,t.RETURN_MESSAGE,t.HOST_NAME,t.HOST_PID,t.HOST_USER,t.PROCESS_INSTANCE_ID)\n"
			    + "  values (s.JOB_INSTANCE_ID,s.PROCESS_INSTANCE_NAME,s.JOB_NAME,s.JOB_PROJECT,s.JOB_INFO,s.JOB_GUID,s.JOB_EXT_ID,s.ROOT_JOB_GUID,s.WORK_ITEM,s.TIME_RANGE_START,s.TIME_RANGE_END,s.VALUE_RANGE_START,s.VALUE_RANGE_END,s.JOB_STARTED_AT,s.JOB_ENDED_AT,s.JOB_RESULT,s.COUNT_INPUT,s.COUNT_OUTPUT,s.COUNT_UPDATED,s.COUNT_REJECTED,s.COUNT_DELETED,s.RETURN_CODE,s.RETURN_MESSAGE,s.HOST_NAME,s.HOST_PID,s.HOST_USER,?)";
		assertEquals("merge statement wrong", expected, actual);
	}

	@Test
	public void testCreateMergeInsertOnly2() throws Exception {
		OracleMerge m = new OracleMerge(connection);
		m.setSourceSelectCode("select * from S_TEST");
		String expectedTableName = "XTEST";
		m.setAllowInsert(true);
		m.setAllowUpdate(false);
		m.setAllowDelete(false);
		m.setFixedColumnValue("JOB_INSTANCE_ID", 99);
		m.setFixedColumnValue("FILE_ID", null);
		m.setFixedColumnValue("MAPPING_ID", 2);
		m.setFixedColumnValue("AMOS_IMPORT_OK", 0);
		m.setTargetTableName(expectedTableName);
		m.init();
		String actual = m.buildMergeStatement();
		System.out.println(actual);
		String expected = "merge into XTEST t\n"
			    + "using (\n"
			    + "select * from S_TEST\n"
			    + ") s\n"
			    + "on (coalesce(t.CONCODE,'0')=coalesce(s.CONCODE,'0') and coalesce(t.ACTYPE,'0')=coalesce(s.ACTYPE,'0'))\n"
			    + "when not matched then\n"
			    + "  insert (t.CONCODE,t.ACTYPE,t.JOB_INSTANCE_ID,t.FILE_ID,t.MAPPING_ID,t.AMOS_IMPORT_OK)\n"
			    + "  values (s.CONCODE,s.ACTYPE,?,?,?,?)";
		assertEquals("merge statement wrong", expected, actual);
		m.setDoCommit(true);
		m.execute();
	}

	@Test
	public void testCreateMergeUpdateOnly() throws Exception {
		OracleMerge m = new OracleMerge(connection);
		String expectedTableName = "JOB_INSTANCE_STATUS";
		m.setTargetTableName(expectedTableName);
		m.setAllowInsert(false);
		m.setAllowUpdate(true);
		m.setAllowDelete(false);
		m.addExcludeColumn("JOB_DISPLAY_NAME");
		m.setFixedColumnValue("PROCESS_INSTANCE_ID", 99);
		m.init();
		m.setSourceSelectCode("select * from JOB_INSTANCE_STATUS_TEST");
		String actual = m.buildMergeStatement();
		System.out.println(actual);
		String expected = "merge into JOB_INSTANCE_STATUS t\n"
			    + "using (\n"
			    + "select * from JOB_INSTANCE_STATUS_TEST\n"
			    + ") s\n"
			    + "on (t.JOB_INSTANCE_ID=s.JOB_INSTANCE_ID)\n"
			    + "when matched then\n"
			    + "  update set\n"
			    + "    t.PROCESS_INSTANCE_NAME=s.PROCESS_INSTANCE_NAME,\n"
			    + "    t.JOB_NAME=s.JOB_NAME,\n"
			    + "    t.JOB_PROJECT=s.JOB_PROJECT,\n"
			    + "    t.JOB_INFO=s.JOB_INFO,\n"
			    + "    t.JOB_GUID=s.JOB_GUID,\n"
			    + "    t.JOB_EXT_ID=s.JOB_EXT_ID,\n"
			    + "    t.ROOT_JOB_GUID=s.ROOT_JOB_GUID,\n"
			    + "    t.WORK_ITEM=s.WORK_ITEM,\n"
			    + "    t.TIME_RANGE_START=s.TIME_RANGE_START,\n"
			    + "    t.TIME_RANGE_END=s.TIME_RANGE_END,\n"
			    + "    t.VALUE_RANGE_START=s.VALUE_RANGE_START,\n"
			    + "    t.VALUE_RANGE_END=s.VALUE_RANGE_END,\n"
			    + "    t.JOB_STARTED_AT=s.JOB_STARTED_AT,\n"
			    + "    t.JOB_ENDED_AT=s.JOB_ENDED_AT,\n"
			    + "    t.JOB_RESULT=s.JOB_RESULT,\n"
			    + "    t.COUNT_INPUT=s.COUNT_INPUT,\n"
			    + "    t.COUNT_OUTPUT=s.COUNT_OUTPUT,\n"
			    + "    t.COUNT_UPDATED=s.COUNT_UPDATED,\n"
			    + "    t.COUNT_REJECTED=s.COUNT_REJECTED,\n"
			    + "    t.COUNT_DELETED=s.COUNT_DELETED,\n"
			    + "    t.RETURN_CODE=s.RETURN_CODE,\n"
			    + "    t.RETURN_MESSAGE=s.RETURN_MESSAGE,\n"
			    + "    t.HOST_NAME=s.HOST_NAME,\n"
			    + "    t.HOST_PID=s.HOST_PID,\n"
			    + "    t.HOST_USER=s.HOST_USER,\n"
			    + "    t.PROCESS_INSTANCE_ID=?\n";
		assertEquals("merge statement wrong", expected, actual);
	}

	@Test
	public void testCreateMergeUpdateOnlyWithWhere() throws Exception {
		OracleMerge m = new OracleMerge(connection);
		String expectedTableName = "JOB_INSTANCE_STATUS";
		m.setTargetTableName(expectedTableName);
		m.setAllowInsert(false);
		m.setAllowUpdate(true);
		m.setAllowDelete(false);
		m.setUpdateWhereCondition("RETURN_CODE = 0");
		m.addExcludeColumn("JOB_DISPLAY_NAME");
		m.setFixedColumnValue("PROCESS_INSTANCE_ID", 99);
		m.init();
		m.setSourceSelectCode("select * from JOB_INSTANCE_STATUS_TEST");
		String actual = m.buildMergeStatement();
		System.out.println(actual);
		String expected = "merge into JOB_INSTANCE_STATUS t\n"
			    + "using (\n"
			    + "select * from JOB_INSTANCE_STATUS_TEST\n"
			    + ") s\n"
			    + "on (t.JOB_INSTANCE_ID=s.JOB_INSTANCE_ID)\n"
			    + "when matched then\n"
			    + "  update set\n"
			    + "    t.PROCESS_INSTANCE_NAME=s.PROCESS_INSTANCE_NAME,\n"
			    + "    t.JOB_NAME=s.JOB_NAME,\n"
			    + "    t.JOB_PROJECT=s.JOB_PROJECT,\n"
			    + "    t.JOB_INFO=s.JOB_INFO,\n"
			    + "    t.JOB_GUID=s.JOB_GUID,\n"
			    + "    t.JOB_EXT_ID=s.JOB_EXT_ID,\n"
			    + "    t.ROOT_JOB_GUID=s.ROOT_JOB_GUID,\n"
			    + "    t.WORK_ITEM=s.WORK_ITEM,\n"
			    + "    t.TIME_RANGE_START=s.TIME_RANGE_START,\n"
			    + "    t.TIME_RANGE_END=s.TIME_RANGE_END,\n"
			    + "    t.VALUE_RANGE_START=s.VALUE_RANGE_START,\n"
			    + "    t.VALUE_RANGE_END=s.VALUE_RANGE_END,\n"
			    + "    t.JOB_STARTED_AT=s.JOB_STARTED_AT,\n"
			    + "    t.JOB_ENDED_AT=s.JOB_ENDED_AT,\n"
			    + "    t.JOB_RESULT=s.JOB_RESULT,\n"
			    + "    t.COUNT_INPUT=s.COUNT_INPUT,\n"
			    + "    t.COUNT_OUTPUT=s.COUNT_OUTPUT,\n"
			    + "    t.COUNT_UPDATED=s.COUNT_UPDATED,\n"
			    + "    t.COUNT_REJECTED=s.COUNT_REJECTED,\n"
			    + "    t.COUNT_DELETED=s.COUNT_DELETED,\n"
			    + "    t.RETURN_CODE=s.RETURN_CODE,\n"
			    + "    t.RETURN_MESSAGE=s.RETURN_MESSAGE,\n"
			    + "    t.HOST_NAME=s.HOST_NAME,\n"
			    + "    t.HOST_PID=s.HOST_PID,\n"
			    + "    t.HOST_USER=s.HOST_USER,\n"
			    + "    t.PROCESS_INSTANCE_ID=?\n"
			    + "  where RETURN_CODE = 0\n";
		assertEquals("merge statement wrong", expected, actual);
	}

	@Test
	public void testCreateMergeUpdateWithDelete() throws Exception {
		OracleMerge m = new OracleMerge(connection);
		m.setAllowInsert(false);
		m.setAllowUpdate(true);
		m.setAllowDelete(true);
		m.setUpdateWhereCondition("RETURN_CODE = 0");
		m.setDeleteWhereCondition("RETURN_CODE > 0");
		m.addExcludeColumn("JOB_DISPLAY_NAME");
		m.setFixedColumnValue("PROCESS_INSTANCE_ID", 99);
		m.setSourceSelectCode("select * from JOB_INSTANCE_STATUS_TEST");
		String expectedTableName = "JOB_INSTANCE_STATUS";
		m.setTargetTableName(expectedTableName);
		m.init();
		String actual = m.buildMergeStatement();
		System.out.println(actual);
		String expected = "merge into JOB_INSTANCE_STATUS t\n"
			    + "using (\n"
			    + "select * from JOB_INSTANCE_STATUS_TEST\n"
			    + ") s\n"
			    + "on (t.JOB_INSTANCE_ID=s.JOB_INSTANCE_ID)\n"
			    + "when matched then\n"
			    + "  update set\n"
			    + "    t.PROCESS_INSTANCE_NAME=s.PROCESS_INSTANCE_NAME,\n"
			    + "    t.JOB_NAME=s.JOB_NAME,\n"
			    + "    t.JOB_PROJECT=s.JOB_PROJECT,\n"
			    + "    t.JOB_INFO=s.JOB_INFO,\n"
			    + "    t.JOB_GUID=s.JOB_GUID,\n"
			    + "    t.JOB_EXT_ID=s.JOB_EXT_ID,\n"
			    + "    t.ROOT_JOB_GUID=s.ROOT_JOB_GUID,\n"
			    + "    t.WORK_ITEM=s.WORK_ITEM,\n"
			    + "    t.TIME_RANGE_START=s.TIME_RANGE_START,\n"
			    + "    t.TIME_RANGE_END=s.TIME_RANGE_END,\n"
			    + "    t.VALUE_RANGE_START=s.VALUE_RANGE_START,\n"
			    + "    t.VALUE_RANGE_END=s.VALUE_RANGE_END,\n"
			    + "    t.JOB_STARTED_AT=s.JOB_STARTED_AT,\n"
			    + "    t.JOB_ENDED_AT=s.JOB_ENDED_AT,\n"
			    + "    t.JOB_RESULT=s.JOB_RESULT,\n"
			    + "    t.COUNT_INPUT=s.COUNT_INPUT,\n"
			    + "    t.COUNT_OUTPUT=s.COUNT_OUTPUT,\n"
			    + "    t.COUNT_UPDATED=s.COUNT_UPDATED,\n"
			    + "    t.COUNT_REJECTED=s.COUNT_REJECTED,\n"
			    + "    t.COUNT_DELETED=s.COUNT_DELETED,\n"
			    + "    t.RETURN_CODE=s.RETURN_CODE,\n"
			    + "    t.RETURN_MESSAGE=s.RETURN_MESSAGE,\n"
			    + "    t.HOST_NAME=s.HOST_NAME,\n"
			    + "    t.HOST_PID=s.HOST_PID,\n"
			    + "    t.HOST_USER=s.HOST_USER,\n"
			    + "    t.PROCESS_INSTANCE_ID=?\n"
			    + "  where RETURN_CODE = 0\n"
			    + "  delete where RETURN_CODE > 0\n";
		assertEquals("merge statement wrong", expected, actual);
	}
	
	@Test
	public void testCreateMergeInsertAndUpdateWithDelete() throws Exception {
		OracleMerge m = new OracleMerge(connection);
		m.setAllowInsert(true);
		m.setAllowUpdate(true);
		m.setAllowDelete(true);
		m.setUpdateWhereCondition("RETURN_CODE = 0");
		m.setDeleteWhereCondition("RETURN_CODE > 0");
		m.addExcludeColumn("JOB_DISPLAY_NAME");
		m.setFixedColumnValue("PROCESS_INSTANCE_ID", 99);
		m.setSourceSelectCode("select * from JOB_INSTANCE_STATUS_TEST");
		String expectedTableName = "JOB_INSTANCE_STATUS";
		m.setTargetTableName(expectedTableName);
		m.init();
		String actual = m.buildMergeStatement();
		System.out.println(actual);
		String expected = "merge into JOB_INSTANCE_STATUS t\n"
			    + "using (\n"
			    + "select * from JOB_INSTANCE_STATUS_TEST\n"
			    + ") s\n"
			    + "on (t.JOB_INSTANCE_ID=s.JOB_INSTANCE_ID)\n"
			    + "when matched then\n"
			    + "  update set\n"
			    + "    t.PROCESS_INSTANCE_NAME=s.PROCESS_INSTANCE_NAME,\n"
			    + "    t.JOB_NAME=s.JOB_NAME,\n"
			    + "    t.JOB_PROJECT=s.JOB_PROJECT,\n"
			    + "    t.JOB_INFO=s.JOB_INFO,\n"
			    + "    t.JOB_GUID=s.JOB_GUID,\n"
			    + "    t.JOB_EXT_ID=s.JOB_EXT_ID,\n"
			    + "    t.ROOT_JOB_GUID=s.ROOT_JOB_GUID,\n"
			    + "    t.WORK_ITEM=s.WORK_ITEM,\n"
			    + "    t.TIME_RANGE_START=s.TIME_RANGE_START,\n"
			    + "    t.TIME_RANGE_END=s.TIME_RANGE_END,\n"
			    + "    t.VALUE_RANGE_START=s.VALUE_RANGE_START,\n"
			    + "    t.VALUE_RANGE_END=s.VALUE_RANGE_END,\n"
			    + "    t.JOB_STARTED_AT=s.JOB_STARTED_AT,\n"
			    + "    t.JOB_ENDED_AT=s.JOB_ENDED_AT,\n"
			    + "    t.JOB_RESULT=s.JOB_RESULT,\n"
			    + "    t.COUNT_INPUT=s.COUNT_INPUT,\n"
			    + "    t.COUNT_OUTPUT=s.COUNT_OUTPUT,\n"
			    + "    t.COUNT_UPDATED=s.COUNT_UPDATED,\n"
			    + "    t.COUNT_REJECTED=s.COUNT_REJECTED,\n"
			    + "    t.COUNT_DELETED=s.COUNT_DELETED,\n"
			    + "    t.RETURN_CODE=s.RETURN_CODE,\n"
			    + "    t.RETURN_MESSAGE=s.RETURN_MESSAGE,\n"
			    + "    t.HOST_NAME=s.HOST_NAME,\n"
			    + "    t.HOST_PID=s.HOST_PID,\n"
			    + "    t.HOST_USER=s.HOST_USER,\n"
			    + "    t.PROCESS_INSTANCE_ID=?\n"
			    + "  where RETURN_CODE = 0\n"
			    + "  delete where RETURN_CODE > 0\n";
		assertEquals("merge statement wrong", expected, actual);
	}

	@Test
	public void testCreateMergeInsertAndUpdateOnlyColumnsWithDelete() throws Exception {
		OracleMerge m = new OracleMerge(connection);
		m.setAllowInsert(false);
		m.setAllowUpdate(true);
		m.addUpdateOnlyColumn("JOB_GUID");
		m.addUpdateOnlyColumn("WORK_ITEM");
		m.setAllowDelete(true);
		m.setUpdateWhereCondition("RETURN_CODE = 0");
		m.setDeleteWhereCondition("RETURN_CODE > 0");
		m.addExcludeColumn("JOB_DISPLAY_NAME");
		m.setFixedColumnValue("PROCESS_INSTANCE_ID", 99);
		m.setSourceSelectCode("select * from JOB_INSTANCE_STATUS_TEST");
		String expectedTableName = "JOB_INSTANCE_STATUS";
		m.setTargetTableName(expectedTableName);
		m.init();
		String actual = m.buildMergeStatement();
		System.out.println(actual);
		String expected = "merge into JOB_INSTANCE_STATUS t\n"
			    + "using (\n"
			    + "select * from JOB_INSTANCE_STATUS_TEST\n"
			    + ") s\n"
			    + "on (t.JOB_INSTANCE_ID=s.JOB_INSTANCE_ID)\n"
			    + "when matched then\n"
			    + "  update set\n"
			    + "    t.JOB_GUID=s.JOB_GUID,\n"
			    + "    t.WORK_ITEM=s.WORK_ITEM,\n"
			    + "    t.PROCESS_INSTANCE_ID=?\n"
			    + "  where RETURN_CODE = 0\n"
			    + "  delete where RETURN_CODE > 0\n";
		assertEquals("merge statement wrong", expected, actual);
	}

	@Test
	public void testCreateMergeInsertAndUpdateOnlyColumns() throws Exception {
		OracleMerge m = new OracleMerge(connection);
		m.setAllowInsert(false);
		m.setAllowUpdate(true);
		m.addUpdateOnlyColumn("AMOS_IMPORT_OK");
		m.setAllowDelete(false);
		m.setSourceSelectCode("select * from Z_XDHECT");
		String expectedTableName = "XDHECT";
		m.setTargetTableName(expectedTableName);
		m.init();
		String actual = m.buildMergeStatement();
		System.out.println(actual);
		String expected = "merge into XDHECT t\n"
			    + "using (\n"
			    + "select * from Z_XDHECT\n"
			    + ") s\n"
			    + "on (t.DOCNO=s.DOCNO and t.DOCTYPE=s.DOCTYPE and t.REVISION=s.REVISION and t.ISSUEDBY=s.ISSUEDBY)\n"
			    + "when matched then\n"
			    + "  update set\n"
			    + "    t.AMOS_IMPORT_OK=s.AMOS_IMPORT_OK\n";
		assertEquals("merge statement wrong", expected, actual);
	}

}
