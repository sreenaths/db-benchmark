package com.sree.dbBenchmark.impl;

import com.sree.dbBenchmark.PerfTest;
import com.sree.dbBenchmark.data.DagData;
import com.sree.dbBenchmark.data.PerfData;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PostgresPerfTestImpl implements PerfTest{

  private Connection postgres;

  public static final String[] FIELDS = {"dagID", "appID", "appAttemptID", "callerContextID", "dagName, queueName", "user", "status", "logURL", "callerContextType", "submitTime", "startTime", "initTime", "finishTime", "timeTaken"};

  public PostgresPerfTestImpl() {
    postgres = getConnection();
  }

  private Connection getConnection() {
    Connection postgres = null;
    try {
      Class.forName("org.postgresql.Driver");
      postgres = DriverManager.getConnection("jdbc:postgresql://172.27.48.128:5432/postgres", "postgres", "blah");
      postgres.setAutoCommit(false);
    } catch (Exception e) {
      e.printStackTrace();
      System.err.println(e.getClass().getName()+": "+e.getMessage());
      System.exit(0);
    }
    return postgres;
  }

  private static String createSql(DagData dag) {
    String sql = "INSERT INTO dag (" +

        "dagID, appID, appAttemptID, callerContextID, " +
        "dagName, queueName, userName, " +
        "status, " +
        "tablesWritten, queryText, isDDL, " +
        "submitTime, startTime, initTime, finishTime, timeTaken, " +
        "logURL, callerContextType, " +
        "amWebServiceVersion, tezVersion, " +
        "dagPlan, vertexNameIdMap, diagnosticsTxt, counters, taskStats)" +

        " VALUES (" +

        "'%s', '%s', '%s', '%s', " +
        "'%s', '%s', '%s', " +
        "'%s', " +
        "%s, '%s', %b, " +
        "'%s', '%s', '%s', '%s', '%s', " +
        "'%s', '%s', " +
        "'%s', '%s', " +
        "'%s', '%s', '%s', '%s', '%s')";

    sql = String.format(sql,
        // Indexed
        dag.dagID,
        dag.appID,
        dag.appAttemptID,
        dag.callerContextID,

        dag.dagName,
        dag.queueName,
        dag.userName,

        dag.status,

        "ARRAY['" + String.join("','", dag.tablesWritten) + "']",
        dag.queryText.replace("'", "''"),
        dag.isDDL,

        dag.submitTime,
        dag.startTime,
        dag.initTime,
        dag.finishTime,
        dag.timeTaken,

        // Not Indexed
        dag.logURL,
        dag.callerContextType,

        dag.amWebServiceVersion,
        dag.tezVersion,

        dag.dagPlan,
        dag.vertexNameIdMap,
        dag.diagnostics,
        dag.counters,
        dag.taskStats
        );

    return sql;
  }

  public PerfData writeData(List<DagData> dataList) throws SQLException, SolrServerException, IOException {

    Statement stmt = postgres.createStatement();

    PerfData perfData = new PerfData();
    for(int i = 0; i < dataList.size(); i++) {
      stmt.executeUpdate(createSql(dataList.get(i)));
    }
    perfData.registerEvent();
    stmt.close();
    postgres.commit();
    perfData.registerEvent();

    return perfData;
  }

  public PerfData readData(String query, Integer rows) throws SQLException, IOException, SolrServerException{
    PerfData perfData = new PerfData();

    Statement stmt = postgres.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    ResultSet result = stmt.executeQuery( String.format("SELECT %s FROM dag WHERE %s LIMIT %d;", String.join(", ", FIELDS), query, rows));

    result.last();
    perfData.data = result.getRow();

    stmt.close();
    perfData.registerEvent();

    return perfData;
  }

  public PerfData getFacet() throws SQLException, IOException, SolrServerException {
    PerfData perfData = new PerfData();
    ResultSet result;

    perfData.data = 0;

    Statement stmt = postgres.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    result = stmt.executeQuery("(select count(1) as cnt, status, null AS queueName, null AS tablesWritten from dag where userName='userName1' GROUP BY status ORDER BY cnt DESC)" +
        "UNION ALL\n" +
        "(select count(1) as cnt, null AS status, queueName, null AS tablesWritten from dag where userName='userName1' GROUP BY queueName ORDER BY cnt DESC)\n" +
        "UNION ALL\n" +
        "(select count(1) as cnt, null AS status, null AS queueName, unnest(tablesWritten) tablesWritten from dag where userName='userName1' GROUP BY tablesWritten ORDER BY cnt DESC)\n");

//    result = stmt.executeQuery("(select count(1) as cnt, status, null AS queueName, null AS tablesWritten from dag where userName='userName1' GROUP BY status ORDER BY cnt DESC)\n" +
//        "UNION ALL\n" +
//        "(select count(1) as cnt, null AS status, queueName, null AS tablesWritten from dag where userName='userName1' GROUP BY queueName ORDER BY cnt DESC)\n" +
//        "UNION ALL\n" +
//        "(select count(1) as cnt, null AS status, null AS queueName, unnest(tablesWritten) tablesWritten from dag where userName='userName1' GROUP BY tablesWritten ORDER BY cnt DESC)\n");
    result.last();
    perfData.data += result.getRow();

    stmt.close();
    perfData.registerEvent();

    return perfData;
  }

  public void close() throws SQLException {
    postgres.close();
  }

}
