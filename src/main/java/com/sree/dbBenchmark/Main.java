package com.sree.dbBenchmark;

import com.sree.dbBenchmark.data.DagData;
import com.sree.dbBenchmark.impl.PostgresPerfTestImpl;
import com.sree.dbBenchmark.impl.SolrPerfTestImpl;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;
import java.sql.SQLException;

public class Main {
  public static void main(String args[]) throws IOException, SolrServerException, SQLException {
    System.out.println("-- Starting Tests --");

    Tests tests = new Tests();

//    PerfTest perfTest = new SolrPerfTestImpl();
    PerfTest perfTest = new PostgresPerfTestImpl();

    tests.writeTests(perfTest);

    System.out.println("----- End Tests -----");
  }
}
