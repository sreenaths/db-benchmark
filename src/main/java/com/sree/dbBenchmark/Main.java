package com.sree.dbBenchmark;

import com.sree.dbBenchmark.data.DagData;
import com.sree.dbBenchmark.impl.PostgresPerfTestImpl;
import com.sree.dbBenchmark.impl.SolrPerfTestImpl;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Main {
  public static void main(String args[]) throws InterruptedException, IOException, SolrServerException, SQLException {
    System.out.println("-- Starting Tests --");

    Tests tests = new Tests();

    List<PerfTest> perfTests = new ArrayList<PerfTest>();
    perfTests.add(new SolrPerfTestImpl());
    perfTests.add(new PostgresPerfTestImpl());

    tests.writeTests(perfTests);

    tests.readTests(perfTests);

    perfTests.get(1).close();

    System.out.println("----- End Tests -----");
  }
}
