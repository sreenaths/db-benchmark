package com.sree.dbBenchmark;

import com.sree.dbBenchmark.data.DagData;
import com.sree.dbBenchmark.impl.PostgresPerfTestImpl;
import com.sree.dbBenchmark.impl.SolrPerfTestImpl;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Main {
  private static Random random = new Random();

  private static List<PerfTest> getPerfTestImpls() {
    List<PerfTest> perfTests = new ArrayList<PerfTest>();
    perfTests.add(new SolrPerfTestImpl());
    perfTests.add(new PostgresPerfTestImpl());

    return perfTests;
  }

  public static void main(String args[]) throws InterruptedException, IOException, SolrServerException, SQLException {
    System.out.println("-- Starting Tests --");

    // Single threaded tests
    Tests tests = new Tests();
    List<PerfTest> perfTests = getPerfTestImpls();
//    tests.writeTests(perfTests);
    tests.readTests(perfTests);
    perfTests.get(1).close();

    // Multithreaded tests
//    System.out.println("----- Concurrent Tests -----");
//
//    System.out.println("Search No, Solr Read Delay, Solr Records Returned, PG Read Delay, PG Records Returned");
//    int i;
//    for(i = 0; i < 50; i++) {
//      new ConcurrentTests(random.nextInt(10), i, getPerfTestImpls());
//    }
//    System.out.println("Created " + i + "threads");

    System.out.println("----- End Tests -----");
  }
}
