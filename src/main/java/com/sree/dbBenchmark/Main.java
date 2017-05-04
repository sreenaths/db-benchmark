package com.sree.dbBenchmark;

import com.sree.dbBenchmark.data.DagData;
import com.sree.dbBenchmark.impl.SolrPerfTestImpl;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;

public class Main {
  public static void main(String args[]) throws IOException, SolrServerException {
    System.out.println("-- Starting Tests --");

    PerfTest perfTest = new SolrPerfTestImpl();

    Tests tests = new Tests();
    tests.writeTests(perfTest);

    System.out.println("----- End Tests -----");
  }
}
