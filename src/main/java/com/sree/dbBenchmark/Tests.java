package com.sree.dbBenchmark;

import com.sree.dbBenchmark.data.DagData;
import com.sree.dbBenchmark.data.PerfData;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Tests {

  private static int BATCH_COUNT = 100;
  private static int BATCH_SIZE = 100;

  public void writeTests(PerfTest perfTest) throws IOException, SolrServerException {
    System.out.println("-- Start write test --");
    PerfData totalPerfData = new PerfData();

    for(int i = 0; i < BATCH_COUNT; i++) {
      List<DagData> list = new ArrayList<DagData>();
      for(int j = 0; j < BATCH_SIZE; j++) {
        list.add(new DagData());
      }

      PerfData perfData = perfTest.writeData(list);

      System.out.printf("Batch %d | Insert: %dms | Commit: %dms | %drecord/sec \n",
          (i + 1),
          perfData.getEventDelay(1),
          perfData.getTotalDelay(),
          (BATCH_SIZE / (perfData.getTotalDelay()/1000)));
    }

    totalPerfData.registerEvent();
    System.out.printf("Total time : %dms\n", totalPerfData.getTotalDelay());
    System.out.println("-- End write test --");
  }

  public void readTests(PerfTest perfTest) {
  }

  public void readWriteTests(PerfTest perfTest) {
  }

}
