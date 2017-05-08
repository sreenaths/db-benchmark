package com.sree.dbBenchmark;

import com.sree.dbBenchmark.data.DagData;
import com.sree.dbBenchmark.data.PerfData;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Tests {

  private static int BATCH_COUNT = 1;
  private static int BATCH_SIZE = 1;

  public void writeTests(PerfTest perfTest) throws IOException, SolrServerException, SQLException {
    System.out.printf("-- Starting incremental write test : %dx%d--\n", BATCH_COUNT, BATCH_SIZE);
    PerfData totalPerfData = new PerfData();

    for(int i = 0; i < BATCH_COUNT; i++) {

      List<DagData> list = new ArrayList<DagData>();
      int batchSize = BATCH_SIZE * i;

      for(int j = 0; j < batchSize; j++) {
        list.add(new DagData());
      }

      PerfData perfData = perfTest.writeData(list);

      System.out.printf("Batch %d | Batch size %d | Insert: %dms | Commit: %dms | %drecord/sec \n",
          (i + 1),
          list.size(),
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
