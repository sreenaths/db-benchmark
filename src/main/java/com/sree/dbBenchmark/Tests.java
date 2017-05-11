package com.sree.dbBenchmark;

import com.sree.dbBenchmark.data.DagData;
import com.sree.dbBenchmark.data.PerfData;
import com.sree.dbBenchmark.impl.SolrPerfTestImpl;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Tests {

  private static int[] randBatchSize = {325,439,509,31,635,427,28,353,46,40,223,25,209,89,601,21,14,728,191,244,44,411,420,67,24,132,578,101,335,238,64,258,13,220,249,312,282,728,98,719,629,738,94,82,524,21,14,107,381,163,5,26,140,74,65,633,165,638,177,504,208,82,374,92,265,49,763,64,355,54,382,261,916,804,161,483,2,147,374,435,423,70,85,176,138,446,275,197,504,695,286,32,577,44,4,8,9,473,819,15,782,151,85,188,898,277,455,304,208,364,463,248,279,110,31,28,32,280,4,203,126,327,129,194,343,68,140,83,99,361,84,59,30,48,369,704,442,240,353,637,553,206,19,15,102,48,11,414,226,97,166,114,594,19,128,722,26,920,165,34,270,342,60,517,5,324,192,233,299,290,13,463,72,465,11,119,10,579,83,57,94,60,63,327,16,116,164,137,146,183,132,300,534,88,85,249,299,30,529,76,268,116,4,45,713,34,235,142,53,243,487,435,71,13,783,137,63,24,21,204,17,312,21,200,308,582,2,283,2,612,417,76,118,31,110,22,2,32,15,586,16,155,92,140,257,590,60,83,725,325,11,654,311,237,751,271,309,77,50,105,19,1,485,272,431,473,581,767,779,193,10,608,40,126,134,347,713,29,87,523,22,33,467,22,430,168,248,237,87,618,48,358,299,263,36,231,393,131,609,66,80,236,129,120,194,341,587,722,221,36,586,541,303,147,135,37,268,77,128,61,238,265,39,427,68,81,233,516,19,76,488,131,581,440,525,85,358,126,513,94,450,30,44,545,267,117,73,206,36,474,33,109,747,269,93,459,229,5,312,142,222,293,3,35,1,57,358,515,158,43,463,36,944,100,864,465,132,53,23,586,66,125,64,219,52,280,21,206,753,190,744,88,137,33,90,64,16,438,122,505,29,170,38,53,69,160,646,645,320,93,41,118,243,214,601,390,52,320,27,505,5,52,4,887,536,88,305,161,160,218,73,71,180,84,416,19,53,144,486,38,488,24,27,136,122,1,764,520,115,57,223,120,234,189,302,554,347,234,55,686,121,198,584,47,15,12,354,32,305,401,185,210,187,591,698,13,517,205,503,17,106,50,19,241,39,656,376,641,664,75,283,560,223,213,488,1,530,330,632,437,16,762,217,160,130,389,277,20,284,432,149,77,306,495,7,358,187,168,162,359,292,127,17,189,536,603,411,58,344,208,118,109,52,22,14,4,356,370,38,288,12,411,443,306,356,15,215,99,56,171,78,71,81,304,57,42,114,11,884,35,221,505,40,62,49,169,170,438,235,364,485,79,4,367,624,22,509,499,33,489,251,511,14,94,754,150,272,381,59,64,125,95,455,151,702,566,524,64,322,6,230,110,60,501,73,301,548,15,243,118,277,110,230,73,143,429,77,261,36,3,977,439,95,267,612,3,107,58,182,34,86,309,140,31,23,819,872,760,906,451,826,701,18,177,27,586,225,96,11,631,451,679,177,436,813,152,65,223,202,581,244,347,2,684,688,74,1,176,112,92,629,739,18,178,52,269,287,199,296,452,353,174,296,406,123,157,438,152,319,452,244,577,412,701,29,535,297,4,58,187,439,16,204,24,105,242,59,59,221,95,568,386,22,413,32,144,129,202,495,112,54,466,629,149,31,29,365,198,196,226,31,180,78,163,11,433,349,225,247,7,254,172,242,388,119,165,679,23,493,293,117,387,5,215,76,847,223,18,22,174,90,82,606,18,453,37,165,948,25,84,125,316,640,151,552,610,22,224,118,57,43,240,424,733,131,146,266,57,139,25,320,139,194,383,208,105,242,97,75,31,716,124,555,75,353,70,433,44,22,75,46,8,313,220,587,96,392,116,2,54,65,64,100,167,789,41,45,317,32,58,607,335,139,37,354,93,141,268,46,186,262,5,491,469,130,866,142,18,49,368,102,18,279,522,119,234,16,12,237,447,103,138,818,402,203,901,610,334,78,383,362,657,219,583,192,701,382,60,152,181,282,38,265,166,619,65,687,110,247,3,446,101,216,5,88,594,1,118,354,348,129,127,503,284,26,109,350,57,210,344,28,275,35,112,491,801,302,2,585,2,338,263,170,435,5,237,168,34,2,3,29,283,273,95,47,246,328,235,260,756,150,299,140,232,21,296,155,121,617,201,106,181,155,368,165,42,409,247,634,702,50,61,229,64,101,98,272,335,246,1,287,165,129,140,253,10,324,97,399,732,123,34,286,99,365,165,140,20,132,176,281,108,105,28,114,56,36,129,215,362};
  private static int BATCH_REDUCTION_FACTOR = 1;
  private static int BATCH_COUNT = 5000;

  public void writeTests(List<PerfTest> perfTests) throws InterruptedException, IOException, SolrServerException, SQLException {
    System.out.printf("-- Starting random write test : Batch count %d --\n", BATCH_COUNT);

    System.out.println("Batch No, Batch Size, Record Count," +
        "Solr Write, Solr Commit, Solr RPS, " +
        "Postgres write, Postgres commmit, Postgres RPS");

    PerfData totalPerfData = new PerfData();
    int recordCount = 0;

    for(int i = 0; i < BATCH_COUNT; i++) {

      List<DagData> list = new ArrayList<DagData>();
      int batchSize = Math.round(randBatchSize[i % randBatchSize.length] / BATCH_REDUCTION_FACTOR);

      if(batchSize == 0) {
        batchSize = 1;
      }

      for(int j = 0; j < batchSize; j++) {
        list.add(new DagData());
        recordCount++;
      }

      PerfData perfDataS = perfTests.get(0).writeData(list);
      PerfData perfDataP = perfTests.get(1).writeData(list);

      System.out.printf("%d,%d,%d,%d,%d,%d,%d,%d,%d\n", i, batchSize, recordCount,
          perfDataS.getEventDelay(1), perfDataS.getEventDelay(2), Math.round(batchSize / (perfDataS.getTotalDelay() * 0.001)),
          perfDataP.getEventDelay(1), perfDataP.getEventDelay(2), Math.round(batchSize / (perfDataP.getTotalDelay() * 0.001))
      );

      TimeUnit.SECONDS.sleep(1);
    }

    totalPerfData.registerEvent();
    System.out.printf("Total time : %dms\nTotal records: %d\n", totalPerfData.getTotalDelay(), recordCount);
    System.out.println("-- End write test --");
  }

  public void readTests(List<PerfTest> perfTests) throws InterruptedException, IOException, SolrServerException, SQLException {

    // -- Exact match --
    // 1. All dag by user name (We have userName0-userName9 )
    System.out.println("1. Search No, Solr Read Delay, Solr Records Returned, PG Read Delay, PG Records Returned");
    for(int i=0; i<10; i++ ) {
      PerfData perfDataS = perfTests.get(0).readData(String.format("user:userName%d", i), 10);
      PerfData perfDataP = perfTests.get(1).readData(String.format("userName = 'userName%d'", i), 10);

      System.out.printf("%d, %d, %d, %d, %d\n", i, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);
    }

    // 2. All DAGs with status X
    System.out.println("2. Search No, Solr Read Delay, Solr Records Returned, PG Read Delay, PG Records Returned");
    for(int i=0; i<DagData.STATUSES.size(); i++ ) {
      String status = DagData.STATUSES.get(i);
      PerfData perfDataS = perfTests.get(0).readData(String.format("-status:%s", status), 100);
      PerfData perfDataP = perfTests.get(1).readData(String.format("status = '%s'", status), 100);

      System.out.printf("%d, %d, %d, %d, %d\n", i, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);
    }
    // 3. All DAGs with status != X
    System.out.println("3. Search No, Solr Read Delay, Solr Records Returned, PG Read Delay, PG Records Returned");
    for(int i=0; i<DagData.STATUSES.size(); i++ ) {
      String status = DagData.STATUSES.get(i);
      PerfData perfDataS = perfTests.get(0).readData(String.format("status:%s", status), 10);
      PerfData perfDataP = perfTests.get(1).readData(String.format("status != '%s'", status), 10);

      System.out.printf("%d, %d, %d, %d, %d\n", i, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);
    }

    // 4. All DAGs with status in KILLED or FAILED
    System.out.println("4. Search No, Solr Read Delay, Solr Records Returned, PG Read Delay, PG Records Returned");
    for(int i=0; i<5; i++ ) {
      PerfData perfDataS = perfTests.get(0).readData(String.format("status:(KILLED FAILED)"), 10);
      PerfData perfDataP = perfTests.get(1).readData(String.format("status IN ('KILLED', 'FAILED')"), 10);

      System.out.printf("%d, %d, %d, %d, %d\n", i, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);
    }

    // 5. tableX in tablesWritten - Check against multiple values
    System.out.println("5. Search No, Solr Read Delay, Solr Records Returned, PG Read Delay, PG Records Returned");
    for(int i=0; i<10; i++ ) {
      PerfData perfDataS = perfTests.get(0).readData(String.format("tablesWritten:table_%d", i), 10);
      PerfData perfDataP = perfTests.get(1).readData(String.format("'table_%s' = ANY(tablesWritten)", i), 10);

      System.out.printf("%d, %d, %d, %d, %d\n", i, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);
    }
    // 6. Add DAGs under the application x
    System.out.println("6. Search No, Solr Read Delay, Solr Records Returned, PG Read Delay, PG Records Returned");
    for(int i=0; i<10; i++ ) {
      PerfData perfDataS = perfTests.get(0).readData(String.format("appID:application_%d", i), 10);
      PerfData perfDataP = perfTests.get(1).readData(String.format("appID = 'application_%s'", i), 10);

      System.out.printf("%d, %d, %d, %d, %d\n", i, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);
    }
    // 7. is DDL - Binary value check
    System.out.println("7. Search No, Solr Read Delay, Solr Records Returned, PG Read Delay, PG Records Returned");
    for(int i=0; i<10; i++ ) {
      PerfData perfDataS = perfTests.get(0).readData(String.format("isDDL:%b", i%2 == 0), 10);
      PerfData perfDataP = perfTests.get(1).readData(String.format("isDDL = %b", i%2 == 0), 10);

      System.out.printf("%d, %d, %d, %d, %d\n", i, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);
    }

    // -- Range search --
    // 8. Queries that took less than X min
    System.out.println("8. Search No, Solr Read Delay, Solr Records Returned, PG Read Delay, PG Records Returned");
    for(int i=1; i<10; i++ ) {
      PerfData perfDataS = perfTests.get(0).readData(String.format("timeTaken:[* TO %d]", i*2000), 10);
      PerfData perfDataP = perfTests.get(1).readData(String.format("timeTaken < %d", i*2000), 10);

      System.out.printf("%d, %d, %d, %d, %d\n", i, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);
    }
    // 9. Queries that took more than x mins
    System.out.println("9. Search No, Solr Read Delay, Solr Records Returned, PG Read Delay, PG Records Returned");
    for(int i=1; i<10; i++ ) {
      PerfData perfDataS = perfTests.get(0).readData(String.format("timeTaken:[%d TO *]", i*2000), 10);
      PerfData perfDataP = perfTests.get(1).readData(String.format("timeTaken > %d", i*2000), 10);

      System.out.printf("%d, %d, %d, %d, %d\n", i, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);
    }
    // 10. Queries that took between 5-10 min
    System.out.println("10. Search No, Solr Read Delay, Solr Records Returned, PG Read Delay, PG Records Returned");
    for(int i=1; i<10; i++ ) {
      PerfData perfDataS = perfTests.get(0).readData(String.format("timeTaken:[%d TO %d]", i*2000, i*2000+2000), 10);
      PerfData perfDataP = perfTests.get(1).readData(String.format("timeTaken BETWEEN %d AND %d", i*2000, i*2000+2000), 10);

      System.out.printf("%d, %d, %d, %d, %d\n", i, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);
    }

    // -- Pattern match --
    // 11. Query text containing pattern - "%ORDER BY%" (Complete words)
    System.out.println("11. Search No, Solr Read Delay, Solr Records Returned, PG Read Delay, PG Records Returned");
    for(int i=0; i<10; i++ ) {
      PerfData perfDataS = perfTests.get(0).readData("queryText:\"ORDER BY\"", 10);
      PerfData perfDataP = perfTests.get(1).readData("queryText LIKE '%ORDER BY%'", 10);

      System.out.printf("%d, %d, %d, %d, %d\n", i, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);
    }
    // -- Pattern match --
    // 12. Query text containing pattern - "%RDER B%" (Broken words)
    System.out.println("12. Search No, Solr Read Delay, Solr Records Returned, PG Read Delay, PG Records Returned");
    for(int i=0; i<10; i++ ) {
      PerfData perfDataS = perfTests.get(0).readData("queryText:*RDER B*", 10);
      PerfData perfDataP = perfTests.get(1).readData("queryText LIKE '%RDER B%'", 10);

      System.out.printf("%d, %d, %d, %d, %d\n", i, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);
    }
    // -- Pattern match --
    // 13. Query text containing pattern - "%RDER%" (Broken word)
    System.out.println("13. Search No, Solr Read Delay, Solr Records Returned, PG Read Delay, PG Records Returned");
    for(int i=0; i<10; i++ ) {
      PerfData perfDataS = perfTests.get(0).readData("queryText:*RDER*", 10);
      PerfData perfDataP = perfTests.get(1).readData("queryText LIKE '%RDER%'", 10);

      System.out.printf("%d, %d, %d, %d, %d\n", i, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);
    }

    // -- Multiple condition ---
    // 14. DAGs that took more than 5 min in the last 24 hours
    System.out.println("14. Search No, Solr Read Delay, Solr Records Returned, PG Read Delay, PG Records Returned");
    for(int i=1; i<10; i++ ) {
      PerfData perfDataS = perfTests.get(0).readData(String.format("timeTaken:[300000 TO *] AND startTime:[NOW-1DAY TO NOW]"), 10);
      PerfData perfDataP = perfTests.get(1).readData(String.format("timeTaken > 300000 AND startTime > now() - interval '1 day'"), 10);

      System.out.printf("%d, %d, %d, %d, %d\n", i, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);
    }

    // 15. 4 & 6
    System.out.println("15. Search No, Solr Read Delay, Solr Records Returned, PG Read Delay, PG Records Returned");
    for(int i=0; i<10; i++ ) {
      PerfData perfDataS = perfTests.get(0).readData(String.format("status:(KILLED FAILED) AND appID:application_%d", i), 10);
      PerfData perfDataP = perfTests.get(1).readData(String.format("status IN ('KILLED', 'FAILED') AND appID = 'application_%s'", i), 10);

      System.out.printf("%d, %d, %d, %d, %d\n", i, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);
    }

    // 16. 5 & 9 & 11
    System.out.println("16. Search No, Solr Read Delay, Solr Records Returned, PG Read Delay, PG Records Returned");
    for(int i=1; i<10; i++ ) {
      PerfData perfDataS = perfTests.get(0).readData(String.format("tablesWritten:table_%d AND timeTaken:[%d TO *] AND queryText:\"ORDER BY\"", i, i*2000), 10);
      PerfData perfDataP = perfTests.get(1).readData(String.format("'table_%d' = ANY(tablesWritten) AND timeTaken > %d AND queryText LIKE '%s'", i, i*2000, "%ORDER BY%"), 10);

      System.out.printf("%d, %d, %d, %d, %d\n", i, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);
    }

    // 17. 1 & 5 & 7 & 9 & 11
    System.out.println("17. Search No, Solr Read Delay, Solr Records Returned, PG Read Delay, PG Records Returned");
    for(int i=1; i<10; i++ ) {
      PerfData perfDataS = perfTests.get(0).readData(String.format("user:userName%d AND tablesWritten:table_%d AND isDDL:%b AND timeTaken:[%d TO *] AND queryText:\"ORDER BY\"", i, i, i%2 == 0, i*2000), 10);
      PerfData perfDataP = perfTests.get(1).readData(String.format("userName = 'userName%d' AND 'table_%d' = ANY(tablesWritten) AND isDDL = %b AND timeTaken > %d AND queryText LIKE '%s'", i, i, i%2 == 0, i*2000, "%ORDER BY%"), 10);

      System.out.printf("%d, %d, %d, %d, %d\n", i, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);
    }

    // 18. 1 & 5 & 7 & 9 & 11 with pagination - 0 to 5 pages

    // 19. 1 & 5 & 7 & 9 & 11 with sorting - sort on duration DESC
    System.out.println("19. Search No, Solr Read Delay, Solr Records Returned, PG Read Delay, PG Records Returned");
    for(int i=0; i<5; i++ ) {
      SolrPerfTestImpl solrPerfTest = (SolrPerfTestImpl)perfTests.get(0);
      PerfData perfDataS = solrPerfTest.readDataWithSort(String.format("user:userName1 AND tablesWritten:table_1 AND isDDL:true AND timeTaken:[2000 TO *] AND queryText:\"CREATE\""), 10);
      PerfData perfDataP = perfTests.get(1).readData(String.format("userName = 'userName1' AND 'table_1' = ANY(tablesWritten) AND isDDL = true AND timeTaken > 2000 AND queryText LIKE '%s' ORDER BY timeTaken DESC", "%CREATE%"), 10);

      System.out.printf("%d, %d, %d, %d, %d\n", i, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);
    }

    // -- Others --
    // 20. How long does it take for the written records to reflect in the data searched.
    System.out.println("20. How long does it take for the written records to reflect");
    DagData dag = new DagData();

    List<DagData> list = new ArrayList<DagData>();
    list.add(dag);

    PerfData perfDataS, perfDataP;

    perfTests.get(0).writeData(list);
    perfDataS = perfTests.get(0).readData(String.format("id:%s", dag.dagID), 10);
    perfTests.get(1).writeData(list);
    perfDataP = perfTests.get(1).readData(String.format("dagID = '%s'", dag.dagID), 10);

    System.out.printf("%d, %d\n", perfDataS.data, perfDataP.data);

    // 21. Time taken for returning faceted data - Facet on user, tabes_written, database & status
    System.out.println("21. Search No, Solr Read Delay, Solr Records Returned, PG Read Delay, PG Records Returned");
    for(int i=0; i<5; i++ ) {
      perfDataS = perfTests.get(0).getFacet();
      perfDataP = perfTests.get(1).getFacet();

      System.out.printf("%d, %d, %d, %d, %d\n", i, perfDataS.getTotalDelay(), perfDataS.data, perfDataP.getTotalDelay(), perfDataP.data);
    }

    // 22. Loading non-indexed data blobs - Eg. dagPlan
    // 23. Search + Facet

  }

  public void readWriteTests(List<PerfTest> perfTests) throws InterruptedException, IOException, SolrServerException, SQLException {
  }

}
