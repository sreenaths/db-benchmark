
CREATE TYPE status AS ENUM ('NEW', 'INITED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'KILLED', 'ERROR', 'TERMINATING', 'COMMITTING');

CREATE TABLE dag(

  -- Index using hash
  dagID varchar(255),
  appID varchar(255),
  appAttemptID varchar(255),
  callerContextID varchar(255),

  dagName varchar(255),
  queueName varchar(255),
  userName varchar(255),

  status status,

  tablesWritten text[],
  queryText text,
  isDDL boolean,

  -- B-tree
  submitTime timestamp ,
  startTime timestamp ,
  initTime timestamp ,
  finishTime timestamp,
  timeTaken bigint,

  -- Not indexed
  logURL varchar(1000),
  callerContextType varchar(255),

  amWebServiceVersion varchar(10),
  tezVersion varchar(10),

  dagPlan text,
  vertexNameIdMap text,
  diagnosticsTxt text,
  counters text,
  taskStats text,

  PRIMARY KEY(dagID)
);

-- Hash indexes
CREATE INDEX app_idx on dag USING hash (appId);
CREATE INDEX app_attempt_idx on dag USING hash (appAttemptID);
CREATE INDEX caller_context_idx on dag USING hash (callerContextID);

CREATE INDEX dag_name_idx on dag USING hash (dagName);
CREATE INDEX queue_name_idx on dag USING hash (queueName);
CREATE INDEX user_name_idx on dag USING hash (dagName);

CREATE INDEX status_idx on dag USING hash (status);

CREATE INDEX tables_written_idx on dag USING hash (tablesWritten);
CREATE INDEX query_text_idx on dag USING hash (queryText);
CREATE INDEX is_DDL_idx on dag USING hash (isDDL);

-- BTree indexes
CREATE INDEX submit_time_idx on dag USING btree (submitTime);
CREATE INDEX start_time_idx on dag USING btree (startTime);
CREATE INDEX init_time_idx on dag USING btree (initTime);
CREATE INDEX finish_time_idx on dag USING btree (finishTime);
CREATE INDEX time_time_idx on dag USING btree (timeTaken);
