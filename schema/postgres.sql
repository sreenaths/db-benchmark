
CREATE TYPE status AS ENUM ('NEW', 'INITED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'KILLED', 'ERROR', 'TERMINATING', 'COMMITTING');

CREATE TABLE dag(

  dagID varchar(255),
  appID varchar(255),
  appAttemptID varchar(255),
  callerContextID varchar(255),

  dagName varchar(255),
  queueName varchar(255),

  userName varchar(255),
  status status,
  logURL varchar(1000),
  callerContextType varchar(255),

  submitTime timestamp ,
  startTime timestamp ,
  initTime timestamp ,
  finishTime varchar(255),
  timeTaken interval,

  amWebServiceVersion varchar(100),
  tezVersion varchar(100),

  dagPlan text,
  vertexNameIdMap text,
  diagnosticsTxt text,
  counters text,
  taskStats text,

  PRIMARY KEY(dagID)

);