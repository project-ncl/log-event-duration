# Log event duration

Is a Kafka stream processor to enrich log events with the duration. 
The duration is calculated from two log events with a matching 'tag'.  

The "end" message is enriched with an extra field `operationTook` containing the duration in millis abd the string " Took [millis] ms." is appended to the log message. 
 
When "duration topic" is specified the enriched "end" messages are sent to the "duration topic".     

![schema](./log-event-duration.svg)

## Configuration
_kafkaPropertiesPath_: a path to kafka.properties file
_inputTopicName_: topic name from which the logs are read
_outputTopicName_: topic name where the enriched logs are written
_durationsTopicName_: optional topic name for enriched end messages only

## Log message format
Log messages must be in json format and contain the fields bellow, other fields are passed through. 
- process_stage_name
- process_stage_step (BEGIN|END)
- processContext
- @timestamp (uuuu-MM-dd'T'HH:mm:ss.SSSZ)
- message 

Example input messages:
```
{
  "@timestamp":"2020-12-27T15:30:00.000+0100",
  "message":"This important task started.",
  "mdc":{
    "process_stage_name:"importat_task",
    "process_stage_step:"BEGIN",
    "processContext:"12345"
  },
  [other passthrough fields]
}

{
  "@timestamp":"2020-12-27T15:30:15.000+0100",
  "message":"This important task completed.",
  "mdc":{
    "process_stage_name:"importat_task",
    "process_stage_step:"END",
    "processContext:"12345"
  },
  [other passthrough fields]
}

#enriched end message
{
  "@timestamp":"2020-12-27T15:30:15.000+0100",
  "message":"This important task completed. Took 15000 ms.",
  "mdc":{
    "process_stage_name:"importat_task",
    "process_stage_step:"END",
    "processContext:"12345"
  },
  "operationTook":15000,
  [other passthrough fields]
}
```
## Kafka partitions
The matching "BEGIN" and the "END" events must end in the same partition of the input topic,
ensure this make sure the Kafka message keys are properly set or use one partition only.
A stream processor is using "processContext" and "process_stage_name" to match the messages. 

## Running the application
TBD