# test data generator for streams join
This generator simply sends messages via SMF to 2 different topics: 

## Topics
```
solace/samples/0
solace/samples/1
```

## Content
Message content:
  `{"key": 26, "message":"message # 10 for key 26"} `

or with timestamp:
  `{"key": 26, "message":"message # 10 for key 26", "ts":1758792159360}`

Per default we have 100 keys and the message text simply has a counter to keep track how many messages were published with each key.

## Invocation: 
No timestamps
  `java -jar target/send-join-1.0-SNAPSHOT.jar localhost:55554 default default default`

With timestamps
  `java -jar target/send-join-1.0-SNAPSHOT.jar localhost:55554 default default default yes`
