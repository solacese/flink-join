# test data generator for streams join
This generator simply sends messages via SMF to 2 different topics: 

## Topics
```
solace/samples/0
solace/samples/1
[solace/samples/2]
...
```
Depends on the value `MAX_STREAMS`. 

## Content
Message content:
  `{"key": 26, "message":"message # 10 for key 26"} `

or with timestamp:
  `{"key": 26, "message":"message # 10 for key 26", "ts":1758792159360}`

Then number of input streams as well as the number of keys can only be changed in the source file. (Plus the message rate). 

## Invocation: 
No timestamps
  `java -jar target/join-data-generator-1.0-SNAPSHOT.jar localhost:55554 default default default`

With timestamps
  `java -jar target/join-data-generator-1.0-SNAPSHOT.jar localhost:55554 default default default yes`
