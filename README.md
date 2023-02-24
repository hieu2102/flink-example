# Flink Example Job

sample code && deployment for Flink

this job reads messages from Kafka and publish a modified message (add "modified value:" to message's value)

# Running Flink Jobs in IDE

for Intellij
`Run` -> `Edit Configurations` -> `Modify options...` -> `Java` -> `Add dependencies with "Provided" scope to classpath`

# Setup Environment

## Start Flink && Kafka

```bash 
docker-compose up -d
```

## Compile Flink Job

```bash 
mvn clean compile assembly:single
```

## Deploy Jar

Using GUI at http://localhost:8081

at tab `Submit New Job` -> `Add New`

# Use

## Submit New Job

Using GUI at http://localhost:8081

at tab `Submit New Job` -> click on Jar Name -> `Submit`

override the job's default ENV values using `Program Arguments`

specify ENV key and value as you would when running a jar file from terminal:

`--${ENV_KEY1} ${ENV_VALUE1} --${ENV_KEY2} ${ENV_VALUE2}`

## Publish Messages to Source Topic

```bash 
echo "hello there" | kcat -b localhost:9092 -P -t transactions -H "header1=header value" -H "nullheader" -H "emptyheader=" -H "header1=duplicateIsOk"
echo "general Kenobi" | kcat -b localhost:9092 -P -t transactions -H "header1=header value" -H "nullheader" -H "emptyheader=" -H "header1=duplicateIsOk
```

## Consume Messages from Sink Topic

```bash 
kcat -b localhost:9092 -t fraud -C
```

```plaintext
modified value:hello there
% Reached end of topic fraud [0] at offset 3
modified value:general Kenobi
% Reached end of topic fraud [0] at offset 4
```