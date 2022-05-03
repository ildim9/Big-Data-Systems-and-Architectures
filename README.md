# Big-Data-Systems-and-Architectures
The projects in this repository are about how to manage data with systems like Redis , MongoDB , Spark , Airflow and Kafka.
## Project 1 :  Airflow-Kafka Pipelines
### For the `first task` of the project the idea was to use  Airflow to define a small workflow that leverages BashOperator via Python Programming Language. 
More specificly :

- Subtask 1.1: Creates a string variable called “firstName” assigning to it the string that
corresponds to your first name with all letters in lowercase.
- Subtask 1.2: Creates a string variable called “lastName” assigning to it the string that
corresponds to you last name with all letters in lowercase.
- Subtask 2.1: It gets the firstName as input and outputs the same string with the first letter
capitalized.
- Subtask 2.2: It gets the lastName as input and outputs the same string with the first letter
capitalized.
- Subtask 3: Displays on screen a string that concatenates the firstName and the lastName
adding a space character in between.

### For the `second task` of the project the idea was to use Kafka-python package.
Create a `KafkaProducer` to publish messages in a topic called “clima”. Each message should be in JSON format, containing one value for a temperature and
one for a humidity reading (both integers, you can select names of your choice for the keys).
Each message should also contain a timestamp representing the time point during which the
measurements took place. Moreover, use the send() function to publish five example
messages (with values of your choice). Also create a KafkaConsumer that subscribes to the
respective topic and prints the whole history of messages on screen.

## Project 2 :  Redis - MongoDB
In this project the idea was to query web-scraped data from Greece's largest used Motorcycles Online Marketplace www.car.gr.
