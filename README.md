# Data Engineer Challenge

This project was created as a solution for the Data Engineer Challenge, a task by the TDA team. The report will provide an overview of real-time data processing from Kafka.

The project is written in Python. The app consumes messages from the Kafka topic, counts unique users and then writes the results to the new topic.


### Prerequisites

If you want to run an application, you need to have it installed:
- Python 3+
- pip
- virtualenv
- Kafka

You can install Python on Linux by running this command:
```
sudo apt-get install python3
```
You can check that Python is installed: 
```
python --version
```
Next, install pip:
```
sudo apt install python3-pip
```
Then, using the pip, you have to install virtualenv:
```
pip install virtualenv
```
Finally, it is also necessary to install Kafka.

Kafka is written in Java and Scala and requires jre 1.7 and above to run it. In this step, we will ensure Java is installed.
```
sudo apt-get update
sudo apt-get install default-jre
```
Next, install zookeeperd:
```
sudo apt-get install zookeeperd
```
You can now check if Zookeeper is alive and if it’s OK:
```
telnet localhost 2181
```
at Telnet prompt, enter:
```
ruok
```
If it’s all okay it will end telnet session and reply with:
```
imok
```

Now, we will download Kafka:
```
cd ~
wget "https://www.apache.org/dyn/closer.cgi?path=/kafka/2.2.0/kafka_2.11-2.2.0.tgz"
wget "https://www.apache.org/dist/kafka/2.2.0/kafka_2.11-2.2.0.tgz.asc"
```

Optionally check the integrity of the downloaded file:
```
curl http://kafka.apache.org/KEYS | gpg --import
gpg --verify kafka_2.12-1.0.1.tgz.asc kafka_2.12-1.0.1.tgz
```

Create a directory and extract Kafka:
```
sudo mkdir /opt/kafka
sudo tar -xvzf kafka_2.11-2.2.0.tgz --directory /opt/kafka --strip-components 1
rm -rf kafka_2.12-1.0.1.tgz kafka_2.12-1.0.1.tgz.asc
```

If everything's fine right now, we can start Kafka server:
```
sudo /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
```

To run the application it is necessary to create two topics: one for input raw data that we will consume and second one in which we will produce the results:
```
/opt/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic in
/opt/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic out
```
Finally, download test data from [link](http://tx.tamedia.ch.s3.amazonaws.com/challenge/data/stream.jsonl.gz) and produce them into the topic in:
```
zcat stream.gz | kafka-console-producer --broker-list localhost:9092 --topic in
```
The app uses this data, so it is necessary to extract the file into a subdirectory within the project:
```
zcat stream.jsonl.gz > /path-to-project-directory/kafka-benchmark/data/stream.jsonl
```

## Running the app

After cloning the project, it's necessary to do the following:

Jump into the project directory:
```
cd kafka-benchmark/
```

Activate virtual environment:
```
source env/bin/activate
```

Install the required packages:
```
pip install --upgrade -r requirements.txt
```

Run the app:
```
python app.py
```


## Report

With the application comes a report explaining how I decided to implement the solution in this way. It contains benchmark of various Kafka clients, data structures, and counting mechanisms.

The report is written in the Jupiter Notebook and you can access it [here](https://github.com/DimitrijeM/kafka-benchmark/blob/master/kafka_benchmark_report.ipynb). If you want to run a code in notebook, you can do this by running a notebook in a virtual environment:

```
source env/bin/activate
ipython kernel install --user --name=env
jupyter notebook kafka_benchmark_report.ipynb
```


## Author

* **Dimitrije Milenković** - [Github](https://github.com/DimitrijeM/), [Linkedin](https://www.linkedin.com/in/dimitrijemilenkovicdm/)


