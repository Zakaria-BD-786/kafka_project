package com.kafka.project;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class UberProducerSynchronous {

	public static void main(String []args) throws IOException, InterruptedException{

		final Logger logger = LoggerFactory.getLogger(UberProducerSynchronous.class);
		String bootstrapservers = "127.0.0.1:9092";	

		//topic to which records will be published to
		String topic = "uber_data"; 

		//actual stream of data to be read from file
		String fileName = "C:/Users/md.zakaria.barbhuiya/Desktop/uber.csv";


		if (args.length == 2) {
			topic = args[1];
			fileName = args[0];
		} else {
			System.out.println("Using hard coded parameters unless you specify the topic and file <file topic>   ");
		}


		// create properties	
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapservers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");







		//create producer
		KafkaProducer <String,String> producer = new KafkaProducer<String,String>(properties);


		//create producer record

		/* Each line from the file will be read as one record to the topic */

		logger.info("Sending message to topic: " +topic);
		File f = new File(fileName);
		FileReader fr = new FileReader(f);
		BufferedReader reader = new BufferedReader(fr);
		String line = reader.readLine();
		line = reader.readLine(); //skip header

		while (line != null) {

			ProducerRecord<String,String> record = new ProducerRecord<String,String>(topic,line);

			//send record(synchronous)

			try{
				RecordMetadata metadata = producer.send(record).get();
				logger.info("Sent message: " +line);
				logger.info("Topic: " +metadata.topic() + "\n Partition: " +metadata.partition() + "\n Offset: " +metadata.offset()
				+ "\n TimeStamp: " +metadata.timestamp());
				line = reader.readLine(); 
				Thread.sleep(600);
			}catch (Exception e) {
				e.printStackTrace();
				logger.error("SynchronousProducer failed with an exception",e);
			}


		}

		producer.close();



	}
}
