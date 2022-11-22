package com.example.Test;

import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.exceptions.CsvException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.protocol.Message;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

@RestController
public class KafkaController {


    @GetMapping(value = "/producer")
    public String producer() throws IOException {
      
        ClassLoader classLoader = getClass().getClassLoader();

        URL resource = classLoader.getResource("Test.csv");

        List<ParserCSV> beans = new CsvToBeanBuilder(new FileReader(resource.getFile()))
                .withType(ParserCSV.class)
                .build()
                .parse();
        Properties props = new Properties();


        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");


        props.put("retries", 0);


        props.put("batch.size", 16384);


        props.put("linger.ms", 1);

        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        List<Long> Amounts = new ArrayList<>();

        for (ParserCSV test : beans) {
            if (test.getAmount() < 1000) {
                NewTopic newTopic1= new NewTopic("Topic1", 1, (short) 1);

                long amountPercent = (test.getAmount() * 10) / 100;
                Amounts.add(amountPercent);
                Producer<String, List<Long>> producerTopic1 = new KafkaProducer
                        <String, List<Long>>(props);


                producerTopic1.send(new ProducerRecord<String, List<Long>>("topic1",
                        Amounts));

                producerTopic1.close();
            } else if (test.getAmount() > 1000) {
                NewTopic newTopic2= new NewTopic("Topic2", 1, (short) 1);

                long amountConverted = test.getAmount() / 20;
                Amounts.add(amountConverted);
                Producer<String, List<Long>> producerTopic2 = new KafkaProducer
                        <String, List<Long>>(props);


                producerTopic2.send(new ProducerRecord<String, List<Long>>("topic2",
                        Amounts));

                producerTopic2.close();
            }

        }


        return "Message sent to the Kafka Topic  Successfully";


    }

    @GetMapping(value = "/starter-consumer")
    public String consumerSalaryStarter() throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList("topic1"));
        Set<String> values = new HashSet<>();
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records)
                    values.add(record.value());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        FileWriter writer = new FileWriter("topic1.txt");

        for (String result : values) {


            writer.write(Integer.parseInt(result + System.lineSeparator()));

            writer.close();
        }


        return "Message consumed from_topic Successfully";


    }

    @GetMapping(value = "/dollar-consumer")
    public String consumerInDollar() throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList("topic2"));
        Set<String> values = new HashSet<>();
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records)
                    values.add(record.value());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        FileWriter writer = new FileWriter("topic2.txt");

        for (String result : values) {


            writer.write(Integer.parseInt(result + System.lineSeparator()));

            writer.close();
        }


        return "Message consumed from_topic Successfully";


    }
}