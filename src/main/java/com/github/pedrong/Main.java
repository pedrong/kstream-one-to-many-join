package com.github.pedrong;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Example usage of {@link OneToManyJoinTransformer}.
 * This is not a production ready code! Do not copy and paste without a good sanitization and Clean Code reading.
 *
 * @author Pedro Gontijo
 */
public class Main {

    final static Logger log = LoggerFactory.getLogger(Main.class);

    final static String EMPLOYEES_TOPIC = "TEST-EMPLOYEES";
    final static String DEPARTMENT_TOPIC = "TEST-DEPARTMENTS"; // a.k.a. input
    final static String STREAM_OUTPUT_TOPIC = "TEST-OUTPUT";

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "master.fanflow-kafka.service.us-east-1.dynamic.dev.frgcloud.com:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-one-to-many-join");
        properties.put("key.serializer", LongSerializer.class);
        properties.put("value.serializer", JsonSerializer.class);

        Main main = new Main();

        main.loadEmployeeData(EMPLOYEES_TOPIC, properties);
        main.startInputDataPublisher(DEPARTMENT_TOPIC, properties);
        main.startStream(properties);
    }


    final ObjectMapper objectMapper = new ObjectMapper();

    private void loadEmployeeData(final String topic, Properties publisherProperties) throws IOException {

        log.info("Loading employee data to topic/table.");

        final Producer<Long, JsonNode> kafkaProducer = new KafkaProducer<>(publisherProperties);
        final JsonNode employeesJson = objectMapper.readTree(this.getClass().getResourceAsStream("/employee_data.json"));

        log.info("{} employees found.", employeesJson.size());

        employeesJson.forEach(employeeJson -> {
            try {
                kafkaProducer.send(new ProducerRecord<>(topic, employeeJson.get("id").asLong(), employeeJson)).get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private void startInputDataPublisher(final String topic, Properties publisherProperties) throws IOException {

        JsonNode departmentRootJson = objectMapper.readTree(this.getClass().getResourceAsStream("/input_data.json"));

        List<ProducerRecord> records = new ArrayList<>();

        departmentRootJson.forEach(departmentJson -> {
            try {
                // Reads all departments, creates kafka records and add them to list
                records.add(new ProducerRecord<>(topic, departmentJson.get("department_id").asLong(), departmentJson));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        log.info("{} departments found.", records.size());

        final Producer<Long, JsonNode> kafkaProducer = new KafkaProducer<>(publisherProperties);

        // Sends a random department record every 10s

        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                log.info("Sending new department.");
                kafkaProducer.send(records.get((int) (Math.random() * 100)));
            }
        };

        Timer timer = new Timer();
        timer.schedule(timerTask, 20000,10000);
    }

    private void startStream(Properties properties) {

        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer());

        final StreamsBuilder builder = new StreamsBuilder();

        final GlobalKTable<Long, JsonNode> employeesGlobalKTable = builder.globalTable(EMPLOYEES_TOPIC,
                Consumed.with(Serdes.Long(), jsonSerde), Materialized.as("test-employees-state-store"));

        builder
                // Create stream reading all departments
                .stream(DEPARTMENT_TOPIC, Consumed.with(Serdes.Long(), jsonSerde))
                // Log
                .peek((k, v) -> log.info("BEFORE key: {} - value: {}", k, v))
                // Joins the departments and employees
                .transform(() -> new OneToManyJoinTransformer<>(employeesGlobalKTable,
                        // KeyValueMapper which should return a collection of ids
                        (departmentId, departmentJson) -> {
                            final List<Long> employeeIds = new ArrayList<>();
                            departmentJson.get("employees").forEach(employeeFromDep -> employeeIds.add(employeeFromDep.get("employee_id").longValue()));
                            return employeeIds;
                        },
                        // ValueJoiner which should join the current value with the elements found in the KTable
                        (departmentJson, foundEmployees) -> {
                            ((ObjectNode) departmentJson).putArray("employees").addAll(foundEmployees);
                            return departmentJson;
                        }, true, true))
                // Logs after the join
                .peek((k, v) -> log.info("AFTER key: {} - value: {}", k, v))
                .to(STREAM_OUTPUT_TOPIC, Produced.with(Serdes.Long(), jsonSerde));

        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        kafkaStreams.start();
    }

}
