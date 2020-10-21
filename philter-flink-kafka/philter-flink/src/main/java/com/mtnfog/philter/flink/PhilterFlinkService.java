package com.mtnfog.philter.flink;

import com.mtnfog.philter.flink.functions.FilterMapFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.File;
import java.util.Properties;

public class PhilterFlinkService {

    private static final Logger LOGGER = LogManager.getLogger(PhilterFlinkService.class);

    private static final String SSL_TRUSTSTORE_TYPE = "ssl.truststore.type";
    private static final String SECURITY_PROTOCOL = "kafka.security.protocol";
    private static final String SSL_TRUSTSTORE_LOCATION = "kafka.ssl.truststore.location";
    private static final String SSL_TRUSTSTORE_PASSWORD = "kafka.ssl.truststore.password";
    private static final String SSL_KEYSTORE_LOCATION = "kafka.ssl.keystore.location";
    private static final String SSL_KEYSTORE_PASSWORD = "kafka.ssl.keystore.password";
    private static final String SSL_KEY_PASSWORD = "kafka.ssl.key.password";
    private static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM = "ssl.endpoint.identification.algorithm";
    private static final String SSL_CIPHER_SUITES = "ssl.cipher.suites";
    private static final String SSL_ENABLED_PROTOCOLS = "ssl.enabled.protocols";

    private Properties applicationProperties;

    public static void main(String[] args) throws Exception {

        final PhilterFlinkService philterFlinkService = new PhilterFlinkService();
        philterFlinkService.execute(new File("application-ssl.properties"));

    }

    public void execute(File propertiesFile) throws Exception {

        LOGGER.info("Loading properties.");
        final ParameterTool parameterTool = ParameterTool.fromPropertiesFile(propertiesFile.getAbsolutePath());
        applicationProperties = parameterTool.getProperties();

        // Register the parameters globally.
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().setGlobalJobParameters(parameterTool);

        final FlinkKafkaConsumer<String> flinkKafkaConsumer = getConsumer();
        final FlinkKafkaProducer<String> flinkKafkaProducer = getProducer();

        final DataStream<String> stream = environment.addSource(flinkKafkaConsumer);

        LOGGER.info("Starting Philter Flink job.");

        stream
                .map(new FilterMapFunction("https://127.0.0.1:8080", "context", "default"))
                .addSink(flinkKafkaProducer);

        environment.execute();

    }

    private FlinkKafkaProducer<String> getProducer() {

        final String brokers = applicationProperties.get("kafka.brokers").toString();
        final String outboundTopic = applicationProperties.get("kafka.topic.outbound").toString();

        final Properties kafkaProducerProperties = new Properties();

        kafkaProducerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProducerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        kafkaProducerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Defaults to allowing self-signed certificates.
        kafkaProducerProperties.put("ssl.endpoint.identification.algorithm", "");

        // If kafka.security.protocol is anything other than PLAINTEXT enable SSL.
        if(!StringUtils.equalsAnyIgnoreCase(applicationProperties.getProperty("kafka." + SECURITY_PROTOCOL, "PLAINTEXT"), "PLAINTEXT")) {

            kafkaProducerProperties.put(SECURITY_PROTOCOL, applicationProperties.getProperty("kafka." + SECURITY_PROTOCOL));
            kafkaProducerProperties.put(SSL_TRUSTSTORE_LOCATION, applicationProperties.getProperty("kafka." + SSL_TRUSTSTORE_LOCATION));
            kafkaProducerProperties.put(SSL_TRUSTSTORE_PASSWORD, applicationProperties.getProperty("kafka." + SSL_TRUSTSTORE_PASSWORD));
            kafkaProducerProperties.put(SSL_KEYSTORE_LOCATION, applicationProperties.getProperty("kafka." + SSL_KEYSTORE_LOCATION));
            kafkaProducerProperties.put(SSL_KEYSTORE_PASSWORD, applicationProperties.getProperty("kafka." + SSL_KEYSTORE_PASSWORD));
            kafkaProducerProperties.put(SSL_KEY_PASSWORD, applicationProperties.getProperty("kafka." + SSL_KEY_PASSWORD));

            kafkaProducerProperties.put(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, applicationProperties.getProperty("kafka." + SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "HTTPS"));
            kafkaProducerProperties.put(SSL_CIPHER_SUITES, applicationProperties.getProperty("kafka." + SSL_CIPHER_SUITES, ""));
            kafkaProducerProperties.put(SSL_ENABLED_PROTOCOLS, applicationProperties.getProperty("kafka." + SSL_ENABLED_PROTOCOLS, "TLSv1.2,TLSv1.1,TLSv1"));
            kafkaProducerProperties.put(SSL_TRUSTSTORE_TYPE, applicationProperties.getProperty("kafka." + SSL_TRUSTSTORE_TYPE, "JKS"));

        }

        LOGGER.info("Creating Kafka producer: {}, {}", brokers, outboundTopic);

        return new FlinkKafkaProducer<>(outboundTopic, new SimpleStringSchema(), kafkaProducerProperties);

    }

    private FlinkKafkaConsumer<String> getConsumer() {

        final String inboundTopic = applicationProperties.get("kafka.topic.inbound").toString();
        final String brokers = applicationProperties.get("kafka.brokers").toString();
        final String consumerGroup = applicationProperties.get("kafka.group.id").toString();

        final Properties kafkaConsumerProperties = new Properties();

        kafkaConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        kafkaConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, applicationProperties.getProperty("kafka.auto.offset.reset", "earliest"));

        // Defaults to allowing self-signed certificates.
        kafkaConsumerProperties.put("ssl.endpoint.identification.algorithm", "");

        // If kafka.security.protocol is anything other than PLAINTEXT enable SSL.
        if(!StringUtils.equalsAnyIgnoreCase(applicationProperties.getProperty("kafka." + SECURITY_PROTOCOL, "PLAINTEXT"), "PLAINTEXT")) {

            LOGGER.info("Configuring Kafka client for SSL");

            kafkaConsumerProperties.put(SECURITY_PROTOCOL, applicationProperties.getProperty("kafka." + SECURITY_PROTOCOL));
            kafkaConsumerProperties.put(SSL_TRUSTSTORE_LOCATION, applicationProperties.getProperty("kafka." + SSL_TRUSTSTORE_LOCATION));
            kafkaConsumerProperties.put(SSL_TRUSTSTORE_PASSWORD, applicationProperties.getProperty("kafka." + SSL_TRUSTSTORE_PASSWORD));
            kafkaConsumerProperties.put(SSL_KEYSTORE_LOCATION, applicationProperties.getProperty("kafka." + SSL_KEYSTORE_LOCATION));
            kafkaConsumerProperties.put(SSL_KEYSTORE_PASSWORD, applicationProperties.getProperty("kafka." + SSL_KEYSTORE_PASSWORD));
            kafkaConsumerProperties.put(SSL_KEY_PASSWORD, applicationProperties.getProperty("kafka." + SSL_KEY_PASSWORD));

            kafkaConsumerProperties.put(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, applicationProperties.getProperty("kafka." + SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "HTTPS"));
            kafkaConsumerProperties.put(SSL_CIPHER_SUITES, applicationProperties.getProperty("kafka." + SSL_CIPHER_SUITES, ""));
            kafkaConsumerProperties.put(SSL_ENABLED_PROTOCOLS, applicationProperties.getProperty("kafka." + SSL_ENABLED_PROTOCOLS, "TLSv1.2,TLSv1.1,TLSv1"));
            kafkaConsumerProperties.put(SSL_TRUSTSTORE_TYPE, applicationProperties.getProperty("kafka." + SSL_TRUSTSTORE_TYPE, "JKS"));

        }

        LOGGER.info("Creating Kafka consumer: {}, {}, {}", brokers, inboundTopic, consumerGroup);

        return new FlinkKafkaConsumer<>(inboundTopic, new SimpleStringSchema(), kafkaConsumerProperties);

    }

}
