package com.tink.credentialsjoindemo.topology;

import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.*;

import com.tink.credentialsjoindemo.model.CredentialJoined;
import com.tink.credentialsjoindemo.model.CredentialSensitiveData;
import com.tink.credentialsjoindemo.model.CredentialUpdate;
import com.tink.credentialsjoindemo.model.CredentialUpdateSerdes;
import com.tink.credentialsjoindemo.model.CredentialsJoinedSerdes;
import com.tink.credentialsjoindemo.model.CredentialsSensitiveDataSerdes;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

class CredentialsJoinTopologyTest {

    public static final String CREDS_UPDATE_TOPIC = "creds-update-topic";
    public static final String CREDS_SENSITIVE_DATA_TOPIC = "creds-sensitive-data-topic";
    public static final String CREDS_JOINED_TOPIC = "creds-joined-topic";

    private final CredentialsJoinTopology credentialTopology = new CredentialsJoinTopology();

    @Test
    void test() throws InterruptedException {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        credentialTopology.buildPipeline(streamsBuilder);
        Topology topology = streamsBuilder.build();

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(
                DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(
                DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration);
        TestInputTopic<String, CredentialUpdate> credsUpdateTopic = topologyTestDriver.createInputTopic(
                CREDS_UPDATE_TOPIC,
                new StringSerializer(),
                CredentialUpdateSerdes.serdes().serializer());

        TestInputTopic<String, CredentialSensitiveData> credsSensitiveTopic = topologyTestDriver.createInputTopic(
                CREDS_SENSITIVE_DATA_TOPIC,
                new StringSerializer(),
                CredentialsSensitiveDataSerdes.serdes().serializer());

        TestOutputTopic<String, CredentialJoined> outputTopic = topologyTestDriver.createOutputTopic(
                CREDS_JOINED_TOPIC,
                new StringDeserializer(),
                CredentialsJoinedSerdes.serdes().deserializer());

        UUID consentId = UUID.randomUUID();
        Instant instant = Instant.now();

        credsUpdateTopic.pipeInput("1", new CredentialUpdate(consentId, "Update"), instant);
        credsSensitiveTopic.pipeInput(
                "1", new CredentialSensitiveData(consentId, "Sensitive"), instant.minus(4, ChronoUnit.SECONDS));
        credsSensitiveTopic.pipeInput(
                "1", new CredentialSensitiveData(consentId, "Sensitive22"), instant.plus(5, ChronoUnit.SECONDS));
        credsSensitiveTopic.pipeInput(
                "1",
                new CredentialSensitiveData(consentId, "Sensitive223"),
                Instant.now().plus(10, ChronoUnit.SECONDS));

        outputTopic.readKeyValuesToList().forEach(System.out::println);
    }
}
