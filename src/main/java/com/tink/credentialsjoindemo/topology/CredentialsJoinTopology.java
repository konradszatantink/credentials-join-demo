package com.tink.credentialsjoindemo.topology;

import com.tink.credentialsjoindemo.model.CredentialSensitiveData;
import com.tink.credentialsjoindemo.model.CredentialUpdate;
import com.tink.credentialsjoindemo.model.CredentialUpdateSerdes;
import com.tink.credentialsjoindemo.model.CredentialsJoinedSerdes;
import com.tink.credentialsjoindemo.model.CredentialsSensitiveDataSerdes;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CredentialsJoinTopology {

    public static final String CREDS_UPDATE_TOPIC = "creds-update-topic";
    public static final String CREDS_SENSITIVE_DATA_TOPIC = "creds-sensitive-data-topic";
    public static final String CREDS_JOINED_TOPIC = "creds-joined-topic";
    public static final Serde<CredentialSensitiveData> CREDENTIAL_SENSITIVE_DATA_SERDE =
            CredentialsSensitiveDataSerdes.serdes();
    public static final Serde<CredentialUpdate> CREDENTIAL_UPDATE_SERDE = CredentialUpdateSerdes.serdes();
    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, CredentialUpdate> credsUpdateStream = streamsBuilder.stream(
                        CREDS_UPDATE_TOPIC, Consumed.with(STRING_SERDE, CREDENTIAL_UPDATE_SERDE))
                .selectKey((key, value) -> value.consentId().toString());

        KStream<String, CredentialSensitiveData> sensitiveDataStream = streamsBuilder.stream(
                        CREDS_SENSITIVE_DATA_TOPIC, Consumed.with(STRING_SERDE, CREDENTIAL_SENSITIVE_DATA_SERDE))
                .selectKey((key, value) -> value.consentId().toString());
        CredentialsValueJoiner credentialsValueJoiner = new CredentialsValueJoiner();

        credsUpdateStream
                .leftJoin(
                        sensitiveDataStream,
                        credentialsValueJoiner,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5))
                                .before(Duration.ZERO),
                        StreamJoined.with(STRING_SERDE, CREDENTIAL_UPDATE_SERDE, CREDENTIAL_SENSITIVE_DATA_SERDE))
                .to(CREDS_JOINED_TOPIC, Produced.with(STRING_SERDE, CredentialsJoinedSerdes.serdes()));
    }
}
