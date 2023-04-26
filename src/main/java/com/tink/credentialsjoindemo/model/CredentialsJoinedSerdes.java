package com.tink.credentialsjoindemo.model;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class CredentialsJoinedSerdes extends Serdes.WrapperSerde<CredentialJoined> {

    public CredentialsJoinedSerdes() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(CredentialJoined.class));
    }

    public static Serde<CredentialJoined> serdes() {
        JsonSerializer<CredentialJoined> serializer = new JsonSerializer<>();
        JsonDeserializer<CredentialJoined> deserializer = new JsonDeserializer<>(CredentialJoined.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
