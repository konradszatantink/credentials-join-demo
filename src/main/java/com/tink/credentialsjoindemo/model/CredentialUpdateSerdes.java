package com.tink.credentialsjoindemo.model;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public final class CredentialUpdateSerdes extends Serdes.WrapperSerde<CredentialUpdate> {

    public CredentialUpdateSerdes() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(CredentialUpdate.class));
    }

    public static Serde<CredentialUpdate> serdes() {
        JsonSerializer<CredentialUpdate> serializer = new JsonSerializer<>();
        JsonDeserializer<CredentialUpdate> deserializer = new JsonDeserializer<>(CredentialUpdate.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
