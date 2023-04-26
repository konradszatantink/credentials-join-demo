package com.tink.credentialsjoindemo.model;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public final class CredentialsSensitiveDataSerdes extends Serdes.WrapperSerde<CredentialSensitiveData> {

    public CredentialsSensitiveDataSerdes() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(CredentialSensitiveData.class));
    }

    public static Serde<CredentialSensitiveData> serdes() {
        JsonSerializer<CredentialSensitiveData> serializer = new JsonSerializer<>();
        JsonDeserializer<CredentialSensitiveData> deserializer = new JsonDeserializer<>(CredentialSensitiveData.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
