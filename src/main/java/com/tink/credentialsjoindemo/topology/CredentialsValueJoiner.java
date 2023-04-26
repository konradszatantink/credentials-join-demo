package com.tink.credentialsjoindemo.topology;

import com.tink.credentialsjoindemo.model.CredentialJoined;
import com.tink.credentialsjoindemo.model.CredentialSensitiveData;
import com.tink.credentialsjoindemo.model.CredentialUpdate;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class CredentialsValueJoiner
        implements ValueJoiner<CredentialUpdate, CredentialSensitiveData, CredentialJoined> {

    @Override
    public CredentialJoined apply(CredentialUpdate value1, CredentialSensitiveData value2) {
        return new CredentialJoined(value1.consentId(), value1.credentialContent(), value2.sensitiveData());
    }
}
