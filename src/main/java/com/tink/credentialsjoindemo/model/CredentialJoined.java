package com.tink.credentialsjoindemo.model;

import java.util.UUID;

public record CredentialJoined(UUID consentId, String credentialContent, String sensitiveData) {}
