package com.tink.credentialsjoindemo.model;

import java.util.UUID;

public record CredentialSensitiveData(UUID consentId, String sensitiveData) {}
