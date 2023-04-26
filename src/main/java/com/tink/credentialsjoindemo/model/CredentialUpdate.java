package com.tink.credentialsjoindemo.model;

import java.util.UUID;

public record CredentialUpdate(UUID consentId, String credentialContent) {}
