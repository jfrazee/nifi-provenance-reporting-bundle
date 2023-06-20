package com.joeyfrazee.nifi.reporting;

/**
 * Enum representing environment variables that can be used for configuration of default values.
 *
 * @author brightSPARK Labs
 */
public enum EnvironmentVariable {
    PAGE_SIZE("PAGE_SIZE"),
    MAXIMUM_HISTORY("MAXIMUM_HISTORY"),
    ELASTICSEARCH_URL("ELASTICSEARCH_URL"),
    ELASTICSEARCH_INDEX("ELASTICSEARCH_INDEX"),
    ELASTICSEARCH_CA_CERT_FINGERPRINT("ELASTICSEARCH_CA_CERT_FINGERPRINT"),
    ELASTICSEARCH_USERNAME("ELASTICSEARCH_USERNAME"),
    ELASTICSEARCH_PASSWORD("ELASTICSEARCH_PASSWORD"),
    ;

    private final String name;

    EnvironmentVariable(final String name) {
        this.name = name;
    }

    /**
     * Get the value of the environment variable from the system.
     *
     * @return The value of the environment variable, or null if it has no value.
     */
    public String getValue() {
        return System.getenv(name);
    }
}
