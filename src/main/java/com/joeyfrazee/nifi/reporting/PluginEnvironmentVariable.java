package com.joeyfrazee.nifi.reporting;

import java.util.Optional;

/**
 * Enum representing environment variables that can be used for configuration of default values.
 *
 * @author brightSPARK Labs
 */
public enum PluginEnvironmentVariable {
    /** The environment variable defining the page size for scrolling through the provenance repository. */
    PAGE_SIZE("PAGE_SIZE"),
    /** The environment variable defining how far back to look into the provenance repository to index provenance events. */
    MAXIMUM_HISTORY("MAXIMUM_HISTORY"),
    /** The environment variable defining the address for Elasticsearch. */
    ELASTICSEARCH_URL("ELASTICSEARCH_URL"),
    /** The environment variable defining the name of the Elasticsearch index. */
    ELASTICSEARCH_INDEX("ELASTICSEARCH_INDEX"),
    /** The environment variable defining the HTTP CA certificate SHA-256 fingerprint for Elasticsearch. */
    ELASTICSEARCH_CA_CERT_FINGERPRINT("ELASTICSEARCH_CA_CERT_FINGERPRINT"),
    /** The environment variable defining the username for Elasticsearch authentication. */
    ELASTICSEARCH_USERNAME("ELASTICSEARCH_USERNAME"),
    /** The environment variable defining the password for Elasticsearch authentication. */
    ELASTICSEARCH_PASSWORD("ELASTICSEARCH_PASSWORD"),
    ;

    /** The name of the environment variable within the system. */
    private final String name;

    /** The value associated with the environment variable within the system. */
    private final Optional<String> value;

    /** The prefix prepended to the names of the environment variables within the system. */
    private static final String ENV_VAR_PREFIX = "NIFI_PROVENANCE_REPORTING_";

    /**
     * Default constructor.
     *
     * @param name The name of the plugin environment variable enum to initialise.
     */
    PluginEnvironmentVariable(final String name) {
        this.name = ENV_VAR_PREFIX + name;
        final String rawValue = System.getenv(this.name);
        this.value = Optional.ofNullable(rawValue);
    }

    /**
     * Get the value of the environment variable in the system.
     *
     * @return The value of the environment variable, or null if it has no value.
     */
    public Optional<String> getValue() {
        return this.value;
    }

    /**
     * Get the name of the environment variable in the system.
     *
     * @return The name of the environment variable.
     */
    public String getName() {
        return this.name;
    }
}
