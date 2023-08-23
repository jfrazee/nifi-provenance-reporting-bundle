package com.joeyfrazee.nifi.reporting;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.*;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.ReportingContext;

import javax.mail.*;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeUtility;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SupportsBatching
@Tags({"email", "provenance"})
@CapabilityDescription("Sends an e-mail when a provenance event is considered as an error")
@SupportsSensitiveDynamicProperties
@DynamicProperty(name = "mail.propertyName",
        value = "Value for a specific property to be set in the JavaMail Session object",
        description = "Dynamic property names that will be passed to the Mail session. " +
                "Possible properties can be found in: https://javaee.github.io/javamail/docs/api/com/sun/mail/smtp/package-summary.html.",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "The entirety of the FlowFile's content (as a String object) "
        + "will be read into memory in case the property to use the flow file content as the email body is set to true.")
public class EmailProvenanceReporter extends AbstractProvenanceReporter {

    private static final Pattern MAIL_PROPERTY_PATTERN = Pattern.compile("^mail\\.smtps?\\.([a-z0-9\\.]+)$");

    public static final PropertyDescriptor SMTP_HOSTNAME = new PropertyDescriptor.Builder()
            .name("SMTP Hostname")
            .description("The hostname of the SMTP host")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SMTP_PORT = new PropertyDescriptor.Builder()
            .name("SMTP Port")
            .description("The Port used for SMTP communications")
            .required(true)
            .defaultValue("25")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor SMTP_USERNAME = new PropertyDescriptor.Builder()
            .name("SMTP Username")
            .description("Username for the SMTP account")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();


    public static final PropertyDescriptor SMTP_PASSWORD = new PropertyDescriptor.Builder()
            .name("SMTP Password")
            .description("Password for the SMTP account")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor SMTP_TLS = new PropertyDescriptor.Builder()
            .name("SMTP TLS")
            .displayName("SMTP STARTTLS")
            .description("Flag indicating whether Opportunistic TLS should be enabled using STARTTLS command")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor SMTP_AUTH = new PropertyDescriptor.Builder()
            .name("SMTP Auth")
            .description("Flag indicating whether authentication should be used")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor SMTP_SOCKET_FACTORY = new PropertyDescriptor.Builder()
            .name("SMTP Socket Factory")
            .description("Socket Factory to use for SMTP Connection")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("javax.net.ssl.SSLSocketFactory")
            .build();
    public static final PropertyDescriptor HEADER_XMAILER = new PropertyDescriptor.Builder()
            .name("SMTP X-Mailer Header")
            .description("X-Mailer used in the header of the outgoing email")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("NiFi")
            .build();
    public static final PropertyDescriptor ATTRIBUTE_NAME_REGEX = new PropertyDescriptor.Builder()
            .name("attribute-name-regex")
            .displayName("Attributes to Send as Headers (Regex)")
            .description("A Regular Expression that is matched against all FlowFile attribute names. "
                    + "Any attribute whose name matches the regex will be added to the Email messages as a Header. "
                    + "If not specified, no FlowFile attributes will be added as headers.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .required(false)
            .build();


    public static final PropertyDescriptor CONTENT_TYPE = new PropertyDescriptor.Builder()
            .name("Content Type")
            .description("Mime Type used to interpret the contents of the email, such as text/plain or text/html")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("text/plain")
            .build();
    public static final PropertyDescriptor FROM = new PropertyDescriptor.Builder()
            .name("From")
            .description("Specifies the Email address to use as the sender. "
                    + "Comma separated sequence of addresses following RFC822 syntax.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor TO = new PropertyDescriptor.Builder()
            .name("To")
            .description("The recipients to include in the To-Line of the email. "
                    + "Comma separated sequence of addresses following RFC822 syntax.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor CC = new PropertyDescriptor.Builder()
            .name("CC")
            .description("The recipients to include in the CC-Line of the email. "
                    + "Comma separated sequence of addresses following RFC822 syntax.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor BCC = new PropertyDescriptor.Builder()
            .name("BCC")
            .description("The recipients to include in the BCC-Line of the email. "
                    + "Comma separated sequence of addresses following RFC822 syntax.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor SUBJECT = new PropertyDescriptor.Builder()
            .name("Subject")
            .description("The email subject")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("Message from NiFi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MESSAGE = new PropertyDescriptor.Builder()
            .name("Message")
            .description("The body of the email message")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor INCLUDE_ALL_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Include All Attributes In Message")
            .description("Specifies whether or not all FlowFile attributes should be recorded in the body of the email message")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor INPUT_CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("input-character-set")
            .displayName("Input Character Set")
            .description("Specifies the character set of the FlowFile contents "
                    + "for reading input FlowFile contents to generate the message body "
                    + "or as an attachment to the message. "
                    + "If not set, UTF-8 will be the default value.")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue(StandardCharsets.UTF_8.name())
            .build();
    public static final PropertyDescriptor CONTENT_AS_MESSAGE = new PropertyDescriptor.Builder()
            .name("email-ff-content-as-message")
            .displayName("Flow file content as message")
            .description("Specifies whether or not the FlowFile content should be the message of the email. If true, the 'Message' property is ignored.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    /**
     * Mapping of the mail properties to the NiFi PropertyDescriptors that will be evaluated at runtime
     */
    private static final Map<String, PropertyDescriptor> propertyToContext = new HashMap<>();

    static {
        propertyToContext.put("mail.smtp.host", SMTP_HOSTNAME);
        propertyToContext.put("mail.smtp.port", SMTP_PORT);
        propertyToContext.put("mail.smtp.socketFactory.port", SMTP_PORT);
        propertyToContext.put("mail.smtp.socketFactory.class", SMTP_SOCKET_FACTORY);
        propertyToContext.put("mail.smtp.auth", SMTP_AUTH);
        propertyToContext.put("mail.smtp.starttls.enable", SMTP_TLS);
        propertyToContext.put("mail.smtp.user", SMTP_USERNAME);
        propertyToContext.put("mail.smtp.password", SMTP_PASSWORD);
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        descriptors = super.getSupportedPropertyDescriptors();
        descriptors.add(SMTP_HOSTNAME);
        descriptors.add(SMTP_PORT);
        descriptors.add(SMTP_USERNAME);
        descriptors.add(SMTP_PASSWORD);
        descriptors.add(SMTP_TLS);
        descriptors.add(SMTP_SOCKET_FACTORY);
        descriptors.add(HEADER_XMAILER);
        descriptors.add(ATTRIBUTE_NAME_REGEX);
        descriptors.add(CONTENT_TYPE);
        descriptors.add(FROM);
        descriptors.add(TO);
        descriptors.add(CC);
        descriptors.add(BCC);
        descriptors.add(SUBJECT);
        descriptors.add(MESSAGE);
        descriptors.add(INCLUDE_ALL_ATTRIBUTES);
        descriptors.add(INPUT_CHARACTER_SET);
        descriptors.add(CONTENT_AS_MESSAGE);

        return descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("SMTP property passed to the Mail Session")
                .required(false)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .addValidator(new DynamicMailPropertyValidator())
                .build();
    }

    private static class DynamicMailPropertyValidator implements Validator {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            final Matcher matcher = MAIL_PROPERTY_PATTERN.matcher(subject);
            if (!matcher.matches()) {
                return new ValidationResult.Builder()
                        .input(input)
                        .subject(subject)
                        .valid(false)
                        .explanation(String.format("[%s] does not start with mail.smtp", subject))
                        .build();
            }

            if (propertyToContext.containsKey(subject)) {
                return new ValidationResult.Builder()
                        .input(input)
                        .subject(subject)
                        .valid(false)
                        .explanation(String.format("[%s] overwrites standard properties", subject))
                        .build();
            }

            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(true)
                    .explanation("Valid mail.smtp property found")
                    .build();
        }
    }
    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> errors = new ArrayList<>(super.customValidate(context));

        final String to = context.getProperty(TO).getValue();
        final String cc = context.getProperty(CC).getValue();
        final String bcc = context.getProperty(BCC).getValue();

        if (to == null && cc == null && bcc == null) {
            errors.add(new ValidationResult.Builder().subject("To, CC, BCC").valid(false).explanation("Must specify at least one To/CC/BCC address").build());
        }

        return errors;
    }

    private final Pattern attributeNamePattern = null;


    private void setMessageHeader(final String header, final String value, final Message message) throws MessagingException {
        final ComponentLog logger = getLogger();
        try {
            message.setHeader(header, MimeUtility.encodeText(value));
        } catch (UnsupportedEncodingException e) {
            logger.warn("Unable to add header {} with value {} due to encoding exception", header, value);
        }
    }

    /**
     * Uses the mapping of javax.mail properties to NiFi PropertyDescriptors to build the required Properties object to be used for sending this email
     *
     * @param context  context
     * @param flowFile flowFile
     * @return mail properties
     */
    private Properties getMailPropertiesFromFlowFile(final ProcessContext context, final FlowFile flowFile) {
        final Properties properties = new Properties();

        for (final Map.Entry<String, PropertyDescriptor> entry : propertyToContext.entrySet()) {
            // Evaluate the property descriptor against the flow file
            final String flowFileValue = context.getProperty(entry.getValue()).evaluateAttributeExpressions(flowFile).getValue();
            final String property = entry.getKey();

            // Nullable values are not allowed, so filter out
            if (null != flowFileValue) {
                properties.setProperty(property, flowFileValue);
            }
        }

        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                final String mailPropertyValue = context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue();
                // Nullable values are not allowed, so filter out
                if (null != mailPropertyValue) {
                    properties.setProperty(descriptor.getName(), mailPropertyValue);
                }
            }
        }

        return properties;

    }

    /**
     * Based on the input properties, determine whether an authenticate or unauthenticated session should be used. If authenticated, creates a Password Authenticator for use in sending the email.
     *
     * @param properties mail properties
     * @return session
     */
    private Session createMailSession(final Properties properties) {
        final boolean auth = Boolean.parseBoolean(properties.getProperty("mail.smtp.auth"));

        /*
         * Conditionally create a password authenticator if the 'auth' parameter is set.
         */
        return auth ? Session.getInstance(properties, new Authenticator() {
            @Override
            public PasswordAuthentication getPasswordAuthentication() {
                final String username = properties.getProperty("mail.smtp.user");
                final String password = properties.getProperty("mail.smtp.password");
                return new PasswordAuthentication(username, password);
            }
        }) : Session.getInstance(properties); // without auth

    }

    /**
     * @param context            the current context
     * @param flowFile           the current flow file
     * @param propertyDescriptor the property to evaluate
     * @return an InternetAddress[] parsed from the supplied property
     * @throws AddressException if the property cannot be parsed to a valid InternetAddress[]
     */
    private InternetAddress[] toInetAddresses(final ProcessContext context, final FlowFile flowFile,
                                              PropertyDescriptor propertyDescriptor) throws AddressException {
        InternetAddress[] parse;
        final String value = context.getProperty(propertyDescriptor).evaluateAttributeExpressions(flowFile).getValue();
        if (value == null || value.isEmpty()) {
            if (propertyDescriptor.isRequired()) {
                final String exceptionMsg = "Required property '" + propertyDescriptor.getDisplayName() + "' evaluates to an empty string.";
                throw new AddressException(exceptionMsg);
            } else {
                parse = new InternetAddress[0];
            }
        } else {
            try {
                parse = InternetAddress.parse(value);
            } catch (AddressException e) {
                final String exceptionMsg = "Unable to parse a valid address for property '" + propertyDescriptor.getDisplayName() + "' with value '" + value + "'";
                throw new AddressException(exceptionMsg);
            }
        }
        return parse;
    }
    /**
     * Utility function to get a charset from the {@code INPUT_CHARACTER_SET} property
     * @param context the ProcessContext
     * @return the Charset
     */
    private Charset getCharset(final ProcessContext context) {
        return Charset.forName(context.getProperty(INPUT_CHARACTER_SET).getValue());
    }

    @Override
    public void indexEvent(Map<String, Object> event, ReportingContext context) throws IOException {
        // TODO
    }

    // TODO this does not apply to a reporting task, to be ported
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Properties properties = this.getMailPropertiesFromFlowFile(context, flowFile);
        final Session mailSession = this.createMailSession(properties);
        final Message message = new MimeMessage(mailSession);

        try {
            // Existing code to set up the email message
            message.addFrom(toInetAddresses(context, flowFile, FROM));
            message.setRecipients(Message.RecipientType.TO, toInetAddresses(context, flowFile, TO));
            message.setRecipients(MimeMessage.RecipientType.CC, toInetAddresses(context, flowFile, CC));
            message.setRecipients(Message.RecipientType.BCC, toInetAddresses(context, flowFile, BCC));
            if (attributeNamePattern != null) {
                for (final Map.Entry<String, String> entry : flowFile.getAttributes().entrySet()) {
                    if (attributeNamePattern.matcher(entry.getKey()).matches()) {
                        this.setMessageHeader(entry.getKey(), entry.getValue(), message);
                    }
                }
            }
            this.setMessageHeader("X-Mailer", context.getProperty(HEADER_XMAILER).evaluateAttributeExpressions(flowFile).getValue(), message);
            message.setSubject(context.getProperty(SUBJECT).evaluateAttributeExpressions(flowFile).getValue());


            // Send the message
            try {
                send(message);
                getLogger().debug("Provenance email sent successfully!");
            } catch (MessagingException e) {
                getLogger().error("Failed to send provenance email. Error details: " + e.getMessage());

                // Create a new email message for the error notification
                final Message errorEmail = new MimeMessage(mailSession);

                try {
                    // Set the sender and recipients of the error email
                    errorEmail.setFrom(new InternetAddress("your-email@example.com"));
                    errorEmail.setRecipients(Message.RecipientType.TO, InternetAddress.parse("recipient-email@example.com"));
                    errorEmail.setRecipients(Message.RecipientType.CC, InternetAddress.parse("recipient-email@example.com"));
                    errorEmail.setRecipients(Message.RecipientType.BCC, InternetAddress.parse("recipient-email@example.com"));

                    if (attributeNamePattern != null) {
                        for (final Map.Entry<String, String> entry : flowFile.getAttributes().entrySet()) {
                            if (attributeNamePattern.matcher(entry.getKey()).matches()) {
                                this.setMessageHeader(entry.getKey(), entry.getValue(), message);
                            }
                        }
                    }

                    // Set the subject and body of the error email
                    errorEmail.setSubject("Error in FlowFile Processing");
                    errorEmail.setText("An error occurred while processing the FlowFile: " + e.getMessage());

                    // Send the error email
                    send(errorEmail);
                } catch (MessagingException me) {
                    // Error occurred while sending the error email, handle or log the exception
                    getLogger().error("Error while sending email", me);
                }
            }
        } catch (MessagingException e) {
            getLogger().error("Error while sending email", e);
            throw new RuntimeException(e);
        }
    }
    /**
     * Wrapper for static method {@link Transport#send(Message)} to add testability of this class.
     *
     * @param msg the message to send
     * @throws MessagingException on error
     */
    protected void send(final Message msg) throws MessagingException {
        Transport.send(msg);
    }
}
