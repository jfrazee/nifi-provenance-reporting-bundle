package com.joeyfrazee.nifi.reporting;

import jakarta.mail.*;
import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeUtility;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.ReportingContext;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Tags({"email", "provenance", "smtp"})
@CapabilityDescription("Sends an e-mail when a provenance event is considered as an error")
public class EmailProvenanceReporter extends AbstractProvenanceReporter {

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
        descriptors.add(CONTENT_TYPE);
        descriptors.add(FROM);
        descriptors.add(TO);
        descriptors.add(CC);
        descriptors.add(BCC);
        descriptors.add(INPUT_CHARACTER_SET);

        return descriptors;
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

    private void setMessageHeader(final String header, final String value, final Message message) throws MessagingException {
        try {
            message.setHeader(header, MimeUtility.encodeText(value));
        } catch (UnsupportedEncodingException e) {
            getLogger().warn("Unable to add header {} with value {} due to encoding exception", header, value);
        }
    }

    /**
     * Based on the input properties, determine whether an authenticate or unauthenticated session should be used.
     * If authenticated, creates a Password Authenticator for use in sending the email.
     */
    private Session createMailSession(final Properties properties, ReportingContext context) {
        final boolean auth = Boolean.parseBoolean(context.getProperty(SMTP_AUTH).getValue());

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
     * @param propertyDescriptor the property to evaluate
     * @return an InternetAddress[] parsed from the supplied property
     * @throws AddressException if the property cannot be parsed to a valid InternetAddress[]
     */
    private InternetAddress[] toInetAddresses(final ReportingContext context, PropertyDescriptor propertyDescriptor)
            throws AddressException {
        InternetAddress[] parse;
        final String value = context.getProperty(propertyDescriptor).getValue();
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
     *
     * @param context the ProcessContext
     * @return the Charset
     */
    private Charset getCharset(final ReportingContext context) {
        return Charset.forName(context.getProperty(INPUT_CHARACTER_SET).getValue());
    }

    private String composeMessageContent(final Map<String, Object> event) {
        final StringBuilder message = new StringBuilder();

        message.append("Affected processor:\n")
            .append("\tProcessor name: ").append(event.get("component_name")).append("\n")
            .append("\tProcessor type: ").append(event.get("component_type")).append("\n")
            .append("\tProcess group: ").append(event.get("process_group_name")).append("\n")
            .append("\tURL: ").append(event.get("component_url")).append("\n");

        message.append("\n");
        message.append("Error information:\n")
            .append("\tDetails: ").append(event.get("details")).append("\n")
            .append("\tEvent type: ").append(event.get("event_type")).append("\n");

        if (event.containsKey("updatedAttributes")) {
            Map<String, String> updatedAttributes = (Map<String, String>) event.get("updatedAttributes");
            message.append("\nFlow file - Updated attributes:\n");
            updatedAttributes.keySet().stream().sorted().forEach(attributeName ->
                message.append(String.format("\t%1$s: %2$s\n", attributeName, updatedAttributes.get(attributeName)))
            );
        }

        if (event.containsKey("previousAttributes")) {
            Map<String, String> previousAttributes = (Map<String, String>) event.get("previousAttributes");
            message.append("\nFlow file - Previous attributes:\n");
            previousAttributes.keySet().stream().sorted().forEach(attributeName ->
                message.append(String.format("\t%1$s: %2$s\n", attributeName, previousAttributes.get(attributeName)))
            );
        }

        message.append("\nFlow file - content:\n")
            .append("\tDownload input: ").append(event.get("download_input_content_uri")).append("\n")
            .append("\tDownload output: ").append(event.get("download_output_content_uri")).append("\n")
            .append("\tView input: ").append(event.get("view_input_content_uri")).append("\n")
            .append("\tView output: ").append(event.get("view_output_content_uri")).append("\n");

        message.append("\n");
        return message.toString();
    }

    @Override
    public void indexEvent(final Map<String, Object> event, final ReportingContext context) {
        try {
            // Send the email message only if it is an error event
            if (event.containsKey("status") && event.get("status").equals("Error")) {
                sendErrorEmail(event, context);
            }
        } catch (MessagingException e) {
            getLogger().error("Error sending error email: " + e.getMessage(), e);
        }
    }

    public void sendErrorEmail(Map<String, Object> event, ReportingContext context) throws MessagingException {

        String emailSubject = "Error occurred in processor " + event.get("component_name") + " "
                + "in process group " + event.get("process_group_name");

        final Properties properties = new Properties();
        final Session mailSession = this.createMailSession(properties, context);
        final Message message = new MimeMessage(mailSession);

        try {
            message.addFrom(toInetAddresses(context, FROM));
            message.setRecipients(Message.RecipientType.TO, toInetAddresses(context, TO));
            message.setRecipients(MimeMessage.RecipientType.CC, toInetAddresses(context, CC));
            message.setRecipients(Message.RecipientType.BCC, toInetAddresses(context, BCC));
            this.setMessageHeader("X-Mailer", context.getProperty(HEADER_XMAILER).getValue(), message);
            message.setSubject(emailSubject);

            final String messageText = composeMessageContent(event);

            final String contentType = context.getProperty(CONTENT_TYPE).getValue();
            final Charset charset = getCharset(context);

            message.setContent(messageText, contentType + String.format("; charset=\"%s\"", MimeUtility.mimeCharset(charset.name())));

            message.setSentDate(new Date());

            // message is not a Multipart, need to set Content-Transfer-Encoding header at the message level
            message.setHeader("Content-Transfer-Encoding", MimeUtility.getEncoding(message.getDataHandler()));

            // Send the message
            message.saveChanges();
            send(message);
            getLogger().debug("Error email for provenance event sent successfully!");
        } catch (MessagingException e) {
            getLogger().error("Failed to send error email for provenance event. Error details: " + e.getMessage());
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
