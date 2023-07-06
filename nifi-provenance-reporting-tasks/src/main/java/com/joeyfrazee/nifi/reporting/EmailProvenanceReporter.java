package com.joeyfrazee.nifi.reporting;


import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.*;
import java.util.regex.Pattern;
@SupportsBatching
@Tags({"email", "put", "notify", "smtp"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Sends an e-mail to configured recipients for each incoming FlowFile")
@SupportsSensitiveDynamicProperties
@DynamicProperty(name = "mail.propertyName",
        value = "Value for a specific property to be set in the JavaMail Session object",
        description = "Dynamic property names that will be passed to the Mail session. " +
                "Possible properties can be found in: https://javaee.github.io/javamail/docs/api/com/sun/mail/smtp/package-summary.html.",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "The entirety of the FlowFile's content (as a String object) "
        + "will be read into memory in case the property to use the flow file content as the email body is set to true.")
public abstract class EmailProvenanceReporter extends AbstractProvenanceReporter  {

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
    public static final AllowableValue PASSWORD_BASED_AUTHORIZATION_MODE = new AllowableValue(
            "password-based-authorization-mode",
            "Use Password",
            "Use password"
    );
    public static final AllowableValue OAUTH_AUTHORIZATION_MODE = new AllowableValue(
            "oauth-based-authorization-mode",
            "Use OAuth2",
            "Use OAuth2 to acquire access token"
    );
    public static final PropertyDescriptor AUTHORIZATION_MODE = new PropertyDescriptor.Builder()
            .name("authorization-mode")
            .displayName("Authorization Mode")
            .description("How to authorize sending email on the user's behalf.")
            .required(true)
            .allowableValues(PASSWORD_BASED_AUTHORIZATION_MODE, OAUTH_AUTHORIZATION_MODE)
            .defaultValue(PASSWORD_BASED_AUTHORIZATION_MODE.getValue())
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
            .dependsOn(AUTHORIZATION_MODE, PASSWORD_BASED_AUTHORIZATION_MODE)
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

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully sent will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that fail to send will be routed to this relationship")
            .build();

    private List<PropertyDescriptor> properties;

    private Set<Relationship> relationships;

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
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SMTP_HOSTNAME);
        properties.add(SMTP_PORT);
        properties.add(AUTHORIZATION_MODE);
        properties.add(SMTP_USERNAME);
        properties.add(SMTP_PASSWORD);
        properties.add(SMTP_AUTH);
        properties.add(SMTP_TLS);
        properties.add(SMTP_SOCKET_FACTORY);
        properties.add(FROM);
        properties.add(TO);
        properties.add(SUBJECT);
        properties.add(MESSAGE);

    /**
     * Based on the input properties, determine whether an authenticate or unauthenticated session should be used. If authenticated, creates a Password Authenticator for use in sending the email.
     *
     * @param properties mail properties
     * @return session
     */


    private Session createMailSession(final Properties properties) {
        final boolean auth = Boolean.parseBoolean(properties.getProperty("mail.smtp.auth"));

        private String hostname;
        private int port;
        private String fromAddress;
        private String toAddress;
        private String subject;

        public EmailProvenanceReporter(String hostname, int port, String fromAddress, String toAddress, String subject) {
            this.hostname = hostname;
            this.port = port;
            this.fromAddress = fromAddress;
            this.toAddress = toAddress;
            this.subject = subject;
        }

        @Override
        public void reportProvenance(String recepient ) {
            final String myAccountEmail = "your_email@example.com";
            final String password = "your_email_password";

            Properties properties = new Properties();
            properties.put("mail.smtp.auth", "true");
            properties.put("mail.smtp.starttls.enable", "true");
            properties.put("mail.smtp.host", hostname);
            properties.put("mail.smtp.port", port);

            Session session = Session.getInstance(properties, new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(myAccountEmail, password);
                }
            });
            Message message = prepareMessage(session, myAccountEmail, recepient);
        }
            private Message prepareMessage(Session session, String myAccountEmail, String recepient) {

            try {
                Message message = new MimeMessage(session);
                message.setFrom(new InternetAddress(myAccountEmail));
                message.setRecipients(Message.RecipientType.TO,  InternetAddress.parse(recepient));
                message.setSubject("Error in dataflow");
                message.setText("there is a error in dataflow");

                Transport.send(message);
                System.out.println("Provenance email sent successfully!");
            } catch (MessagingException e) {
                System.out.println("Failed to send provenance email. Error details: " + e.getMessage());
            }
/**
 * Based on the input properties, determine whether an authenticate or unauthenticated session should be used. If authenticated, creates a Password Authenticator for use in sending the email.
 *
 * @param properties mail properties
 * @return session
 */
                private Session createMailSession(final Properties properties) {
                    final boolean auth = Boolean.parseBoolean(properties.getProperty("mail.smtp.auth"));

                // Example usage:
        public static void main(String[] args) {
            String hostname = "your_smtp_host";
            int port = 587;
            String fromAddress = "your_email@example.com";
            String toAddress = "recipient_email@example.com";
            String subject = "Provenance Report";

            EmailProvenanceReporter reporter = new EmailProvenanceReporter(hostname, port, fromAddress, toAddress, subject);
            reporter.reportProvenance("Provenance data to be sent via email");
        }
    }

