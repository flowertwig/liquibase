package liquibase.command.core;

import liquibase.command.*;
import liquibase.configuration.ConfigurationValueObfuscator;
import liquibase.exception.CommandExecutionException;

import java.util.UUID;

public class SyncHubCommandStep extends AbstractCliWrapperCommandStep {

    public static final String[] COMMAND_NAME = {"syncHub"};

    public static final CommandArgumentDefinition<String> URL_ARG;
    public static final CommandArgumentDefinition<String> DEFAULT_SCHEMA_NAME_ARG;
    public static final CommandArgumentDefinition<String> DEFAULT_CATALOG_NAME_ARG;
    public static final CommandArgumentDefinition<String> USERNAME_ARG;
    public static final CommandArgumentDefinition<String> PASSWORD_ARG;
    public static final CommandArgumentDefinition<String> CHANGELOG_FILE_ARG;
    public static final CommandArgumentDefinition<UUID> HUB_CONNECTION_ID_ARG;
    public static final CommandArgumentDefinition<UUID> HUB_PROJECT_ID_ARG;
    public static final CommandArgumentDefinition<String> DRIVER_ARG;
    public static final CommandArgumentDefinition<String> DRIVER_PROPERTIES_FILE_ARG;

    static {
        CommandBuilder builder = new CommandBuilder(COMMAND_NAME);
        URL_ARG = builder.argument("url", String.class).required()
                .description("The JDBC database connection URL").build();
        DEFAULT_SCHEMA_NAME_ARG = builder.argument("defaultSchemaName", String.class)
                .description("The default schema name to use for the database connection").build();
        DEFAULT_CATALOG_NAME_ARG = builder.argument("defaultCatalogName", String.class)
                .description("The default catalog name to use for the database connection").build();
        USERNAME_ARG = builder.argument("username", String.class)
                .description("Username to use to connect to the database").build();
        PASSWORD_ARG = builder.argument("password", String.class)
                .description("Password to use to connect to the database")
                .setValueObfuscator(ConfigurationValueObfuscator.STANDARD)
                .build();
        DRIVER_ARG = builder.argument("driver", String.class)
                .description("The JDBC driver class").build();
        DRIVER_PROPERTIES_FILE_ARG = builder.argument("driverPropertiesFile", String.class)
                .description("The JDBC driver properties file").build();
        CHANGELOG_FILE_ARG = builder.argument("changelogFile", String.class)
                .description("The root changelog").build();
        HUB_CONNECTION_ID_ARG = builder.argument("hubConnectionId", UUID.class)
                .description("Used to identify the specific Connection in which to record or extract data at Liquibase Hub. Available in your Liquibase Hub Project at https://hub.liquibase.com.").build();
        HUB_PROJECT_ID_ARG = builder.argument("hubProjectId", UUID.class)
                .description("Used to identify the specific Project in which to record or extract data at Liquibase Hub. Available in your Liquibase Hub account at https://hub.liquibase.com.").build();
    }

    @Override
    public String[] getName() {
        return COMMAND_NAME;
    }

    @Override
    public void adjustCommandDefinition(CommandDefinition commandDefinition) {
        commandDefinition.setShortDescription("Synchronize the local DatabaseChangeLog table with Liquibase Hub");
    }

    @Override
    protected String[] collectArguments(CommandScope commandScope) throws CommandExecutionException {
        return collectArguments(commandScope, null, null);
    }
}
