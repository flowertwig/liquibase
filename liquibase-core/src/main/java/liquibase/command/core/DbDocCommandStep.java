package liquibase.command.core;

import liquibase.command.*;
import liquibase.configuration.ConfigurationValueObfuscator;
import liquibase.exception.CommandExecutionException;

import java.util.Arrays;

public class DbDocCommandStep extends AbstractCliWrapperCommandStep {

    public static final String[] COMMAND_NAME = {"dbDoc"};

    public static final CommandArgumentDefinition<String> CHANGELOG_FILE_ARG;
    public static final CommandArgumentDefinition<String> URL_ARG;
    public static final CommandArgumentDefinition<String> DEFAULT_SCHEMA_NAME_ARG;
    public static final CommandArgumentDefinition<String> DEFAULT_CATALOG_NAME_ARG;
    public static final CommandArgumentDefinition<String> USERNAME_ARG;
    public static final CommandArgumentDefinition<String> PASSWORD_ARG;
    public static final CommandArgumentDefinition<String> OUTPUT_DIRECTORY_ARG;
    public static final CommandArgumentDefinition<String> DRIVER_ARG;
    public static final CommandArgumentDefinition<String> DRIVER_PROPERTIES_FILE_ARG;

    static {
        CommandBuilder builder = new CommandBuilder(COMMAND_NAME);
        CHANGELOG_FILE_ARG = builder.argument("changelogFile", String.class)
                .description("The root changelog").required().build();
        URL_ARG = builder.argument("url", String.class).required()
                .description("The JDBC database connection URL").build();
        DEFAULT_SCHEMA_NAME_ARG = builder.argument("defaultSchemaName", String.class)
                .description("The default schema name to use for the database connection").build();
        DEFAULT_CATALOG_NAME_ARG = builder.argument("defaultCatalogName", String.class)
                .description("The default catalog name to use for the database connection").build();
        DRIVER_ARG = builder.argument("driver", String.class)
                .description("The JDBC driver class").build();
        DRIVER_PROPERTIES_FILE_ARG = builder.argument("driverPropertiesFile", String.class)
                .description("The JDBC driver properties file").build();
        USERNAME_ARG = builder.argument("username", String.class)
                .description("The database username").build();
        PASSWORD_ARG = builder.argument("password", String.class)
                .description("The database password")
                .setValueObfuscator(ConfigurationValueObfuscator.STANDARD)
                .build();
        OUTPUT_DIRECTORY_ARG = builder.argument("outputDirectory", String.class).required()
                .description("The directory where the documentation is generated").build();
    }

    @Override
    public String[] getName() {
        return COMMAND_NAME;
    }

    @Override
    protected String[] collectArguments(CommandScope commandScope) throws CommandExecutionException {
        return collectArguments(commandScope, null, "outputDirectory");
    }

    @Override
    public void adjustCommandDefinition(CommandDefinition commandDefinition) {
        commandDefinition.setShortDescription("Generates JavaDoc documentation for the existing database and changelogs");
    }
}
