package liquibase.database.core;

import junit.framework.TestCase;
import liquibase.Scope;
import liquibase.ScopeManager;
import liquibase.configuration.ConfiguredValueModifierFactory;
import liquibase.configuration.LiquibaseConfiguration;
import liquibase.exception.DatabaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.logging.Logger;
import liquibase.statement.core.RawSqlStatement;
import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatabaseUtilsTest extends TestCase {

	private static final String CURRENT_SEARCH_PATH = "currentPath";

	private Executor mockExecutor;

	@Override
	public void setUp() throws DatabaseException {
		LiquibaseConfiguration liquibaseConfiguration = Scope.getCurrentScope().getSingleton(LiquibaseConfiguration.class);
		ConfiguredValueModifierFactory configuredValueModifierFactory =
			Scope.getCurrentScope().getSingleton(ConfiguredValueModifierFactory.class);
		Scope           mockScope           = mock(Scope.class);
		ExecutorService mockExecutorService = mock(ExecutorService.class);
		mockExecutor = mock(Executor.class);
		RawSqlStatement showSearchPath = new RawSqlStatement("SHOW SEARCH_PATH");
		when(mockExecutor.queryForObject(refEq(showSearchPath), eq(String.class)))
			.thenReturn(CURRENT_SEARCH_PATH);
		when(mockExecutorService.getExecutor(any(), any()))
			.thenReturn(mockExecutor);
		when(mockScope.getSingleton(ExecutorService.class))
			.thenReturn(mockExecutorService);
		ScopeManager mockScopeManager = mock(ScopeManager.class);
		when(mockScopeManager.getCurrentScope())
			.thenReturn(mockScope);
		Scope.setScopeManager(mockScopeManager);
		when(mockScope.getSingleton(LiquibaseConfiguration.class))
			.thenReturn(liquibaseConfiguration);
		when(mockScope.getSingleton(ConfiguredValueModifierFactory.class))
			.thenReturn(configuredValueModifierFactory);
		when(mockScope.getLog(any()))
			.thenReturn(mock(Logger.class));
	}

	public void testQuoteSearchPathEntries() throws DatabaseException {
		String defaultCatalogueName = "test";
		String defaultSchemaName    = "defaultSchema";

		DatabaseUtils.initializeDatabase(defaultCatalogueName, defaultSchemaName, new PostgresDatabase());

		ArgumentCaptor<RawSqlStatement> sqlCaptor = ArgumentCaptor.forClass(RawSqlStatement.class);
		verify(mockExecutor)
			.execute(sqlCaptor.capture());
		RawSqlStatement setSearchPathStatement = sqlCaptor.getValue();
		String expectedStatement = String.format("SET SEARCH_PATH TO \"%s\", \"%s\"", defaultSchemaName,
			CURRENT_SEARCH_PATH);
		assertEquals(expectedStatement, setSearchPathStatement.getSql());
	}
}