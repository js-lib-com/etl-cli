package com.jslib.etl.impl;

import static com.jslib.util.Strings.join;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

import com.jslib.api.log.Log;
import com.jslib.api.log.LogFactory;
import com.jslib.converter.Converter;
import com.jslib.converter.ConverterException;
import com.jslib.converter.ConverterRegistry;
import com.jslib.etl.DataRecord;
import com.jslib.etl.EtlException;
import com.jslib.etl.IDataSource;
import com.jslib.etl.IExtractor;
import com.jslib.etl.ILoader;
import com.jslib.etl.IQueueElement;
import com.jslib.etl.LoaderException;
import com.jslib.etl.meta.MetaProperties;
import com.jslib.etl.meta.ServiceMeta;
import com.jslib.etl.meta.ServiceType;
import com.jslib.util.Classes;
import com.jslib.util.Strings;
import com.mchange.v2.c3p0.ComboPooledDataSource;

public class SqlDataSource implements IDataSource {
	private static final Log log = LogFactory.getLog(SqlDataSource.class);

	private static final String NAME_PROPERTY = "name";
	private static final String BATCH_SIZE_PROPERTY = "batchSize";

	private static final int BATCH_SIZE = 100;

	private final ServiceType type;
	private final String name;
	private final int batchSize;

	private final ComboPooledDataSource dataSource;
	private final Map<String, String> primaryKeys;
	private final Map<String, Object> lastInsertedIds;

	public SqlDataSource(ServiceMeta serviceMeta) {
		this.type = serviceMeta.getType();

		MetaProperties properties = serviceMeta.getProperties();
		this.name = properties.get(NAME_PROPERTY);
		this.batchSize = properties.get(BATCH_SIZE_PROPERTY, int.class, BATCH_SIZE);

		final Converter converter = ConverterRegistry.getConverter();
		this.dataSource = new ComboPooledDataSource();
		properties.forEach((name, value) -> {
			try {
				Method method = Classes.findMethod(this.dataSource.getClass(), Strings.getMethodAccessor("set", name));
				if (method.getParameterCount() == 1) {
					method.invoke(this.dataSource, converter.asObject(value, method.getParameterTypes()[0]));
				}
			} catch (NoSuchMethodException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | ConverterException e) {
				throw new EtlException("Fail to set property %s on data source %s. ", name, this.name);
			}
		});

		this.primaryKeys = new HashMap<>();
		this.lastInsertedIds = new HashMap<>();
	}

	@Override
	public void close() throws Exception {
		dataSource.getConnection().close();
	}

	@Override
	public ServiceType getType() {
		return type;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Object lastInsertedId(String tableName) {
		return lastInsertedIds.get(tableName);
	}

	@Override
	public char getEscapeChar() {
		switch (type) {
		case MYSQL:
			return '`';
		case POSTGRESQL:
			return '"';
		case SQLITE:
			return '"';
		case MSSQL:
			return '"';

		default:
			return '"';
		}
	}

	private String getPrimaryKey(String tableName) {
		String tablePrimaryKey = primaryKeys.get(tableName);
		if (tablePrimaryKey == null) {
			synchronized (this) {
				tablePrimaryKey = primaryKeys.get(tableName);
				if (tablePrimaryKey == null) {
					try (Connection connection = dataSource.getConnection()) {
						DatabaseMetaData dbmeta = connection.getMetaData();
						ResultSet rs = dbmeta.getPrimaryKeys(null, null, tableName);
						while (rs.next()) {
							tablePrimaryKey = rs.getString("COLUMN_NAME");
							log.debug("Table {} primary key: {}.", tableName, tablePrimaryKey);
							primaryKeys.put(tableName, tablePrimaryKey);
						}
					} catch (SQLException e) {
						throw new EtlException(e);
					}
				}
			}
		}
		return tablePrimaryKey;
	}

	private boolean isPrimaryKey(String tableName, String columnName) {
		return columnName.equals(getPrimaryKey(tableName));
	}

	// --------------------------------------------------------------------------------------------
	// IExtractor implementation

	@Override
	public IExtractor extractor(String tableName, List<String> columnNames, String... whereClause) {
		return new Extractor(tableName, columnNames, whereClause.length > 0 ? whereClause[0] : null);
	}

	private class Extractor implements IExtractor, Iterator<DataRecord> {
		private final String tableName;
		private final List<String> columnNames;
		private final String whereClause;

		public Extractor(String tableName, List<String> columnNames, String whereClause) {
			this.tableName = tableName;
			this.columnNames = columnNames;
			this.whereClause = whereClause;
		}

		private List<DataRecord> values = Collections.emptyList();
		private Iterator<DataRecord> valuesIterator = values.iterator();
		private int valuesOffset;

		@Override
		public Iterator<DataRecord> iterator() {
			return this;
		}

		@Override
		public boolean hasNext() {
			if (valuesIterator == null || !valuesIterator.hasNext()) {
				values = selectDataRecords();
				valuesIterator = values.iterator();
				valuesOffset += values.size();
			}
			return valuesIterator.hasNext();
		}

		@Override
		public DataRecord next() {
			return valuesIterator.next();
		}

		private List<DataRecord> selectDataRecords() {
			StringBuilder sqlBuilder = new StringBuilder();
			sqlBuilder.append("SELECT ");
			sqlBuilder.append(join(columnNames, ','));
			sqlBuilder.append(" FROM ");
			sqlBuilder.append(getEscapeChar());
			sqlBuilder.append(tableName);
			sqlBuilder.append(getEscapeChar());
			if (whereClause != null) {
				sqlBuilder.append(" WHERE ");
				sqlBuilder.append(whereClause);
			}

			switch (type) {
			case MYSQL:
			case POSTGRESQL:
			case SQLITE:
				sqlBuilder.append(" LIMIT ");
				sqlBuilder.append(valuesOffset);
				sqlBuilder.append(',');
				sqlBuilder.append(SqlDataSource.this.batchSize);
				break;

			case MSSQL:
				sqlBuilder.append(" ORDER BY ");
				sqlBuilder.append(getPrimaryKey(tableName));
				sqlBuilder.append(" OFFSET ");
				sqlBuilder.append(valuesOffset);
				sqlBuilder.append(" ROWS FETCH NEXT ");
				sqlBuilder.append(SqlDataSource.this.batchSize);
				sqlBuilder.append(" ROWS ONLY");
				break;

			default:
				break;
			}

			List<DataRecord> records = new ArrayList<>();
			try ( //
					Connection connection = SqlDataSource.this.dataSource.getConnection(); //
					Statement statement = connection.createStatement(); //
					ResultSet result = statement.executeQuery(sqlBuilder.toString()); //
			) {
				connection.setAutoCommit(true);
				while (result.next()) {
					DataRecord record = new DataRecord();
					for (String columnName : columnNames) {
						record.put(columnName, result.getObject(columnName));
					}
					records.add(record);
				}
			} catch (SQLException e) {
				log.error(e);
				return Collections.emptyList();
			}
			log.debug("{} found {} records: {__SQL__}", name, records.size(), sqlBuilder.toString());
			return records;
		}
	}

	// --------------------------------------------------------------------------------------------
	// ILoader implementation

	@Override
	public ILoader loader(String tableName) {
		return new BatchLoader(tableName);
	}

	@Override
	public ILoader loader(String tableName, boolean speedOptimization) {
		if (speedOptimization) {
			return new BatchLoader(tableName);
		}
		return new Loader(tableName);
	}

	private class Loader implements ILoader {
		protected final String tableName;

		public Loader(String tableName) {
			this.tableName = tableName;
		}

		@Override
		public void write(DataRecord record) throws SQLException {
			QueryMeta query = createQuery(record);
			log.trace("{} query: {SQL}", name, query.sql);

			try ( //
					Connection connection = dataSource.getConnection(); //
					PreparedStatement statement = connection.prepareStatement(query.sql, Statement.RETURN_GENERATED_KEYS); //
			) {
				connection.setAutoCommit(true);
				for (int i = 0; i < query.columnNames.size(); ++i) {
					statement.setObject(i + 1, record.get(query.columnNames.get(i)));
				}
				if (statement.executeUpdate() != 1) {
					log.error("{} insert fails.", name);
				}

				if (query.insert) {
					try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
						if (generatedKeys.next()) {
							lastInsertedIds.put(tableName, generatedKeys.getLong(1));
						} else {
							throw new SQLException("Fail to get generated key.");
						}
					}
				}
			} catch (SQLException e) {
				if (e.getSQLState().equals("23505")) {
					log.debug(e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
				} else {
					log.fatal("{exception}:{SQL}", e, query.sql);
					throw e;
				}
			}
		}

		@Override
		public void close() {
		}

		protected QueryMeta createQuery(DataRecord record) {
			String primaryKey = null;

			QueryMeta query = new QueryMeta();
			query.columnNames = new ArrayList<>();
			List<String> wildcards = new ArrayList<>();

			// optimize for insert which is by far the most used use case
			for (String columnName : record.columnNames()) {
				if (isPrimaryKey(tableName, columnName)) {
					primaryKey = columnName;
					query.insert = false;
				} else {
					query.columnNames.add(columnName);
					wildcards.add("?");
				}
			}

			if (primaryKey == null) {
				StringBuilder sql = new StringBuilder();
				sql.append("INSERT INTO ");
				sql.append(getEscapeChar());
				sql.append(tableName);
				sql.append(getEscapeChar());

				sql.append("(");
				for (int i = 0; i < query.columnNames.size(); ++i) {
					if (i > 0) {
						sql.append(',');
					}
					sql.append(query.columnNames.get(i));
				}
				sql.append(") VALUES(");

				for (int i = 0; i < query.columnNames.size(); ++i) {
					if (i > 0) {
						sql.append(',');
					}
					sql.append('?');
				}
				sql.append(')');

				if (type == ServiceType.POSTGRESQL) {
					// sql.append(" RETURNING ");
					// sql.append(getPrimaryKey(tableName));
				}
				query.sql = sql.toString();
			} else {
				StringBuilder sql = new StringBuilder();
				sql.append("UPDATE ");
				sql.append(getEscapeChar());
				sql.append(tableName);
				sql.append(getEscapeChar());
				sql.append(" SET ");

				for (int i = 0; i < query.columnNames.size(); ++i) {
					if (i > 0) {
						sql.append(',');
					}
					sql.append(query.columnNames.get(i));
					sql.append("=?");
				}

				sql.append(" WHERE ");
				sql.append(primaryKey);
				sql.append("=?");

				query.sql = sql.toString();
				query.columnNames.add(primaryKey);
			}

			return query;
		}

		protected class QueryMeta {
			String sql;
			List<String> columnNames;
			boolean insert = true;
		}
	}

	private class BatchLoader extends Loader implements Runnable {
		// 30 minutes indeed
		private static final int DESTROY_TIMEOUT = 30 * 60 * 1000;

		private final BlockingQueue<IQueueElement> queue;
		private final Thread thread;
		private volatile Throwable exception;

		public BatchLoader(String tableName) {
			super(tableName);

			this.queue = new LinkedBlockingQueue<>();

			this.thread = new Thread(this, "Loader Thread");
			this.thread.setDaemon(false);
			this.thread.start();
		}

		@Override
		public void write(DataRecord record) {
			for (;;) {
				try {
					queue.put(record);
					break;
				} catch (InterruptedException unused) {
				}
			}
		}

		@Override
		public void run() {
			List<DataRecord> records = new ArrayList<>();
			IQueueElement element;

			int recordsCount = 0;
			try (Connection connection = dataSource.getConnection()) {
				connection.setAutoCommit(true);

				for (;;) {
					try {
						element = queue.take();
					} catch (InterruptedException e) {
						continue;
					}

					if (element instanceof ShutdownWriterLoop) {
						log.debug("Got shutdown request on batch loader thread.");
						insert(connection, tableName, records);
						break;
					}

					++recordsCount;
					records.add((DataRecord) element);
					if (records.size() == batchSize) {
						insert(connection, tableName, records);
						records.clear();
					}
				}

			} catch (Throwable e) {
				log.error(e);
				exception = e;
			}

			if (exception != null) {
				log.error("Abort batch thread.");
			} else {
				log.debug("Stop batch loader thread. Processed {} records.", recordsCount);
			}
		}

		private long insert(Connection connection, String tableName, List<DataRecord> records) throws SQLException {
			if (records.isEmpty()) {
				return 0L;
			}
			QueryMeta query = createQuery(records.get(0));
			log.trace("{} query: {SQL}", name, query.sql);

			try (PreparedStatement statement = connection.prepareStatement(query.sql)) {
				for (DataRecord record : records) {
					for (int i = 0; i < query.columnNames.size(); ++i) {
						statement.setObject(i + 1, record.get(query.columnNames.get(i)));
					}
					statement.addBatch();
				}

				int[] rows = statement.executeBatch();
				if (rows.length != records.size()) {
					throw new EtlException("Database %s batch update fail.", name);
				}
				for (int i = 0; i < rows.length; ++i) {
					if (rows[i] == Statement.SUCCESS_NO_INFO) {
						continue;
					}
					if (rows[i] == Statement.EXECUTE_FAILED) {
						throw new EtlException("Database %s batch update fail: %d", name, rows[i]);
					}
				}

			} catch (SQLIntegrityConstraintViolationException e) {
			} catch (SQLException e) {
				if (e.getSQLState().equals("23505")) {
					log.debug(e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
				} else {
					log.fatal("{exception}:{SQL}", e, query.sql);
					throw e;
				}
			}
			return 0L;
		}

		@Override
		public void close() {
			log.trace("close()");

			try {
				queue.put(new ShutdownWriterLoop());
			} catch (InterruptedException e) {
				log.error(e);
			}

			try {
				thread.join(DESTROY_TIMEOUT);
			} catch (InterruptedException e) {
				log.error(e);
			}

			if (exception != null) {
				throw new LoaderException(exception);
			}
		}

		private class ShutdownWriterLoop implements IQueueElement {
		}
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public String testConnection() {
		try {
			dataSource.getConnection();
			return "Database connection is working properly.";
		} catch (SQLException e) {
			log.error(e);
			return e.getMessage();
		}
	}

	private static final Pattern SELECT_QUERY = Pattern.compile("^SELECT", Pattern.CASE_INSENSITIVE);

	@Override
	public Object execute(String query) throws SQLException {
		log.trace(query);
		boolean update = !SELECT_QUERY.asPredicate().test(query);

		try ( //
				Connection connection = SqlDataSource.this.dataSource.getConnection(); //
				Statement statement = connection.createStatement(); //
		) {
			connection.setAutoCommit(true);

			if (update) {
				return statement.executeUpdate(query);
			}

			ResultSet result = statement.executeQuery(query); //
			if (!result.next()) {
				return null;
			}
			return result.getObject(1);
		} catch (SQLException e) {
			exception(e, query);
		}
		return null;
	}

	private static void exception(SQLException e, String sql) throws SQLException {
		// TODO: non portable solution
		if (e.getSQLState().equals("23505")) {
			log.debug(e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
		} else {
			log.fatal("{exception}:{SQL}", e, sql);
			throw e;
		}
	}
}
