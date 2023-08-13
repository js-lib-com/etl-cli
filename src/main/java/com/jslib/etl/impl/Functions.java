package com.jslib.etl.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.jslib.api.log.Log;
import com.jslib.api.log.LogFactory;
import com.jslib.converter.Converter;
import com.jslib.converter.ConverterRegistry;
import com.jslib.etl.EtlException;
import com.jslib.etl.FunctionException;
import com.jslib.etl.IDataSource;
import com.jslib.etl.IProject;
import com.jslib.util.Strings;

/**
 * Built in functions invoked via natural language like expressions. A function expression starts with a function name followed
 * by a stop word, e.g. a conjunction, and invocation arguments. Function name uses spaces as word separator and letter case is
 * not relevant. If more arguments they are separated by comma and 'and' conjunction.
 * 
 * @author Iulian Rotaru
 */
@SuppressWarnings("unused")
public class Functions {
	private static final Log log = LogFactory.getLog(Functions.class);

	private final Converter converter;
	private final Patterns patterns;
	private final IProject project;
	private final Map<String, Method> methods;

	public Functions(IProject project) {
		log.trace("Functions(IProject)");
		this.converter = ConverterRegistry.getConverter();
		this.patterns = new Patterns();
		this.project = project;
		this.methods = new HashMap<>();

		for (Method method : getClass().getDeclaredMethods()) {
			if (Modifier.isPrivate(method.getModifiers())) {
				method.setAccessible(true);
				methods.put(method.getName(), method);
			}
		}
	}

	public Object invoke(String expression) {
		if (patterns.isSql(expression)) {
			return executeSql(expression);
		}

		// last inserted ID from commons.mission
		// person name on SELECT Operator1 FROM ngs.information
		// translate to English and upper case on SELECT Floor FROM ngs.information
		// timestamp by yyyy-MM-dd'T'HH:mm:ss on SELECT Timestamp FROM ngs.measures_inspectra ORDER BY Timestamp DESC LIMIT 1

		int position = expression.indexOf(" on ");
		if (position == -1) {
			return invoke(expression, null);
		}

		Object argument = executeSql(expression.substring(position + 4).trim());
		expression = expression.substring(0, position).trim();
		return invoke(expression, argument);
	}

	// invoked by transformers and by above
	public Object invoke(String expression, Object argument) {
		// last inserted ID from commons.mission
		// person name
		// translate to English and upper case
		// timestamp by yyyy-MM-dd'T'HH:mm:ss, ro-RO

		String[] extraArguments = null;
		String[] p = expression.split("\\s+(by|with|at|in)\\s+");
		if (p.length == 2) {
			extraArguments = p[1].split(",\\s*"); // yyyy-MM-dd'T'HH:mm:ss, ro-RO
			expression = p[0]; // timestamp
		} else {
			extraArguments = new String[0];
			p = expression.split("\\s+from\\s+");
			if (p.length == 2) {
				if (argument != null) {
					throw new IllegalStateException("Argument provided by caller and by expression.");
				}
				argument = p[1];
				expression = p[0];
			}
		}

		assert argument != null;

		// last inserted ID
		// person name
		// translate to English and upper case
		// timestamp by yyyy-MM-dd'T'HH:mm:ss

		Object value = argument;
		for (String functionName : expression.split("\\s+and\\s+")) {
			Method function = methods.get(Strings.toMemberName(functionName));
			if (function == null) {
				throw new EtlException("Function %s not found.", functionName);
			}

			Object[] arguments = new Object[extraArguments.length + 1];
			arguments[0] = value;
			for (int i = 0; i < extraArguments.length; ++i) {
				arguments[i + 1] = extraArguments[i];
			}

			assert function.getParameterTypes().length == arguments.length;
			for (int i = 0; i < arguments.length; ++i) {
				if (arguments[i] instanceof String) {
					arguments[i] = converter.asObject((String) arguments[i], function.getParameterTypes()[i]);
				}
			}
			// log.trace("Invoke {}({}).", functionName, Strings.join(arguments, ", "));

			try {
				value = function.invoke(this, arguments);
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				throw new EtlException(e);
			}
		}
		return value;
	}

	private Method method(String expression, WordsIterator words) {
		if (!words.hasNext()) {
			throw new EtlException("Function not found for expression |%s|.", expression);
		}
		StringBuilder methodName = new StringBuilder(words.next().toLowerCase());
		while (words.hasNext()) {
			String word = words.next();
			methodName.append(Character.toUpperCase(word.charAt(0)));
			methodName.append(word.substring(1).toLowerCase());
			Method method = this.methods.get(methodName.toString());
			if (method != null) {
				return method;
			}
		}
		throw new EtlException("Function not found for expression |%s|.", expression);
	}

	private static String methodName(String[] words, int count) {
		StringBuilder methodName = new StringBuilder(words[0].toLowerCase());
		for (int i = 1; i < count; ++i) {
			methodName.append(Character.toUpperCase(words[i].charAt(0)));
			methodName.append(words[i].substring(1).toLowerCase());
		}
		return methodName.toString();
	}

	private static final long MIN_TIMESTAMP = Timestamp.from(Instant.parse("2000-01-01T00:00:00.00Z")).getTime();

	private Timestamp timestamp(String timestampValue, String dateFormat) throws ParseException {
		if (timestampValue == null) {
			return null;
		}
		DateFormat format = new SimpleDateFormat(dateFormat);
		format.setTimeZone(TimeZone.getTimeZone("EEST"));
		Timestamp timestamp = new Timestamp(format.parse(timestampValue).getTime());
		if (timestamp.getTime() < MIN_TIMESTAMP || timestamp.getTime() > System.currentTimeMillis()) {
			log.error("Invalid timestamp value {}.", timestamp);
			throw new EtlException("Invalid timestamp value.");
		}
		return timestamp;
	}

	private int integer(String integerValue) {
		return Integer.parseInt(integerValue);
	}

	private double number(String doubleValue) {
		return Double.parseDouble(doubleValue);
	}

	private Object lastInsertedId(TableName tableName) {
		log.trace("lastInsertedId(TableName)");
		IDataSource dataSource = project.getDataSource(tableName.getDataSourceName());
		return dataSource.lastInsertedId(tableName.getSimpleName());
	}

	private static final Map<String, String> ENGLISH_DICTIONARY = new HashMap<>();
	static {
		ENGLISH_DICTIONARY.put("subterana", "underground");
	}

	private String translateToEnglish(String text) {
		String translation = ENGLISH_DICTIONARY.get(text.toLowerCase());
		return translation != null ? translation : text;
	}

	private String upperCase(String text) {
		return text.toUpperCase();
	}

	private String personName(String name) {
		return Strings.toTitleCase(name);
	}

	private double kphToMps(double kph) {
		return 1000.0 * kph / 3600.0;
	}

	public Object executeSql(String query) {
		log.debug(query);

		// SELECT Operator1 FROM ngs.information
		// SELECT TOP 1 e.name FROM defect
		// DELETE FROM commons.mission WHERE application_mission='...'

		Pattern pattern = Pattern.compile("^(?:SELECT|DELETE)\\s+.*FROM\\s+(\\w+).+$");
		Matcher matcher = pattern.matcher(query);
		if (!matcher.find()) {
			throw new EtlException("Invalid SQL expression: %s", query);
		}

		String dataSourceName = matcher.group(1);
		IDataSource dataSource = project.getDataSource(dataSourceName);
		query = query.replaceAll("FROM\\s+\\w+\\.(\\w+)", Strings.concat("FROM ", dataSource.getEscapeChar(), "$1", dataSource.getEscapeChar()));
		query = query.replaceAll("JOIN\\s+\\w+\\.(\\w+)", Strings.concat("JOIN ", dataSource.getEscapeChar(), "$1", dataSource.getEscapeChar()));
		try {
			return dataSource.execute(query);
		} catch (SQLException e) {
			throw new FunctionException(e);
		}
	}

	private static final List<String> STOP_WORDS = new ArrayList<>();
	static {
		STOP_WORDS.add("from");
	}

	private static class Node {
		Map<String, Node> children = new HashMap<>();
		Method method;
	}

	private static class WordsTree {
		Node root;

		Method get(List<String> words) {
			return get(root, words, 0);
		}

		Method get(Node node, List<String> words, int level) {
			if (level == words.size()) {
				return null;
			}

			Node child = node.children.get(words.get(level));
			if (child.method != null) {
				return child.method;
			}
			for (Map.Entry<String, Node> entry : node.children.entrySet()) {
				Method method = get(entry.getValue(), words, level + 1);
				if (method != null) {
					return method;
				}
			}
			return null;
		}
	}

	private static class WordsIterator implements Iterator<String> {
		private final String[] words;

		private int index;

		public WordsIterator(String text) {
			this.words = text.split("\\s+");
			this.index = -1;
		}

		@Override
		public boolean hasNext() {
			return index + 1 < words.length;
		}

		@Override
		public String next() {
			return words[++index];
		}

		public void back() {
			--index;
		}

		public String rest() {
			if (!hasNext()) {
				return null;
			}
			StringBuilder text = new StringBuilder(next());
			while (hasNext()) {
				text.append(' ');
				text.append(next());
			}
			return text.toString();
		}
	}
}
