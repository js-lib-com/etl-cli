package com.jslib.etl;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.util.NoSuchElementException;

import com.jslib.api.log.Log;
import com.jslib.api.log.LogFactory;
import com.jslib.etl.impl.Project;
import com.jslib.etl.meta.IParser;
import com.jslib.etl.meta.ParserFactory;
import com.jslib.etl.meta.ProjectMeta;
import com.jslib.util.Files;
import com.jslib.util.Strings;

public class Main {
	private static final Log log = LogFactory.getLog(Main.class);

	public static void main(String... arguments) {
		LogFactory.getLogContext().put("app_name", "ETL");
		LogFactory.getLogContext().put("app_provider", app_provider(arguments));
		log.trace("main(String...)");

		// ETL file path can be anywhere in command line arguments; in sample below is first
		// etl ngs.etl.md --tablet $tablet_serial --database $database_file --file $database_archive

		String etlFile = etl_file(arguments);
		if (etlFile == null) {
			log.warn("Missing ETL file argument.");
			System.exit(1);
			return;
		}

		ParserFactory factory = new ParserFactory();
		IParser parser = factory.create(etlFile);

		try (Reader reader = new FileReader(etlFile)) {
			ProjectMeta meta = parser.parse(reader);

			try (IProject project = new Project()) {
				project.parseArguments(meta.getArguments(), arguments);
				project.parseMeta(meta);

				for (ITask task : project.getTasks()) {
					if (!task.execute()) {
						System.exit(2);
						return;
					}
				}
			}

		} catch (NoSuchElementException e) {
			log.fatal("Invalid command line arguments |{}|. See {}.", Strings.join(arguments), etlFile);
			System.exit(3);
			return;
		} catch (FileNotFoundException e) {
			log.fatal("ETL file not found {}.", etlFile);
			System.exit(4);
			return;
		} catch (IOException e) {
			log.fatal("Error reading ETL file {}: {}.", etlFile, e);
			System.exit(5);
			return;
		} catch (ParseException e) {
			log.fatal("Parse exception on ETL file {}: {}.", etlFile, e);
			System.exit(6);
			return;
		} catch (FunctionException e) {
			log.fatal("Function fail on ETL file {}: {}.", etlFile, e);
			System.exit(7);
			return;
		} catch (ExtractorException e) {
			log.fatal("Extractor fail on ETL file {}: {}.", etlFile, e);
			System.exit(7);
			return;
		} catch (LoaderException e) {
			log.fatal("Loader fail on ETL file {}: {}.", etlFile, e);
			System.exit(8);
			return;
		} catch (EtlException e) {
			log.fatal("ETL fail on file {}: {}.", etlFile, e);
			System.exit(9);
			return;
		} catch (Exception e) {
			log.dump(e);
			System.exit(10);
			return;
		}

		System.exit(0);
	}

	private static String app_provider(String[] arguments) {
		return Files.basename(etl_file(arguments)).split("\\.")[0].toUpperCase();
	}

	private static String etl_file(String... arguments) {
		for (String argument : arguments) {
			if (!argument.startsWith("-")) {
				return argument;
			}
		}
		return null;
	}
}
