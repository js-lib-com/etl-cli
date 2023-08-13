package com.jslib.etl.meta;

import java.io.IOException;
import java.io.Reader;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.jslib.util.Classes;

@Ignore
public class MarkdownParserTest {
	private IParser parser;

	@Before
	public void beforeTest() {
		parser = new MarkdownParser();
	}

	@Test
	public void Given_WhenParse_Then() throws IOException {
		// given
		Reader reader = Classes.getResourceAsReader("ngs.etl.md");

		// when
		ProjectMeta project = parser.parse(reader);

		// then

		System.out.println();
		System.out.println("----------------------------------------");

		System.out.println(project.getArguments());
		System.out.println(project.getVariables());
		for (ServiceMeta service : project.getServices()) {
			System.out.println(service);
		}
		for (TaskMeta taskMeta : project.getTasksMeta()) {
			System.out.println(taskMeta);
		}
	}
}
