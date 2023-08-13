package com.jslib.etl.meta;

import com.jslib.util.Files;

public class ParserFactory {
	public IParser create(String descriptorFile) {
		final String fileType = Files.getExtension(descriptorFile);
		switch (fileType) {
		case "md":
			return new MarkdownParser();

		default:
			throw new IllegalStateException("Not recognized file type " + fileType);
		}
	}
}
