/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.chinacache.robin.format.messageformat;

import org.apache.storm.hdfs.bolt.format.RecordFormat;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * RecordFormat implementation that uses field and record delimiters. By default
 * uses a comma (",") as the field delimiter and a newline ("\n") as the record
 * delimiter.
 * 
 * Also by default, this implementation will output all the field values in the
 * tuple in the order they were declared. To override this behavior, call
 * <code>withFields()</code> to specify which tuple fields to output.
 * 
 */
public class LogExceptionRecordFormat implements RecordFormat {

	private static final long serialVersionUID = 1L;
	public static final String DEFAULT_FIELD_DELIMITER = ",";
	public static final String DEFAULT_RECORD_DELIMITER = "\n";
	private String fieldDelimiter = DEFAULT_FIELD_DELIMITER;
	private String recordDelimiter = DEFAULT_RECORD_DELIMITER;
	private Fields fields = null;

	/**
	 * Only output the specified fields.
	 * 
	 * @param fields
	 * @return
	 */
	public LogExceptionRecordFormat withFields(Fields fields) {
		this.fields = fields;
		return this;
	}

	/**
	 * Overrides the default field delimiter.
	 * 
	 * @param delimiter
	 * @return
	 */
	public LogExceptionRecordFormat withFieldDelimiter(String delimiter) {
		fieldDelimiter = delimiter;
		return this;
	}

	/**
	 * Overrides the default record delimiter.
	 * 
	 * @param delimiter
	 * @return
	 */
	public LogExceptionRecordFormat withRecordDelimiter(String delimiter) {
		recordDelimiter = delimiter;
		return this;
	}

	@Override
	public byte[] format(Tuple tuple) {
		StringBuilder sb = new StringBuilder();
		sb.append(tuple.getValue(0) + "\n");
		return sb.toString().getBytes();
	}
}
