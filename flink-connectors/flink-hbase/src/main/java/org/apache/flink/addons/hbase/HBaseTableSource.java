/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.addons.hbase;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.conf.Configuration;

/**
 * Creates a table source that helps to scan data from an hbase table
 *
 * Note : the colNames are specified along with a familyName and they are seperated by a ':'
 * For eg, cf1:q1 - where cf1 is the familyName and q1 is the qualifier name
 */
public class HBaseTableSource implements BatchTableSource<Row>, ProjectableTableSource<Row> {

	private Configuration conf;
	private String tableName;
	private byte[] rowKey;
	private String[] colNames;
	private TypeInformation<?>[] colTypes;

	public HBaseTableSource(Configuration conf, String tableName, byte[] rowKey, String[] colNames,
							TypeInformation<?>[] colTypes) {
		this.conf = conf;
		this.tableName = Preconditions.checkNotNull(tableName, "Table  name");
		this.rowKey = Preconditions.checkNotNull(rowKey, "Rowkey");
		this.colNames = Preconditions.checkNotNull(colNames, "Field names");
		this.colTypes = Preconditions.checkNotNull(colTypes, "Field types");
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return new RowTypeInfo(colTypes);
	}

	@Override
	public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
		return execEnv.createInput(new HBaseTableSourceInputFormat(conf, tableName, colNames, colTypes), getReturnType());
	}

	@Override
	public ProjectableTableSource<Row> projectFields(int[] fields) {
		String[] newColNames = new String[fields.length];
		TypeInformation<?>[] newColTypes =  new TypeInformation<?>[fields.length];
		int i = 0;
		for(int field : fields) {
			newColNames[i] = colNames[field];
			newColTypes[i] = colTypes[field];
			i++;
		}
		return new HBaseTableSource(conf, tableName, rowKey, newColNames, newColTypes);
	}
}
