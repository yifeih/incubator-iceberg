/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.netflix.iceberg.spark.source;

import com.netflix.iceberg.FileFormat;
import com.netflix.iceberg.Table;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.SupportsBatchRead;
import org.apache.spark.sql.sources.v2.SupportsBatchWrite;
import org.apache.spark.sql.sources.v2.reader.ScanBuilder;
import org.apache.spark.sql.sources.v2.writer.BatchWrite;
import org.apache.spark.sql.sources.v2.writer.WriteBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.Locale;
import java.util.Optional;

import static com.netflix.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static com.netflix.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

public class IcebergSparkTable implements SupportsBatchRead, SupportsBatchWrite  {

    private final Table table;

    public IcebergSparkTable(Table table) {
        this.table = table;
    }

    @Override
    public ScanBuilder newScanBuilder(DataSourceOptions options) {


    }

    @Override
    public WriteBuilder newWriteBuilder(DataSourceOptions options) {
        IcebergWriterBuilder writerBuilder = new IcebergWriterBuilder(table);

        Optional<String> formatOption = options.get("iceberg.write.format");
        if (formatOption.isPresent()) {
            writerBuilder = writerBuilder.setFileFormat(formatOption.get());
        }
        return writerBuilder;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public StructType schema() {
        return null;
    }


    private static class IcebergWriterBuilder implements WriteBuilder {
        private final Table table;
        private final Optional<FileFormat> fileFormat;

        public IcebergWriterBuilder(Table table) {
            this.table = table;
            this.fileFormat = Optional.empty();
        }

        public IcebergWriterBuilder(Table table, FileFormat fileFormat) {
            this.table = table;
            this.fileFormat = Optional.of(fileFormat);
        }

        public IcebergWriterBuilder setFileFormat(String format) {
            return new IcebergWriterBuilder(this.table,
                FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH)));
        }

        @Override
        public BatchWrite buildForBatch() {
            fileFormat.ifPresent(fileFormat1 -> new Writer(table, fileFormat1));
            return new Writer(table,
                FileFormat.valueOf(table.properties()
                    .getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT)
                    .toUpperCase(Locale.ENGLISH)));
        }

    }
}
