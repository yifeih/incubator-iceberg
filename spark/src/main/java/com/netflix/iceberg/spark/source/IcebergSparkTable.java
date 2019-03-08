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

import com.google.common.collect.Lists;
import com.netflix.iceberg.FileFormat;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.spark.SparkFilters;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.SupportsBatchRead;
import org.apache.spark.sql.sources.v2.SupportsBatchWrite;
import org.apache.spark.sql.sources.v2.reader.*;
import org.apache.spark.sql.sources.v2.writer.BatchWrite;
import org.apache.spark.sql.sources.v2.writer.WriteBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.List;
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
        return new IcebergReaderBuilder(table);
    }

    @Override
    public WriteBuilder newWriteBuilder(DataSourceOptions options) {
        IcebergWriterBuilder writerBuilder = new IcebergWriterBuilder(table);

        Optional<String> formatOption = options.get("iceberg.write.format");
        formatOption.ifPresent(writerBuilder::setFileFormat);
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
        private FileFormat fileFormat;

        public IcebergWriterBuilder(Table table) {
            this.table = table;
            this.fileFormat = FileFormat.valueOf(table.properties()
                .getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT)
                .toUpperCase(Locale.ENGLISH));
        }

        public void setFileFormat(String format) {
            fileFormat = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
        }

        @Override
        public BatchWrite buildForBatch() {
            return new Writer(table, fileFormat);
        }

    }

    private static class IcebergReaderBuilder implements ScanBuilder,
        SupportsPushDownFilters,
        SupportsPushDownRequiredColumns {

        private static final Filter[] NO_FILTERS = new Filter[0];

        private final Table table;
        private Filter[] pushedFilters = NO_FILTERS;
        private List<Expression> filterExpressions = Lists.newArrayList();
        private StructType requestedSchema = null;

        public IcebergReaderBuilder(Table table) {
            this.table = table;
        }

        @Override
        public Filter[] pushFilters(Filter[] filters) {
            List<Expression> expressions = Lists.newArrayListWithExpectedSize(filters.length);
            List<Filter> pushed = Lists.newArrayListWithExpectedSize(filters.length);

            for (Filter filter : filters) {
                Expression expr = SparkFilters.convert(filter);
                if (expr != null) {
                    expressions.add(expr);
                    pushed.add(filter);
                }
            }

            this.filterExpressions = expressions;
            this.pushedFilters = pushed.toArray(new Filter[0]);

            // Spark doesn't support residuals per task, so return all filters
            // to get Spark to handle record-level filtering
            return filters;
        }

        @Override
        public Filter[] pushedFilters() {
            return pushedFilters;
        }

        @Override
        public void pruneColumns(StructType requiredSchema) {
            this.requestedSchema = requiredSchema;
        }

        @Override
        public Scan build() {
            return new Reader(table, filterExpressions, requestedSchema);
        }
    }
}
