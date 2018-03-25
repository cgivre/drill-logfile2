/*
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
package org.apache.drill.exec.store.log;

import com.google.common.base.Charsets;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class LogRecordReader extends AbstractRecordReader {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LogRecordReader.class);

  private static class ColumnDefn {
    public final String name;
    public final int index;
    public int dataType;
    public NullableVarCharVector.Mutator mutator;

    public ColumnDefn(String name, int index) {
      this.name = name;
      this.index = index;
    }

    public String toString(){
      return "Name: " + this.name + ", Index: " + this.index;
    }
  }

  private static final int BATCH_SIZE = BaseValueVector.INITIAL_VALUE_ALLOCATION;

  private final DrillFileSystem dfs;
  private final FileWork fileWork;
  private final String userName;
  private final LogFormatConfig formatConfig;
  private ColumnDefn columns[];
  private List<ColumnDefn> columnList;
  private Pattern pattern;
  private BufferedReader reader;
  private int rowIndex;
  private int fieldCount;
  private int capturingGroups;
  private boolean errorFound;
  private OutputMutator outputMutator;

  public LogRecordReader(FragmentContext context, DrillFileSystem dfs,
                         FileWork fileWork, List<SchemaPath> columns, String userName,
                         LogFormatConfig formatConfig) {
    this.dfs = dfs;
    this.fileWork = fileWork;
    this.userName = userName;
    this.formatConfig = formatConfig;
    errorFound = false;
    this.columnList = new ArrayList<ColumnDefn>();

    // Ask the superclass to parse the projection list.
    setColumns(columns);
  }

  @Override
  public void setup(final OperatorContext context, final OutputMutator output) {
    this.outputMutator = output;

    setupPattern();
    openFile();
    setupProjection();
    defineVectors();

  }

  private void setupPattern() {
    try {
      pattern = Pattern.compile(formatConfig.regex);
      Matcher m = pattern.matcher("test");
      fieldCount = m.groupCount();
      capturingGroups = m.groupCount();
    } catch (PatternSyntaxException e) {
      throw UserException
          .validationError(e)
          .message("Failed to parse regex: \"%s\"", formatConfig.regex)
          .build(logger);
    }
  }

  private void setupProjection() {
    if (isSkipQuery()) {
      projectNone();
    } else if (isStarQuery()) {
      projectAll();
    } else {
      projectSubset();
    }
  }

  private void projectNone() {
    columns = new ColumnDefn[]{new ColumnDefn("dummy", -1)};
  }

  private void openFile() {
    InputStream in;
    try {
      in = dfs.open(new Path(fileWork.getPath()));
    } catch (Exception e) {
      throw UserException
          .dataReadError(e)
          .message("Failed to open open input file: %s", fileWork.getPath())
          .addContext("User name", userName)
          .build(logger);
    }
    reader = new BufferedReader(new InputStreamReader(in, Charsets.UTF_8));
  }

  private void projectAll() {
    List<String> fields = formatConfig.getFields();
    if (fields.isEmpty()) {
      throw UserException
          .validationError()
          .message("The plugin configuration does not define any columns.")
          .build(logger);
    } else if (fields.size() < capturingGroups) {
      int delta = capturingGroups - fields.size();
      for (int i = 0; i < delta; i++) {
        fields.add("field_" + i);
      }
    }
    int finalFieldSize = fields.size();
    if( !formatConfig.getErrorOnMismatch() ){
      finalFieldSize += 1;
    }
    columns = new ColumnDefn[finalFieldSize];

    for (int i = 0; i < columns.length - 1; i++) {
      columns[i] = new ColumnDefn(fields.get(i), i);
    }

    if( !formatConfig.getErrorOnMismatch()){
      columns[ columns.length -1 ] = new ColumnDefn("unmatched_rows", columns.length -1);
    }

  }

  private void projectSubset() {
    Collection<SchemaPath> project = this.getColumns();
    assert !project.isEmpty();
    columns = new ColumnDefn[project.size()];
    if( !formatConfig.getErrorOnMismatch()){
      columns = new ColumnDefn[project.size() + 1];
    }

    List<String> fields = formatConfig.getFields();
    int colIndex = 0;
    for (SchemaPath column : project) {
      if (column.getAsNamePart().hasChild()) {
        throw UserException
            .validationError()
            .message("The log format plugin supports only simple columns")
            .addContext("Projected column", column.toString())
            .build(logger);
      }
      String name = column.getAsNamePart().getName();
      int patternIndex = -1;
      for (int i = 0; i < fields.size(); i++) {
        if (fields.get(i).equalsIgnoreCase(name)) {
          patternIndex = i;
          break;
        }
      }
      columns[colIndex++] = new ColumnDefn(name, patternIndex);
    }

    if( !formatConfig.getErrorOnMismatch()){
      columns[ columns.length -1 ] = new ColumnDefn("unmatched_rows", columns.length -1);
    }
  }

  private void defineVectors() {
    for (int i = 0; i < columns.length; i++) {
      MaterializedField field = MaterializedField.create(columns[i].name,
          Types.optional(MinorType.VARCHAR));
      try {
        columns[i].mutator = outputMutator.addField(field, NullableVarCharVector.class).getMutator();
      } catch (SchemaChangeException e) {
        throw UserException
            .systemError(e)
            .message("Vector creation failed")
            .build(logger);
      }
    }
  }

  @Override
  public int next() {
    rowIndex = 0;
    while (nextLine()) {
    }
    return rowIndex;
  }

  private boolean nextLine() {
    String line;
    try {
      line = reader.readLine();
    } catch (IOException e) {
      throw UserException
          .dataReadError(e)
          .message("Error reading file:")
          .addContext("File", fileWork.getPath())
          .build(logger);
    }
    if (line == null) {
      return false;
    }
    Matcher m = pattern.matcher(line);
    if (m.matches()) {
      loadVectors(m);
    } else if (formatConfig.getErrorOnMismatch()) {
      throw UserException
          .validationError()
          .message("Line " + rowIndex + " does not match regex.")
          .addContext("Line: ", line)
          .build(logger);
    } else {
      //if (!errorFound) {
        //Increase the column data set size
        columns = addErrorFoundColumn(columns);

        NullableVarCharVector.Mutator mutator = columns[columns.length -1].mutator;
        mutator.set(rowIndex, line.getBytes());
        try {
          outputMutator.addField(errorField, NullableVarCharVector.class);
          //columns[columns.length - 1].mutator = outputMutator.addField(field, NullableVarCharVector.class).getMutator();
        } catch (SchemaChangeException e) {  //SchemaChangeException
          throw UserException
              .systemError(e)
              .message("Vector creation failed")
              .build(logger);
        }
        fieldCount++;
        errorFound = true;
      }

      //NullableVarCharVector.Mutator mutator = columns[columns.length - 1].mutator;
      //mutator.set(rowIndex, line.getBytes());
      rowIndex++;
    }
    return rowIndex < BATCH_SIZE;
  }

  private ColumnDefn[] addErrorFoundColumn(ColumnDefn[] c) {
    ColumnDefn[] columnsWithError = new ColumnDefn[fieldCount + 1];

    for (int i = 0; i < fieldCount; i++) {
      AllocationHelper.allocate( c[i], BATCH_SIZE, 256);
      columnsWithError[i] = c[i];
    }
    columnsWithError[fieldCount] = new ColumnDefn("unmatched_lines", fieldCount);
    fieldCount += 1;

    return columnsWithError;
  }

  private void loadVectors(Matcher m) {
    for (int i = 0; i < capturingGroups; i++) {
      NullableVarCharVector.Mutator mutator = columns[i].mutator;
      if (columns[i].index == -1) {
        mutator.setNull(rowIndex);
      } else {
        String value = m.group(columns[i].index + 1);
        if (value == null) {
          mutator.setNull(rowIndex);
        } else {
          mutator.set(rowIndex, value.getBytes());
        }
      }
    }
    rowIndex++;
  }

  @Override
  public void close() {
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        logger.warn("Error when closing file: " + fileWork.getPath(), e);
      }
      reader = null;
    }
  }

}