package org.embulk.filter.split;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class SplitFilterPlugin
        implements FilterPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("keep_input")
        @ConfigDefault("true")
        public boolean getKeepInput();

        @Config("skip_null_or_empty")
        @ConfigDefault("true")
        public boolean getSkipNullOrEmpty();

        @Config("delimiter")
        @ConfigDefault("\",\"")
        public String getDelimiter();

        @Config("target_key")
        public String getTargetKey();

        @Config("output_key")
        public String getOutputKey();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        int i = 0;
        if (task.getKeepInput()) {
            for (Column inputColumn : inputSchema.getColumns()) {
                Column outputColumn = new Column(i++, inputColumn.getName(), inputColumn.getType());
                builder.add(outputColumn);
            }
        }
        Column outputColumn = new Column(i++, task.getOutputKey(), Types.STRING);
        builder.add(outputColumn);

        Schema outputSchema = new Schema(builder.build());
        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final Column targetColumn = inputSchema.lookupColumn(task.getTargetKey());
        final Column outputColumn = outputSchema.lookupColumn(task.getOutputKey());

        return new PageOutput() {
            private PageReader reader = new PageReader(inputSchema);
            private PageBuilder builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

            @Override
            public void finish()
            {
                builder.finish();
            }

            @Override
            public void close()
            {
                builder.close();
            }

            @Override
            public void add(Page page)
            {
                reader.setPage(page);
                while (reader.nextRecord()) {
                    List<String> words = getWords(task, targetColumn);
                    if (reader.isNull(targetColumn) || words.isEmpty()) {
                        if (task.getSkipNullOrEmpty()) {
                            continue;
                        }
                        setColumns(outputSchema, outputColumn, null);
                        builder.addRecord();
                    }
                    else {
                        for (String word : words) {
                            setColumns(outputSchema, outputColumn, word);
                            builder.addRecord();
                        }
                    }
                }
            }

            /**
             * @param task
             * @param targetColumn
             * @return
             */
            private List<String> getWords(final PluginTask task, final Column targetColumn)
            {
                List<String> words = Lists.newArrayList();
                if (targetColumn.getType().equals(Types.STRING)) {
                    words = Arrays.asList(StringUtils.split(reader.getString(targetColumn), task.getDelimiter()));
                }
                else if (targetColumn.getType().equals(Types.JSON)) {
                    final Value json = reader.getJson(targetColumn);
                    if (json.isArrayValue()) {
                        for (Value value : json.asArrayValue().list()) {
                            words.add(value.toString());
                        }
                    }
                }
                return words;
            }

            /**
             * @param outputSchema
             * @param outputColumn
             * @param word
             */
            private void setColumns(final Schema outputSchema, final Column outputColumn, String word)
            {
                for (Column column : outputSchema.getColumns()) {
                    if (column.getName().equals(outputColumn.getName())) {
                        if (word == null) {
                            builder.setNull(outputColumn);
                        }
                        else {
                            builder.setString(outputColumn, word);
                        }
                        continue;
                    }
                    setKeepColumns(column);
                }
            }

            /**
             * @param column
             */
            private void setKeepColumns(Column column)
            {
                if (reader.isNull(column)) {
                    builder.setNull(column);
                    return;
                }
                if (Types.STRING.equals(column.getType())) {
                    builder.setString(column, reader.getString(column));
                }
                else if (Types.BOOLEAN.equals(column.getType())) {
                    builder.setBoolean(column, reader.getBoolean(column));
                }
                else if (Types.DOUBLE.equals(column.getType())) {
                    builder.setDouble(column, reader.getDouble(column));
                }
                else if (Types.LONG.equals(column.getType())) {
                    builder.setLong(column, reader.getLong(column));
                }
                else if (Types.TIMESTAMP.equals(column.getType())) {
                    builder.setTimestamp(column, reader.getTimestamp(column));
                }
                else if (Types.JSON.equals(column.getType())) {
                    builder.setJson(column, reader.getJson(column));
                }
            }
        };
    }
}
