package org.embulk.filter.split;

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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

public class SplitFilterPlugin
        implements FilterPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("keep_input")
        @ConfigDefault("true")
        public boolean getKeepInput();

        @Config("delimiter")
        @ConfigDefault("\",\"")
        public String getDelimiter();

        @Config("target_key")
        public String getTargetKey();

        @Config("output_key")
        @ConfigDefault("null")
        public Optional<String> getOutputKey();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        int i = 0;
        if (task.getKeepInput()) {
            for (Column inputColumn: inputSchema.getColumns()) {
                Column outputColumn = new Column(i++, inputColumn.getName(), inputColumn.getType());
                builder.add(outputColumn);
            }
        }
        if (task.getOutputKey().isPresent()) {
            Column outputColumn = new Column(i++, task.getOutputKey().get(), Types.STRING);
            builder.add(outputColumn);
        }
        Schema outputSchema = new Schema(builder.build());
        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema,
            Schema outputSchema, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        final Column targetColumn = inputSchema.lookupColumn(task.getTargetKey());
        final Column outputColumn = task.getOutputKey().isPresent() ? 
            outputSchema.lookupColumn(task.getOutputKey().get()) :
            outputSchema.lookupColumn(task.getTargetKey());

        return new PageOutput() {
            private PageReader reader = new PageReader(inputSchema);
            private PageBuilder builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

            @Override
            public void finish() {
                builder.finish();
            }

            @Override
            public void close() {
                builder.close();
            }

            @Override
            public void add(Page page) {
                reader.setPage(page);
                while (reader.nextRecord()) {
                    String[] words = StringUtils.split(reader.getString(targetColumn),task.getDelimiter());
                    for (String word : words) {
                        for (Column column: outputSchema.getColumns()) {
                            if (column.getName().equals(outputColumn.getName())) {
                                builder.setString(outputColumn, word);
                                continue;
                            }
                            if (reader.isNull(column)) {
                                builder.setNull(column);
                                continue;
                            }
                            if (Types.STRING.equals(column.getType())) {
                                builder.setString(column, reader.getString(column));
                            } else if (Types.BOOLEAN.equals(column.getType())) {
                                builder.setBoolean(column, reader.getBoolean(column));
                            } else if (Types.DOUBLE.equals(column.getType())) {
                                builder.setDouble(column, reader.getDouble(column));
                            } else if (Types.LONG.equals(column.getType())) {
                                builder.setLong(column, reader.getLong(column));
                            } else if (Types.TIMESTAMP.equals(column.getType())) {
                                builder.setTimestamp(column, reader.getTimestamp(column));
                            }
                        }
                        builder.addRecord();
                    }
                }
            }
        };
    }
}
