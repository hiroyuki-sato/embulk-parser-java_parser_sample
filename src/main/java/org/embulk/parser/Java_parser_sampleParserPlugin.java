package org.embulk.parser;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
//
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
// import following class if you use LineDecoder;
import org.embulk.spi.util.LineDecoder;
// import following class if you use TimestampParser;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.time.TimestampParseException;
//
import org.embulk.spi.ColumnConfig;
//
import java.util.ArrayList;
//
import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;


public class Java_parser_sampleParserPlugin
        implements ParserPlugin
{
    public interface PluginTask
            extends Task, LineDecoder.DecoderTask, TimestampParser.ParserTask
    {
/*
        @Config("property1")
        public String getProperty1();

        @Config("property2")
        @ConfigDefault("0")
        public int getProperty2();

        // TODO get schema from config or data source
        @Config("columns")
        public SchemaConfig getColumns();
*/
    }

    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        ArrayList<ColumnConfig> columns = new ArrayList<ColumnConfig>();
        columns.add(new ColumnConfig("hoge",STRING ,null));

        Schema schema = new SchemaConfig(columns).toSchema();
        //
        // Schema
        //
        //Schema schema = task.getColumns().toSchema();

        control.run(task.dump(), schema);
    }

    @Override
    public void run(TaskSource taskSource, Schema schema,
            FileInput input, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        LineDecoder lineDecoder = new LineDecoder(input,task);
        PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output);
        String line = null;

        while( input.nextFile() ){
            while(true){
              line = lineDecoder.poll();
              if( line == null ){
                  break;
              }

              // TODO
              pageBuilder.setString(0,line);
              pageBuilder.addRecord();
            }
        }
        pageBuilder.finish();
    }
}
