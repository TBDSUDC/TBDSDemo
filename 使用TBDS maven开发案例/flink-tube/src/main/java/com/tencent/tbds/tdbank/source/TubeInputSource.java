package com.tencent.tbds.tdbank.source;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class TubeInputSource extends RichParallelSourceFunction {
    @Override
    public void run(SourceContext sourceContext) throws Exception {
    }

    @Override
    public void cancel() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
