package com.mtnfog.philter.flink.functions;

import com.mtnfog.philter.PhilterClient;
import com.mtnfog.philter.model.FilterResponse;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * Performs the filtering in a Flink map function. This class extends
 * {@link RichMapFunction} in order to be able to set up connections
 * to remote systems.
 */
public class FilterMapFunction extends RichMapFunction<String, String> {

    private PhilterClient philterClient;
    private String philterEndpoint;
    private String context;
    private String filterProfileName;

    public FilterMapFunction(String philterEndpoint, String context, String filterProfileName) {
        this.philterEndpoint = philterEndpoint;
        this.context = context;
        this.filterProfileName = filterProfileName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        this.philterClient = new PhilterClient.PhilterClientBuilder().withEndpoint(philterEndpoint).build();

    }

    @Override
    public String map(String s) throws Exception {

        final FilterResponse filterRepsonse = philterClient.filter(context, "document", filterProfileName, s);

        return filterRepsonse.getFilteredText();

    }

    @Override
    public void close() throws Exception {

    }

}