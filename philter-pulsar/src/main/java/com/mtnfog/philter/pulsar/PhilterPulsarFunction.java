package com.mtnfog.philter.pulsar;

import com.mtnfog.philter.PhilterClient;
import com.mtnfog.philter.model.FilterResponse;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.io.IOException;

public class PhilterPulsarFunction implements Function<String, String> {

    private PhilterClient philterClient;
    private String context;
    private String filterProfileName;

    public PhilterPulsarFunction(String philterEndpoint, String context, String filterProfileName) throws IOException {

        this.philterClient = new PhilterClient.PhilterClientBuilder().withEndpoint(philterEndpoint).build();
        this.context = context;
        this.filterProfileName = filterProfileName;

    }

    @Override
    public String process(String s, Context ctx) throws Exception {

        final FilterResponse filterRepsonse = philterClient.filter(context, "document", filterProfileName, s);

        return filterRepsonse.getFilteredText();

    }

}