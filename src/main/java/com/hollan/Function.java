package com.hollan;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.microsoft.azure.eventhubs.*;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;
import org.json.*;

/**
 * Azure Functions with Blob trigger sending to Event Hubs in batch via SDK
 */
public class Function {
    private static ExecutorService executorService = Executors.newSingleThreadExecutor();
    private static EventHubClient _sender;

    @FunctionName("ProcessBlob")
    public void ProcessBlob(
            @BlobTrigger(name = "data", path = "results/{name}.json", connection = "AzureWebJobsStorage")String content,
            @BindingName("name")String name,
            final ExecutionContext context) 
            throws EventHubException, IOException {
        
        context.getLogger().info("Java Blob trigger function processed a blob. Name: " + name + " with content length: " + content.length());
        JSONArray jArray = new JSONArray(content);

        BatchOptions options = new BatchOptions().with( opt -> opt.partitionKey = "foo");
        EventDataBatch events = getSender().createBatch(options);

        for(Object o: jArray) {
            if(o instanceof JSONObject) {
                EventData event = EventData.create(((JSONObject)o).toString().getBytes("utf-8"));
                event.getProperties().put("from", "javaFunction");
                events.tryAdd(event);
            }
        }

        getSender().sendSync(events);
    }

    private static EventHubClient getSender() 
        throws EventHubException, IOException {
        if(_sender == null)
        {
            _sender = EventHubClient.createSync(System.getenv("EventHubConnectionString"), executorService);
        }
        return _sender;
    }
}
