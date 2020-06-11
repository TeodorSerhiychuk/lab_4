package com.lab5.resteventhub.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Service
public class SendDataEventHubImpl implements SendDataService {

    private final static String CACHE_HOSTNAME = "baldblslfb.redis.cache.windows.net";
    private final static String CACHE_KEY = "YL5jU0zzMefSAOzj1fPFaWjndd5xZe1OgiboGCF4SYc=";


    public void sendAndLog(String url) throws IOException, EventHubException {



        final ConnectionStringBuilder connStr = new ConnectionStringBuilder()
                .setNamespaceName("testtestts")//namespace
                .setEventHubName("test")//hub name
                /*Connection string–primary key*/
                .setSasKeyName("Endpoint=sb://testtestts.servicebus.windows.net/;SharedAccessKeyName=Test;SharedAccessKey=IOY/eRfb4SK8sQc2ZSMulap9vtd7UGrxMqWE6kCRiIE=;EntityPath=test")
                /*Primary key*/
                .setSasKey("IOY/eRfb4SK8sQc2ZSMulap9vtd7UGrxMqWE6kCRiIE=");

        final Gson gson = new GsonBuilder().create();
        final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);
        final EventHubClient ehClient = EventHubClient.createSync(connStr.toString(), executorService);

        try {
            URL data = new URL(url);
            HttpURLConnection con = (HttpURLConnection) data.openConnection();
            int responseCode = con.getResponseCode();
            BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();
            while ((inputLine = br.readLine()) != null) {
                response.append(inputLine);
            }
            br.close();

            JSONArray jsonArray = new JSONArray(response.toString());
            showData(jsonArray, gson, ehClient);

            System.out.println(Instant.now() + ": Send Complete...");
            System.out.println("Press Enter to stop.");
            System.in.read();
        } finally {
            ehClient.closeSync();
            executorService.shutdown();
        }
    }

    public void showData(JSONArray jsonArray, Gson gson, EventHubClient ehClient) throws EventHubException {
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = (JSONObject) jsonArray.get(i);
            System.out.println("Document: " + i);
            byte[] payloadBytes = gson.toJson(jsonObject).getBytes(Charset.defaultCharset());
            EventData sendEvent = EventData.create(payloadBytes);

            ehClient.sendSync(sendEvent);
        }
    }
}
