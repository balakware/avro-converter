package io.confluent.connect.avro;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class mockHSR {

    private WireMockServer wireMockServer;

    @Before
    public void setUp() {
        // Start the WireMock server on a specific port
        wireMockServer = new WireMockServer(3080);
        wireMockServer.start();
        WireMock.configureFor("localhost", 3080);
        System.out.println("started");
    }

    @After
    public void tearDown() {
        // Stop the WireMock server
        wireMockServer.stop();
    }

    @Test
    public void testYourHttpClient() {
        // Define a stub for a specific HTTP endpoint
        WireMock.stubFor(WireMock.get(WireMock.urlEqualTo("/sample"))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "text/plain")
                        .withBody("Hello, WireMock!")));

        // Perform your HTTP client test here
        // Send a request to http://localhost:8080/your-api-endpoint
        // Assert the response

    }


}
