package com.serverless;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

public class Handler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

	private static final Logger LOG = Logger.getLogger(Handler.class);
	
	@Override
	public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
//		 System.out.println("Log message: " + input);
//		LOG.info("received: {}" + input);
		Response responseBody = new Response("Go Serverless v1.x! Your function executed successfully!", input);
		System.out.println("Listing your Amazon DynamoDB tables:\n");
//        Region region = Region.US_EAST_1;
//        DynamoDbClient ddb = DynamoDbClient.builder()
//                .region(region)
//                .build();
//        List<String> a = listAllTables(ddb);
//        Map<String, Object> b = new HashMap<>();
//        b.put("a", a);
//        Response responseBody = new Response("Go Serverless v1.x! Your function executed successfully!", responseBody);
//        ddb.close();
		
		try {
			 System.out.println("do 1" );
            String port = "8000";
            String uri = "localhost:" + port;
//            // Create an in-memory and in-process instance of DynamoDB Local that runs over HTTP
//            final String[] localArgs = {"-inMemory", "-port", port};
//            System.out.println("Starting DynamoDB Local...");
//            server = ServerRunner.createServerFromCommandLineArgs(localArgs);
//            server.start();

            //  Create a client and connect to DynamoDB Local
            //  Note: This is a dummy key and secret and AWS_ACCESS_KEY_ID can contain only letters (A–Z, a–z) and numbers (0–9).
            DynamoDbClient ddbClient = DynamoDbClient.builder()
                    .endpointOverride(URI.create("http://localhost:8000"))
                    .httpClient(UrlConnectionHttpClient.builder().build())
                    .region(Region.US_EAST_1)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                    .build();

            String tableName = "Music2";
            String keyName = "Artist2";
            System.out.println("do 2" );
            // Create a table in DynamoDB Local with table name Music and partition key Artist
            // Understanding core components of DynamoDB: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html
            boolean check = existsTable(ddbClient, tableName);
            String key1 = "No One you knowww";
            String key2 = "The Beatlesss";
            if(!check) {
            	createTable(ddbClient, tableName, keyName);
            	 putItemInTable(ddbClient, tableName, keyName, key1, "albumTitle", "The Colour And The Shape", "awards", "awardVal", "songTitle", "songTitleVal");
                 putItemInTable(ddbClient, tableName, keyName, key2, "albumTitle", "Let It Be", "awards", "awardVal", "songTitle", "songTitleVal");

            }
            System.out.println("do 3" );
            System.out.println("vào 2" );
            getDynamoDBItem(ddbClient, tableName, keyName, key1);
		} catch (Exception e) {
			System.out.println(e.getMessage());
            System.err.println(e.getMessage());
            System.exit(1);
      }
		return ApiGatewayResponse.builder()
				.setStatusCode(200)
				.setObjectBody(responseBody)
				.setHeaders(Collections.singletonMap("X-Powered-By", "AWS Lambda & serverless"))
				.build();
	}
	
	public void putItemInTable(DynamoDbClient ddb, String tableName, String key, String keyVal,
			String albumTitle, String albumTitleValue, String awards, String awardVal, String songTitle,
			String songTitleVal) {

		HashMap<String, AttributeValue> itemValues = new HashMap<String, AttributeValue>();

// Add all content to the table
		itemValues.put(key, AttributeValue.builder().s(keyVal).build());
		itemValues.put(songTitle, AttributeValue.builder().s(songTitleVal).build());
		itemValues.put(albumTitle, AttributeValue.builder().s(albumTitleValue).build());
		itemValues.put(awards, AttributeValue.builder().s(awardVal).build());

		PutItemRequest request = PutItemRequest.builder().tableName(tableName).item(itemValues).build();

		try {
			ddb.putItem(request);
		} catch (ResourceNotFoundException e) {
			System.err.format("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", tableName);
			System.err.println("Be sure that it exists and that you've typed its name correctly!");
			System.exit(1);
		} catch (DynamoDbException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}
	
	 public boolean existsTable(DynamoDbClient ddbClient, String tableName) {
	        try {
	            // Describe the table to check if it exists
	            DescribeTableRequest describeTableRequest = DescribeTableRequest.builder()
	                    .tableName(tableName)
	                    .build();
	            ddbClient.describeTable(describeTableRequest);
	            return true;
	        } catch (ResourceNotFoundException e) {
	            // Table not found, return false
	            return false;
	        }
	    }
	
    public void getDynamoDBItem(DynamoDbClient ddb, String tableName, String key, String keyVal) {

        HashMap<String, AttributeValue> keyToGet = new HashMap<String, AttributeValue>();

        keyToGet.put(key, AttributeValue.builder()
                .s(keyVal).build());

        GetItemRequest request = GetItemRequest.builder()
                .key(keyToGet)
                .tableName(tableName)
                .consistentRead(true)
                .build();

        try {
            Map<String, AttributeValue> returnedItem = ddb.getItem(request).item();

            if (returnedItem.size() != 0) {
                Set<String> keys = returnedItem.keySet();
                for (String key1 : keys) {
                    System.out.format("%s: %s\n", key1, returnedItem.get(key1).s());
                }
            } else {
                System.out.format("No item found with the key: %s!\n", keyToGet.get(key).s());
            }
        } catch (DynamoDbException e) {
            System.err.println(e.toString());
            System.exit(1);
        }
    }

	   public  String createTable(DynamoDbClient ddb, String tableName, String key) {
		   System.out.println("do 4" );
	        DynamoDbWaiter dbWaiter = ddb.waiter();
	        CreateTableRequest request = CreateTableRequest.builder()
	                .attributeDefinitions(AttributeDefinition.builder()
	                        .attributeName(key)
	                        .attributeType(ScalarAttributeType.S)
	                        .build())
	                .keySchema(KeySchemaElement.builder()
	                        .attributeName(key)
	                        .keyType(KeyType.HASH)
	                        .build())
	                .provisionedThroughput(ProvisionedThroughput.builder()
	                        .readCapacityUnits(Long.valueOf(5))
	                        .writeCapacityUnits(Long.valueOf(5))
	                        .build())
	                .tableName(tableName)
	                .build();

	        String newTable = "";
	        try {
	        	 System.out.println("do 5" );
	            CreateTableResponse response = ddb.createTable(request);
	            DescribeTableRequest tableRequest = DescribeTableRequest.builder()
	                    .tableName(tableName)
	                    .build();
	            System.out.println("do 6" );
	            // Wait until the Amazon DynamoDB table is created
	            WaiterResponse<DescribeTableResponse> waiterResponse = dbWaiter.waitUntilTableExists(tableRequest);
	            waiterResponse.matched().response().ifPresent(System.out::println);
	            System.out.println("do 7" );
	            newTable = response.tableDescription().tableName();
	            System.out.println("do 8" );
	            return newTable;

	        } catch (DynamoDbException e) {
	        	 System.out.println(e.getMessage());
	            System.out.println(e.toString());
	            System.exit(1);
	        }
	        return "";
	    }
}
