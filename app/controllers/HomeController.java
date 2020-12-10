package controllers;

import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.StaticTableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;
import software.amazon.awssdk.enhanced.dynamodb.model.CreateTableEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;

import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;

import static software.amazon.awssdk.enhanced.dynamodb.mapper.StaticAttributeTags.primaryPartitionKey;
import static software.amazon.awssdk.enhanced.dynamodb.mapper.StaticAttributeTags.primarySortKey;

/**
 * This controller contains an action to handle HTTP requests
 * to the application's home page.
 */
public class HomeController extends Controller {

    public Result index() {
        return ok(views.html.index.render());
    }

    private static final TableSchema<Customer> CUSTOMER_SCHEMA =
            StaticTableSchema.builder(Customer.class)
                    .newItemSupplier(Customer::new)
                    .addAttribute(String.class, a -> a.name("id")
                            .getter(Customer::getId)
                            .setter(Customer::setId)
                            .tags(primaryPartitionKey()))
                    .addAttribute(String.class, a -> a.name("custName")
                            .getter(Customer::getCustName)
                            .setter(Customer::setCustName)
                            .tags(primarySortKey()))
                    .addAttribute(String.class, a -> a.name("email")
                            .getter(Customer::getEmail)
                            .setter(Customer::setEmail))
                    .addAttribute(Instant.class, a -> a.name("regDate")
                            .getter(Customer::getRegistrationDate)
                            .setter(Customer::setRegistrationDate)
                    ).build();

    public CompletableFuture<Result> createTable() {

        DynamoDbAsyncTable<Customer> table = getDynamoDbAsyncTable();

        return table.createTable(CreateTableEnhancedRequest
                .builder().provisionedThroughput(
                        ProvisionedThroughput
                                .builder()
                                .readCapacityUnits(5L)
                                .writeCapacityUnits(5L)
                                .build())
                .build())
                .handle((unused, throwable) ->
                        throwable == null
                                ? ok("Table created")
                                : badRequest("Error during table creation", throwable.getMessage())
                );
    }

    public CompletableFuture<Result> putItem() {

        DynamoDbAsyncTable<Customer> table = getDynamoDbAsyncTable();

        // Create an Instant
        LocalDate     localDate     = LocalDate.parse("2020-04-07");
        LocalDateTime localDateTime = localDate.atStartOfDay();
        Instant       instant       = localDateTime.toInstant(ZoneOffset.UTC);

        // Populate the Table
        Customer custRecord = new Customer();
        custRecord.setCustName("Susan Blue");
        custRecord.setId("id103");
        custRecord.setEmail("sblue@noserver.com");
        custRecord.setRegistrationDate(instant);
        // Put the customer data into a DynamoDB table
        return table.putItem(custRecord).handle(
                (unused, throwable) ->
                        throwable == null
                                ? ok("User Created")
                                : badRequest(throwable.getMessage())
        );
    }

    public CompletableFuture<Result> getItem() {

        DynamoDbAsyncTable<Customer> table = getDynamoDbAsyncTable();

        Key key = Key.builder()
                .partitionValue("id103")
                .sortValue("Susan Blue")
                .build();

        // Get the item by using the key
        return table.getItem(r -> r.key(key))
                .handle((customer, throwable) ->
                        throwable == null
                                ? ok(Json.toJson(customer))
                                : badRequest(Json.toJson(throwable.getMessage()))
                );
    }

    private static DynamoDbAsyncTable<Customer> getDynamoDbAsyncTable() {
        DynamoDbAsyncClient dynamoDbAsyncClient = DynamoDbAsyncClient
                .builder()
                .endpointOverride(URI.create("http://localhost:8000"))
                .build();

        DynamoDbEnhancedAsyncClient enhancedAsyncClient = DynamoDbEnhancedAsyncClient
                .builder()
                .dynamoDbClient(dynamoDbAsyncClient).build();


        //This way it works
        //return enhancedAsyncClient.table("Customer", CUSTOMER_SCHEMA);
        //This way it fails
        return enhancedAsyncClient.table("Customer", TableSchema.fromClass(Customer.class));
    }


    //Create the Customer table
    @DynamoDbBean
    public static class Customer {

        private String  id;
        private String  name;
        private String  email;
        private Instant regDate;

        @DynamoDbPartitionKey
        public String getId() { return this.id; }

        public void setId(String id) { this.id = id; }

        @DynamoDbSortKey
        public String getCustName() { return this.name; }

        public void setCustName(String name) { this.name = name; }

        public String getEmail() { return this.email; }

        public void setEmail(String email) { this.email = email; }

        public Instant getRegistrationDate() { return regDate; }

        public void setRegistrationDate(Instant registrationDate) { this.regDate = registrationDate; }

        @Override
        public String toString() {
            return "Customer{" +
                    "id='" + id + '\'' +
                    ", name='" + name + '\'' +
                    ", email='" + email + '\'' +
                    ", regDate=" + regDate +
                    '}';
        }
    }



}