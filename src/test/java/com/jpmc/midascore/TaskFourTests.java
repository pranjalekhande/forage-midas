package com.jpmc.midascore;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.UserRecord;

/*
 * 
Documentation: Task 4 - Integrating Incentives API
Objective
Integrate the Incentives API with Midas Core to fetch incentives for validated transactions. Ensure incentives are correctly applied to the recipient's balance and recorded in the transaction details.

Steps
		1. Create Incentive Class
			Represents the Incentives API response.
			Fields: amount (float).
		2. Add IncentivesService
			A Spring-managed service for communicating with the Incentives API.
			Endpoint: http://localhost:8080/incentive.
			Method: postForObject() sends a Transaction object and receives an Incentive.
		3. Update TransactionRecord Entity
			Added a new field:
			incentive (float) to store the incentive amount.
		4. Modify TransactionListener
			Validate transactions and post them to the Incentives API.
			Adjust balances:
			Deduct transaction amount from the sender.
			Add both the transaction amount and incentive to the recipient.
			Record the transaction with the incentive amount.
		5. Run the Incentives API
			Steps to Start:
			Navigate to the services folder.
			Run the JAR file:
				java -jar incentives-api.jar
			The API runs on port 8080.
		6. Run TaskFourTests
			Populate users and process transactions.
			Use the debugger to record the final balance of "Wilbur" after all transactions.
			Key Files Modified
			TransactionRecord: Added incentive field.
			TransactionListener: Integrated Incentives API and adjusted balance logic.
			IncentivesService: New service to handle API interaction.
Verification
	Successfully run TaskFourTests.
	Debug and record "Wilbur’s" final balance after all transactions.

 */


@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TaskFourTests {
    static final Logger logger = LoggerFactory.getLogger(TaskFourTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;
    

    @Autowired
    private DatabaseConduit databaseConduit;

    @Test
    void task_four_verifier() throws InterruptedException {
        userPopulator.populate();
        String[] transactionLines = fileLoader.loadStrings("/test_data/alskdjfh.fhdjsk");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        Thread.sleep(2000);


        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("use your debugger to find out what wilbur's balance is after all transactions are processed");
        logger.info("kill this test once you find the answer");
        
     // Fetch "Wilbur" user record after transactions
        UserRecord wilbur = databaseConduit.getUserByName("wilbur");
        if (wilbur != null) {
            logger.info("----------------------------------------------------------");
            logger.info("wilbur's final balance: " + wilbur.getBalance());
            logger.info("----------------------------------------------------------");
            logger.info("----------------------------------------------------------");
            logger.info("----------------------------------------------------------");
            logger.info("----------------------------------------------------------");
        } else {
            logger.error("wilbur user not found in the database.");
        }
        
        
        
        while (true) {
            Thread.sleep(20000);
            logger.info("...");
        }
    }
}
