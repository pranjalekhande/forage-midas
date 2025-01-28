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
 
Documentation: Enhancements to Midas Core for Transaction Validation and User Balance Recording
Objective
Enhance the Midas Core application to validate and process Kafka transactions, record them in the database, and log the balance of a specific user ("Waldorf") after all transactions are processed.

Key Changes
Created TransactionRecord Entity

Purpose: Store validated transactions in the database.
Fields:
sender (Many-to-One relationship with UserRecord)
recipient (Many-to-One relationship with UserRecord)
amount (float)
status ("SUCCESS" or "FAILED")
Updated TransactionListener

Purpose:
Validate transactions (sender/recipient exists, sufficient sender balance).
Adjust sender and recipient balances if valid.
Record successful transactions in the database using TransactionRecord.
Outcome: Discards invalid transactions without modifying the database.
Added getUserByName in DatabaseConduit

Purpose: Retrieve UserRecord by name for specific use cases.
Usage: Used in TaskThreeTests to fetch and log Waldorf’s balance.
Enhanced UserRepository

Added findByName(String name) method to fetch users by name.
Modified TaskThreeTests

Steps:
Populate users from /test_data/lkjhgfdsa.hjkl.
Process transactions from /test_data/mnbvcxz.vbnm using Kafka.
Fetch Waldorf's record and log their final balance.
Validation: Ensures transaction processing updates user balances correctly.

 */



@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TaskThreeTests {
    static final Logger logger = LoggerFactory.getLogger(TaskThreeTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;
    
    @Autowired
    private DatabaseConduit databaseConduit;

    @Test
    void task_three_verifier() throws InterruptedException {
        userPopulator.populate();
        String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        Thread.sleep(2000);


        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("use your debugger to find out what waldorf's balance is after all transactions are processed");
        logger.info("kill this test once you find the answer");
                
        
     // Fetch "Waldorf" user record after transactions
        UserRecord waldorf = databaseConduit.getUserByName("waldorf");
        if (waldorf != null) {
            logger.info("----------------------------------------------------------");
            logger.info("Waldorf's final balance: " + waldorf.getBalance());
            logger.info("----------------------------------------------------------");
        } else {
            logger.error("Waldorf user not found in the database.");
        }
        
        Thread.sleep(20000);
    }
}
