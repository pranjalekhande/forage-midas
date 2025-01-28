package com.jpmc.midascore.component;



import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;




@Component
public class TransactionListener {
	
	private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;

	public TransactionListener(UserRepository userRepository, TransactionRecordRepository transactionRecordRepository) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
    }
	
	
	@Autowired
	private IncentivesService incentivesService;
	
	@KafkaListener(topics = "${general.kafka-topic}", groupId = "${spring.kafka.consumer.group-id}")
	public void listen(Transaction transaction) {
		
		
        System.out.println("Received Transaction: " + transaction);
        System.out.println("Sender ID: " + transaction.getSenderId());
        System.out.println("Recipient ID: " + transaction.getRecipientId());
        System.out.println("Amount: " + transaction.getAmount());
        
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null || sender.getBalance() < transaction.getAmount()) {
            System.out.println("Invalid transaction. Discarded.");
            return;
        }
        
        if (sender.getBalance() < transaction.getAmount()) {
            System.out.println("Insufficient balance. Transaction discarded.");
            return;
        }
        
        // Adjust sender balance
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        
        // Fetch incentive
        Incentive incentive = incentivesService.fetchIncentive(transaction);
        
        
        

        // Adjust recipient balance
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentive.getAmount());

        
        // Save changes
        userRepository.save(sender);
        userRepository.save(recipient);

        // Record the transaction
        TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(),incentive.getAmount(), "SUCCESS");
        transactionRecordRepository.save(record);

        System.out.println("Transaction processed with incentive: " + incentive.getAmount());

        
        
    }

}
