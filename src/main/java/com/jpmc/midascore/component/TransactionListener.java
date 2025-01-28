package com.jpmc.midascore.component;



import com.jpmc.midascore.foundation.Transaction;
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
	
	
	@KafkaListener(topics = "${general.kafka-topic}", groupId = "${spring.kafka.consumer.group-id}")
	public void listen(Transaction transaction) {
        System.out.println("Received Transaction: " + transaction);
        System.out.println("Sender ID: " + transaction.getSenderId());
        System.out.println("Recipient ID: " + transaction.getRecipientId());
        System.out.println("Amount: " + transaction.getAmount());
        
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            System.out.println("Invalid sender or recipient ID. Transaction discarded.");
            return;
        }
        
        if (sender.getBalance() < transaction.getAmount()) {
            System.out.println("Insufficient balance. Transaction discarded.");
            return;
        }
        
     // Adjust balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        // Save changes
        userRepository.save(sender);
        userRepository.save(recipient);

        // Record the transaction
        TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(), "SUCCESS");
        transactionRecordRepository.save(record);

        System.out.println("Transaction processed successfully.");

        
        
    }

}
