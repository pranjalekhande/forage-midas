package com.jpmc.midascore.component;



import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;

import com.jpmc.midascore.foundation.Incentive;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentivesService {
	
	private final RestTemplate restTemplate;

    public IncentivesService(RestTemplateBuilder restTemplateBuilder) {
        this.restTemplate = restTemplateBuilder.build();
    }

    public Incentive fetchIncentive(Transaction transaction) {
        String url = "http://localhost:8080/incentive";
        return restTemplate.postForObject(url, transaction, Incentive.class);
    
        
    }

}
