package com.jpmc.midascore.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.jpmc.midascore.entity.TransactionRecord;

@Repository
public interface TransactionRepository extends CrudRepository<TransactionRecord, Long>{
    
}
