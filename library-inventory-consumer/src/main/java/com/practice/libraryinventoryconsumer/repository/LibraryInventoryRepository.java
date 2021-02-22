package com.practice.libraryinventoryconsumer.repository;

import com.practice.libraryinventoryconsumer.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LibraryInventoryRepository extends CrudRepository<LibraryEvent, Integer> {
}
