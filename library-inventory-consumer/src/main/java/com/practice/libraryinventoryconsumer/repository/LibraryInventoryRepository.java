package com.practice.libraryinventoryconsumer.repository;

import com.practice.libraryinventoryconsumer.entity.LibraryEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LibraryInventoryRepository extends JpaRepository<LibraryEvent, Integer> {
}
