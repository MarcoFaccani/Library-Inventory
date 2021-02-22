package com.practice.libraryinventoryconsumer.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
public class Book {

    @Id
    private Integer id;
    private String name;
    private String author;

    @OneToOne
    @JoinColumn(name = "library_event_id")
    private LibraryEvent libraryEvent;
}
