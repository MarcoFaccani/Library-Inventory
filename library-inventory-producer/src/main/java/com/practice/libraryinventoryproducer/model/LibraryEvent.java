package com.practice.libraryinventoryproducer.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class LibraryEvent {

    public enum Type {
        NEW, UPDATE
    }

    private Integer id;
    private Type type;

    @NotNull
    @Valid
    private Book book;

}
