package com.practice.libraryinventoryproducer.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Book {

    @NotNull
    private Integer id;

    @NotBlank
    private String name;

    @NotBlank
    private String author;
}
