package com.practice.libraryinventoryproducer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.practice.libraryinventoryproducer.controller.LibraryEventController;
import com.practice.libraryinventoryproducer.model.Book;
import com.practice.libraryinventoryproducer.model.LibraryEvent;
import com.practice.libraryinventoryproducer.producer.LibraryEventProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventController.class)
@AutoConfigureMockMvc
public class LibraryInventoryControllerUnitTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private LibraryEventProducer libraryEventProducer;

    @Autowired
    private ObjectMapper objectMapper;


    @Test
    public void postLibraryEvent() throws Exception {

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .id(null)
                .type(LibraryEvent.Type.NEW)
                .book(Book.builder()
                        .author("Patrick O'Brian")
                        .name("Master and Commander")
                        .id(346)
                        .build())
                .build();

        doNothing().when(libraryEventProducer).sendLibraryEvent(isA(LibraryEvent.class));

        mockMvc.perform(post("/api/v1/library-event")
                .content(objectMapper.writeValueAsString(libraryEvent))
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());
    }


    // test input validation #1 of 2
    @Test
    public void postLibraryEvent_NullBook() throws Exception {

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .id(null)
                .type(LibraryEvent.Type.NEW)
                .book(null)
                .build();

        doNothing().when(libraryEventProducer).sendLibraryEvent(isA(LibraryEvent.class));

        mockMvc.perform(post("/api/v1/library-event")
                .content(objectMapper.writeValueAsString(libraryEvent))
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string("book: must not be null"));

    }

    // test input validation #2
    @Test
    public void postLibraryEvent_EmptyBook() throws Exception {

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .id(null)
                .type(LibraryEvent.Type.NEW)
                .book(new Book())
                .build();

        doNothing().when(libraryEventProducer).sendLibraryEvent(isA(LibraryEvent.class));

        mockMvc.perform(post("/api/v1/library-event")
                .content(objectMapper.writeValueAsString(libraryEvent))
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string("book.author: must not be blank; book.id: must not be null; book.name: must not be blank"));

    }

}
