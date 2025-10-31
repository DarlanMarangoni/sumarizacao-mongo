package com.example.pocsumarizacaomongo.dto;

import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;
import java.util.List;

@Document("collection2")
public record Pix(
        String document,
        LocalDateTime createdAt,
        List<Transaction> transactions
) {
}
