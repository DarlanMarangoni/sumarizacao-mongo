package com.example.pocsumarizacaomongo.dto;

public record Transaction(
        Double value,
        java.time.Instant createdAt
) {
}
