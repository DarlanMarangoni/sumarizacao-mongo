package com.example.pocsumarizacaomongo.dto.SumarizatuionDto;

import java.math.BigDecimal;

public record Sumarization(
        BigDecimal oneHour,
        BigDecimal sixHours,
        BigDecimal twentyFourHours
) {
}
