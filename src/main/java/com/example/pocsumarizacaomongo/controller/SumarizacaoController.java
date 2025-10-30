package com.example.pocsumarizacaomongo.controller;

import com.example.pocsumarizacaomongo.dto.SumarizatuionDto.Sumarization;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@RestController
@RequestMapping("/api")
public class SumarizacaoController {

    private final MongoTemplate mongoTemplate;

    public SumarizacaoController(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @GetMapping("/sumarization")
    public Sumarization SumarizacaoController(@RequestParam String document) throws InterruptedException, ExecutionException {
        LocalDateTime now = LocalDateTime.now();
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        Instant oneHour = Instant.from(now.minusHours(1).atZone(ZoneOffset.UTC).toInstant().atZone(ZoneId.of("America/Sao_Paulo")));
        Instant sixHours = Instant.from(now.minusHours(6).atZone(ZoneOffset.UTC).toInstant().atZone(ZoneId.of("America/Sao_Paulo")));
        Instant oneDay = Instant.from(now.minusHours(24).atZone(ZoneOffset.UTC).toInstant().atZone(ZoneId.of("America/Sao_Paulo")));

        Callable<BigDecimal> task1 = () -> new BigDecimal(getByInstant(oneHour, document));
        Callable<BigDecimal> task2 = () -> new BigDecimal(getByInstant(sixHours, document));
        Callable<BigDecimal> task3 = () -> new BigDecimal(getByInstant(oneDay, document));

        List<Future<BigDecimal>> futures = executor.invokeAll(List.of(task1, task2, task3));

        return new Sumarization(futures.get(0).get(),
                futures.get(1).get(),
                futures.get(2).get());
    }

    private String getByInstant(Instant minDate, String document) {
        // 1 - match: filtra o documento principal
        MatchOperation matchDocument = Aggregation.match(Criteria.where("document").is(document));

        // 2 - unwind: desmembra o array
        UnwindOperation unwindTransactions = Aggregation.unwind("transactions");

        // 3 - match: filtra transações por data
        MatchOperation matchDate = Aggregation.match(
                Criteria.where("transactions.createdAt").gte(minDate)
        );

        // 4 - group: soma e pega a última transação
        GroupOperation groupByDocument = Aggregation.group("document")
                .sum("transactions.value").as("totalValue")
                .max("transactions.createdAt").as("lastTransaction")
                .count().as("count");

        // 5 - project: ajusta os campos de saída
        ProjectionOperation projectFields = Aggregation.project()
                .andInclude("totalValue")
                .andExclude("_id");

        // Cria o pipeline completo
        Aggregation aggregation = Aggregation.newAggregation(
                matchDocument,
                unwindTransactions,
                matchDate,
                groupByDocument,
                projectFields
        );

        return mongoTemplate.aggregate(aggregation, "collection2", Map.class)
                .getMappedResults()
                .getFirst()
                .get("totalValue").toString();
    }
}
