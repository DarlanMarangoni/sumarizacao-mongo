package com.example.pocsumarizacaomongo.controller;

import com.example.pocsumarizacaomongo.dto.CreateDto;
import com.example.pocsumarizacaomongo.dto.Pix;
import com.example.pocsumarizacaomongo.dto.SumarizatuionDto.Sumarization;
import com.example.pocsumarizacaomongo.dto.Transaction;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
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

        Callable<BigDecimal> task1 = () -> new BigDecimal(getByInstant(now.minusHours(1), document));
        Callable<BigDecimal> task2 = () -> new BigDecimal(getByInstant(now.minusHours(6), document));
        Callable<BigDecimal> task3 = () -> new BigDecimal(getByInstant(now.minusHours(24), document));

        List<Future<BigDecimal>> futures = executor.invokeAll(List.of(task1, task2, task3));

        return new Sumarization(futures.get(0).get(),
                futures.get(1).get(),
                futures.get(2).get());
    }

    @PostMapping("/sumarization")
    private void create(@RequestBody CreateDto createDto) {

        Transaction[] transactions = new Transaction[createDto.amount()];

        for (int i = 0; i < createDto.amount(); i++) {
            transactions[i] = new Transaction(
                    1.0,
                    LocalDateTime.now().toInstant(ZoneOffset.UTC)
            );
        }
        mongoTemplate.save(new Pix(createDto.document(), LocalDateTime.now(), Arrays.stream(transactions).toList()));
    }

    private String getByInstant(LocalDateTime minDate, String document) {
        // 1 - match: filtra o documento principal
        MatchOperation matchDocument = Aggregation.match(Criteria.where("document").is(document));

        // 2 - unwind: desmembra o array
        UnwindOperation unwindTransactions = Aggregation.unwind("transactions");

        // 3 - match: filtra transações por data
        MatchOperation matchDate = Aggregation.match(
                Criteria.where("transactions.createdAt").gte(minDate.toInstant(ZoneOffset.UTC))
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
