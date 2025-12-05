package org.finos.waltz.jobs.example.demodata.loaders;

import org.apache.poi.ss.usermodel.Sheet;
import org.finos.waltz.common.Columns;
import org.finos.waltz.common.StreamUtilities;
import org.finos.waltz.model.EntityKind;
import org.finos.waltz.schema.tables.records.AssessmentRatingRecord;
import org.jooq.DSLContext;
import org.jooq.lambda.tuple.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Map;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toSet;
import static org.finos.waltz.common.StreamUtilities.mkSiphon;
import static org.finos.waltz.data.JooqUtilities.summarizeResults;
import static org.finos.waltz.jobs.example.demodata.DemoUtils.*;
import static org.jooq.lambda.tuple.Tuple.tuple;

public class AssessmentRatingLoader {

    private static final Logger LOG = LoggerFactory.getLogger(AssessmentRatingLoader.class);

    public static void process(DSLContext waltz,
                               Sheet sheet,
                               Timestamp now) {

        Map<String, Long> appToIdMap = fetchAppExtIdToIdMap(waltz);
        Map<String, Long> assmentDefinationToIdMap = fetchAssessmentDefinitionExtIdToIdMap(waltz);

        StreamUtilities.Siphon<Tuple7<String,String,String, String, Long, Long ,Long>> badRowSiphon = mkSiphon(t -> t.v5 == null || t.v6 == null || t.v7 == null);

        int insertCount = summarizeResults(StreamSupport
                .stream(sheet.spliterator(), false)
                .skip(1)
                .map(row -> tuple(
                        strVal(row, Columns.A),
                        strVal(row, Columns.C),
                        strVal(row, Columns.D),
                        strVal(row, Columns.E)))
                .map(t -> tuple(
                        t.v1,
                        t.v2,
                        t.v3,
                        t.v4,
                        appToIdMap.get(t.v1),
                        assmentDefinationToIdMap.get(t.v2),
                        fetchRatingSchemeItemID(waltz,t.v2,t.v3)))
                .filter(badRowSiphon)
                .map(t -> {
                    AssessmentRatingRecord r = waltz.newRecord(ar);
                    r.setEntityId(t.v5);
                    r.setEntityKind(EntityKind.APPLICATION.name());
                    r.setAssessmentDefinitionId(t.v6);
                    r.setRatingId(t.v7);
                    r.setDescription(t.v4);
                    r.setProvenance(PROVENANCE);
                    r.setLastUpdatedAt(now);
                    r.setLastUpdatedBy(USER_ID);
                    return r;
                })
                .collect(collectingAndThen(toSet(), waltz::batchInsert))
                .execute());

        LOG.debug("Created {} assessment_ratings", insertCount);

        if (badRowSiphon.hasResults()) {
            LOG.info("Bad rows: {}", badRowSiphon.getResults());
        }
    }
}
