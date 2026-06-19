-- -------------------------------------------
-- IBM Event Automation
-- exportedDate: 2026-06-19T13:00:34.764+0000
-- flowId: 73ac2cfd-e957-4fa6-98b1-621d9b3bf2e7
-- flowName: aggregation-migration
--
-- Important
--  *) Sensitive credential values are removed from exported SQL.
--  *) Before you deploy this SQL, update the Kafka connector properties and values
--     with the required configuration for the target environment.
-- -------------------------------------------
CREATE FUNCTION TO_TIMESTAMP_UDF AS 'com.ibm.ei.streamproc.udf.ToTimestampUdf';
CREATE FUNCTION TO_TIMESTAMP_LTZ_UDF AS 'com.ibm.ei.streamproc.udf.ToTimestampLtzUdf';

CREATE TABLE `mn-test___TABLE`
(
    `ts`                           STRING,
    `value`                        BIGINT,
    `ts___EVENT_TIME`              AS CAST (TO_TIMESTAMP_LTZ_UDF(`ts`) AS TIMESTAMP_LTZ(3)),
    WATERMARK FOR `ts___EVENT_TIME` AS `ts___EVENT_TIME` - INTERVAL '0.05' SECOND
)
WITH (
    'properties.bootstrap.servers' = 'kafka.confluent.svc.cluster.local:9092',
    'connector' = 'kafka',
    'json.ignore-parse-errors' = 'true',
    'format' = 'json',
    'topic' = 'mn-test',
    'properties.security.protocol' = 'PLAINTEXT',
    'properties.isolation.level' = 'read_committed',
    'scan.startup.mode' = 'earliest-offset'
);

CREATE TEMPORARY VIEW `mn-test` AS
SELECT
    `ts___EVENT_TIME`              AS `ts`,
    `value`                        AS `value`
FROM `mn-test___TABLE`;

CREATE TEMPORARY VIEW `hourly total` AS
SELECT
    `SUM_value`,
    `aggregateStartTime`,
    `aggregateEndTime`,
    `aggregateResultTime`
FROM (
SELECT
    SUM(`value`) AS `SUM_value`,
    `window_start` AS `aggregateStartTime`,
    `window_end` AS `aggregateEndTime`,
    `window_time` AS `aggregateResultTime`
FROM TABLE (
    TUMBLE( TABLE `mn-test`, DESCRIPTOR(`ts`), INTERVAL '1' HOUR )
)
GROUP BY
    `window_start`,
    `window_end`,
    `window_time`
);

CREATE TABLE `mn-results`
(
    `SUM_value`                    BIGINT,
    `aggregateStartTime`           TIMESTAMP(9),
    `aggregateEndTime`             TIMESTAMP(9),
    `aggregateResultTime`          TIMESTAMP_LTZ(6)
)
WITH (
    'properties.bootstrap.servers' = 'kafka.confluent.svc.cluster.local:9092',
    'connector' = 'kafka',
    'format' = 'json',
    'topic' = 'mn-results',
    'properties.security.protocol' = 'PLAINTEXT'
);

EXECUTE STATEMENT SET
BEGIN
    INSERT INTO `mn-results` SELECT `SUM_value`,`aggregateStartTime`,`aggregateEndTime`,CAST(`aggregateResultTime` AS TIMESTAMP_LTZ(6)) FROM `hourly total`;
END;
-- -------------------------------------------
