INSERT INTO sample_table (id, value, category)
SELECT 
    number AS id,
    randNormal(50, 10) AS value,
    multiIf(
        rand() % 3 = 0, 'A',
        rand() % 3 = 1, 'B',
        'C'
    ) AS category
FROM numbers({{ params.num_rows }})
SETTINGS max_insert_block_size = 100000;
