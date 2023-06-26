MERGE INTO raw.tiwh_part_number_plant_assignment AS orig 
    USING (
        WITH bugs as (
            SELECT
                counter, product_id, using_plant,
                count(known_until) as cnt
            FROM raw.tiwh_part_number_plant_assignment
            WHERE _latest = true 
            GROUP BY counter, product_id, using_plant
            HAVING count(known_until) > 1
        )
            SELECT orig.counter, orig.product_id, orig.using_plant,
                coalesce(LEAD(orig.AN_insert_timestamp) OVER (PARTITION BY orig.counter, orig.product_id, orig.using_plant ORDER BY orig.AN_insert_timestamp ASC), orig.known_until) as known_until,
                orig.AN_insert_timestamp,
                orig.change_mode
            FROM raw.tiwh_part_number_plant_assignment orig
            JOIN bugs ON
                coalesce(orig.counter, 'xx') = coalesce(bugs.counter, 'xx')
                AND coalesce(orig.product_id, 'xx') = coalesce(bugs.product_id, 'xx')
                AND coalesce(orig.using_plant, 'xx') = coalesce(bugs.using_plant, 'xx')
            WHERE orig._latest = true
    ) AS fix
        ON (
            coalesce(orig.counter, 'xx') = coalesce(fix.counter, 'xx')
            AND coalesce(orig.product_id, 'xx') = coalesce(fix.product_id, 'xx')
            AND coalesce(orig.using_plant, 'xx') = coalesce(fix.using_plant, 'xx')
            AND orig._latest = true
            AND orig.change_mode = 'D'
            AND orig.AN_insert_timestamp = fix.AN_insert_timestamp
        ) 
WHEN MATCHED THEN 
    UPDATE SET 
        _latest = false,
        known_until = fix.known_until;
