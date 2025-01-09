CREATE OR REPLACE FUNCTION jaro_winkler_similarity(str1 TEXT, str2 TEXT)
RETURNS FLOAT AS $$
DECLARE
    len1 INT := LENGTH(str1);
    len2 INT := LENGTH(str2);
    max_distance INT := GREATEST(len1, len2) / 2 - 1;
    matches INT := 0;
    transpositions INT := 0;
    prefix INT := 0;
    i INT;
    j INT;
    matched1 BOOLEAN[];
    matched2 BOOLEAN[];
    jaro_similarity FLOAT;
BEGIN
    IF len1 = 0 AND len2 = 0 THEN
        RETURN 1.0;
    END IF;

    matched1 := ARRAY_FILL(FALSE, ARRAY[len1]);
    matched2 := ARRAY_FILL(FALSE, ARRAY[len2]);

    -- Count matches
    FOR i IN 1..len1 LOOP
        FOR j IN GREATEST(1, i - max_distance)..LEAST(len2, i + max_distance) LOOP
            IF NOT matched2[j] AND SUBSTRING(str1, i, 1) = SUBSTRING(str2, j, 1) THEN
                matched1[i] := TRUE;
                matched2[j] := TRUE;
                matches := matches + 1;
                EXIT;
            END IF;
        END LOOP;
    END LOOP;

    IF matches = 0 THEN
        RETURN 0.0;
    END IF;

    -- Count transpositions
    j := 1;
    FOR i IN 1..len1 LOOP
        IF matched1[i] THEN
            WHILE NOT matched2[j] LOOP
                j := j + 1;
            END LOOP;
            IF SUBSTRING(str1, i, 1) != SUBSTRING(str2, j, 1) THEN
                transpositions := transpositions + 1;
            END IF;
            j := j + 1;
        END IF;
    END LOOP;
    transpositions := transpositions / 2;

    jaro_similarity := (matches::FLOAT / len1 +
                        matches::FLOAT / len2 +
                        (matches - transpositions)::FLOAT / matches) / 3;

    -- Compute prefix for Winkler boots
    FOR i IN 1..LEAST(len1, len2) LOOP
        IF SUBSTRING(str1, i, 1) = SUBSTRING(str2, i, 1) THEN
            prefix := prefix + 1;
        ELSE
            EXIT;
        END IF;
        IF prefix >= 4 THEN
            EXIT;
        END IF;
    END LOOP;

    -- Apply Winkler boost
    RETURN jaro_similarity + (prefix * 0.1 * (1 - jaro_similarity));
END;
$$ LANGUAGE plpgsql;
