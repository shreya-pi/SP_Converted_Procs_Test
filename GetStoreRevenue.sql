--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "payment", "rental", "staff", "store" **
CREATE OR REPLACE PROCEDURE GetStoreRevenue (STORE_ID INT)
RETURNS TABLE()
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 2,  "patch": "6.0" }, "attributes": {  "component": "transact",  "convertedOn": "06-27-2025",  "domain": "test" }}'
EXECUTE AS CALLER
AS
$$
    DECLARE
        ProcedureResultSet RESULTSET;
    BEGIN
        !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

        SET NOCOUNT ON;
        ProcedureResultSet := (
        SELECT
            s.store_id,
            SUM(p.amount) AS total_revenue

        FROM
            payment p

        INNER JOIN
                rental r
                ON p.rental_id = r.rental_id

        INNER JOIN
                staff st
                ON p.staff_id = st.staff_id

        INNER JOIN
                store s
                ON st.store_id = s.store_id

        WHERE
            s.store_id = :STORE_ID
        GROUP BY
            s.store_id);
        RETURN TABLE(ProcedureResultSet);
    END;
$$;