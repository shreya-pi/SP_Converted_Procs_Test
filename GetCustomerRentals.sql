--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "rental", "inventory", "film" **
CREATE OR REPLACE PROCEDURE GetCustomerRentals (CUSTOMER_ID INT)
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
            r.rental_id,
            f.title AS film_title,
            r.rental_date,
            r.return_date

        FROM
            rental r

        INNER JOIN
                inventory i
                ON r.inventory_id = i.inventory_id

        INNER JOIN
                film f
                ON i.film_id = f.film_id

        WHERE
            r.customer_id = :CUSTOMER_ID

        ORDER BY r.rental_date DESC);
        RETURN TABLE(ProcedureResultSet);
    END;
$$;