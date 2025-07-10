USE DATABASE dgwconfiguration;

--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "MasterTableList", "MasterTableColumnList" **
CREATE OR REPLACE PROCEDURE dbo.usp_refresh_comparison_scripts ()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 2,  "patch": "6.0" }, "attributes": {  "component": "transact",  "convertedOn": "06-27-2025",  "domain": "test" }}'
EXECUTE AS CALLER
AS
$$
	DECLARE
		SRCDB VARCHAR(255);
		SRCSCHEMA VARCHAR(255);
		SRCTABLE VARCHAR(255);
		DESTDB VARCHAR(255);
		DESTSCHEMA VARCHAR(255);
		DESTTABLE VARCHAR(255);
		COMPARISONSQL VARCHAR;
		KEYCOLUMNS VARCHAR;
		SYSTEMCOLS VARCHAR;
		UPDATECOLUMNLIST VARCHAR;
		FULLSRCCOLUMNLIST VARCHAR;
		FULLDESTCOLUMNLIST VARCHAR;
		TABLEID INT;
		DELIMITER VARCHAR(100) := ',';
		HASHCOLUMNS VARCHAR;

		-- Cursor to loop through enabled tables
		--** SSC-FDM-TS0013 - SNOWFLAKE SCRIPTING CURSOR ROWS ARE NOT MODIFIABLE **
		TableCursor CURSOR
		FOR
			SELECT
				tableId,
				srcDbName,
				srcSchemaName,
				srcTableName,
				destDbName,
				destSchemaName,
				destTableName,
				keyColumns
			FROM
				MasterTableList
			WHERE
				enabled = 1 and RefreshComparisonLogic = 1;
	BEGIN
		 
		 
		 
		 
		 
		 
		 
		 
		 
		 
		 
		 
		 
		 
		 

		-- System columns
		SYSTEMCOLS := 'Row_Hash' || :DELIMITER || 'Eff_Start_timestamp' || :DELIMITER || 'Eff_End_timestamp' || :DELIMITER || 'Is_deleted' || :DELIMITER || 'Is_current';
		 
		OPEN TableCursor;
		FETCH
			TableCursor
			INTO
			:TABLEID,
			:SRCDB,
			:SRCSCHEMA,
			:SRCTABLE,
			:DESTDB,
			:DESTSCHEMA,
			:DESTTABLE,
			:KEYCOLUMNS;
			WHILE (:FETCH_STATUS = 0) LOOP

			--Get the column List
			select
				!!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'STRING_AGG' NODE ***/!!!
				STRING_AGG(columnName, :DELIMITER)
			INTO
				:FULLSRCCOLUMNLIST
			from
				MasterTableColumnList
			where
				tableId = :TABLEID
				and AddtoComparisonLogic = 1;
			FULLDESTCOLUMNLIST := :FULLSRCCOLUMNLIST || :DELIMITER || :SYSTEMCOLS;
			--  Add Row_Hash column to source table if not exists
			COMPARISONSQL := 'IF NOT EXISTS ( SELECT 1 FROM [' || :SRCDB || '].INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_SCHEMA = ''' || :SRCSCHEMA || ''' 
		AND TABLE_NAME = ''' || :SRCTABLE || '''
		AND COLUMN_NAME = ''Row_Hash'')
		 ALTER TABLE [' || :SRCDB || '].[' || :SRCSCHEMA || '].[' || :SRCTABLE || '] ADD Row_Hash VARBINARY(8000);' || CHAR(13) || CHAR(13);




			   -------------------- ADDITION: Update Row_Hash column in source with HASHBYTES over all business columns ----------------------

			   SELECT
				!!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'STRING_AGG' NODE ***/!!!
				STRING_AGG('ISNULL(CAST([' + columnName + '] AS NVARCHAR(MAX)),'''')', '+ '' | '' + ')
			INTO
				:HASHCOLUMNS
			   FROM
				MasterTableColumnList
			   WHERE
				tableId = :TABLEID
				and AddtoComparisonLogic = 1;
			COMPARISONSQL := :COMPARISONSQL || ' UPDATE [' || :SRCDB || '].[' || :SRCSCHEMA || '].[' || :SRCTABLE || '] 
                          SET Row_Hash = HASHBYTES(''SHA2_256'', ' || :HASHCOLUMNS || ');' || CHAR(13) || CHAR(13);
			---pick Key columns
			IF (:KEYCOLUMNS IS NULL) THEN
				BEGIN
					-- Step 2: Add system columns
					KEYCOLUMNS := :FULLSRCCOLUMNLIST;
					UPDATECOLUMNLIST := '';
				END;
			ELSE
				BEGIN
					select
						!!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'STRING_AGG' NODE ***/!!!
						STRING_AGG(FCL.value,',')
					INTO
						:UPDATECOLUMNLIST
 from STRING_SPLIT(@FullSrcColumnList,@Delimiter) as FCL !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'TableValuedFunctionCall' NODE ***/!!!
					LEFT JOIN STRING_SPLIT(@KeyColumns,@Delimiter) as KCL !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'TableValuedFunctionCall' NODE ***/!!! on KCL.value = FCL.value
					where
						KCL.value is null;
				END;
			END IF;

			-----------Step 4: Build SQL
-----Get Source and Targets
			COMPARISONSQL := :COMPARISONSQL || ' ;WITH TARGET AS ' || CHAR(13) || '( SELECT * FROM [' || :DESTDB || '].[' || :DESTSCHEMA || '].[' || :DESTTABLE || '] where Is_current=''Y'')' || CHAR(13) || CHAR(13) || 'MERGE  TARGET
		USING [' || :SRCDB || '].[' || :SRCSCHEMA || '].[' || :SRCTABLE || '] AS SOURCE
				ON ';
-----Prepare Join conditions----Phase 2 we will get coalesce if required here
COMPARISONSQL := :COMPARISONSQL +
						 (SELECT
					!!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'STRING_AGG' NODE ***/!!!
					STRING_AGG(' TARGET.' + KCL.value + ' = SOURCE.' + KCL.value,' AND ' || CHAR(13))
						 FROM STRING_SPLIT(@KeyColumns,@Delimiter) as KCL !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'TableValuedFunctionCall' NODE ***/!!!
						 ) + CHAR(13);
------------WHEN Matched then Update
COMPARISONSQL := :COMPARISONSQL ||
' WHEN MATCHED and TARGET.Row_Hash <> SOURCE.Row_Hash THEN 
				UPDATE SET Eff_End_timestamp=GETDATE(), 
					Is_current = ''N''' || CHAR(13) || CHAR(13);
--------------When Not Matched
COMPARISONSQL := :COMPARISONSQL ||
' WHEN NOT MATCHED BY TARGET THEN 
			INSERT (' || :FULLDESTCOLUMNLIST || ')
			VALUES (' + REPLACE(:FULLSRCCOLUMNLIST, :DELIMITER, :DELIMITER || 'SOURCE.') + ',Row_Hash'+ :DELIMITER +'getdate()'+ :DELIMITER +'''9999-12-31'''+ :DELIMITER +'''N''' + :DELIMITER +'''Y''' + ')' + CHAR(13) + CHAR(13);
---------------When NOT Matched by Source 
COMPARISONSQL := :COMPARISONSQL ||
'WHEN NOT MATCHED BY SOURCE THEN 
			UPDATE SET Eff_End_timestamp=GETDATE(), 
					Is_current = ''N'',
					Is_deleted = ''Y'';' || CHAR(13);
---------------Add changed records
COMPARISONSQL := :COMPARISONSQL || CHAR(13) || CHAR(13) || '------Add changed records' || CHAR(13) ||
'INSERT into [' || :DESTDB || '].[' || :DESTSCHEMA || '].[' || :DESTTABLE || '] (' || :FULLDESTCOLUMNLIST || ')
			SELECT SOURCE.' + REPLACE(:FULLSRCCOLUMNLIST, :DELIMITER, :DELIMITER || 'SOURCE.') + ',SOURCE.Row_Hash'+ :DELIMITER +'getdate()'+ :DELIMITER +'''9999-12-31'''+ :DELIMITER +'''N''' + :DELIMITER +'''Y''' + CHAR(13) + ' from [' + :SRCDB + '].[' + :SRCSCHEMA + '].[' + :SRCTABLE + '] as SOURCE 
			LEFT JOIN [' + :DESTDB + '].[' + :DESTSCHEMA + '].[' + :DESTTABLE + '] AS TARGET
			ON ';
COMPARISONSQL := :COMPARISONSQL || ' TARGET.is_current=''Y'' AND ' +
			           (SELECT
					!!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'STRING_AGG' NODE ***/!!!
					STRING_AGG(' TARGET.' + KCL.value + ' = SOURCE.' + KCL.value,' AND ' || CHAR(13))
						 FROM STRING_SPLIT(@KeyColumns,@Delimiter) as KCL !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'TableValuedFunctionCall' NODE ***/!!!
						 ) + CHAR(13) + ' WHERE TARGET.Eff_Start_timestamp is null;' + CHAR(13) + CHAR(13);

			       -- Step 5: Store in comparisonLogic
			       UPDATE MasterTableList
			       SET
					comparisonLogic = :COMPARISONSQL
			       WHERE
					TableId = :TABLEID;
-- Move to next row
--** SSC-PRF-0003 - FETCH INSIDE A LOOP IS CONSIDERED A COMPLEX PATTERN, THIS COULD DEGRADE SNOWFLAKE PERFORMANCE. **
FETCH
				TableCursor
INTO
				:TABLEID,
				:SRCDB,
				:SRCSCHEMA,
				:SRCTABLE,
				:DESTDB,
				:DESTSCHEMA,
				:DESTTABLE,
				:KEYCOLUMNS;
			END LOOP;
			CLOSE TableCursor;
			!!!RESOLVE EWI!!! /*** SSC-EWI-0058 - FUNCTIONALITY FOR 'DEALLOCATE' IS NOT CURRENTLY SUPPORTED BY SNOWFLAKE SCRIPTING ***/!!!
			DEALLOCATE TableCursor;
	END;
$$;