USE DATABASE DW_CONFIGURATION;

--/****** Object:  StoredProcedure [dbo].[USP_Prepare_Executable_Snapshot]    Script Date: 6/27/2025 12:39:19 PM ******/
----** SSC-FDM-TS0027 - SET ANSI_NULLS ON STATEMENT MAY HAVE A DIFFERENT BEHAVIOR IN SNOWFLAKE **
--SET ANSI_NULLS ON

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

SET QUOTED_IDENTIFIER ON;

--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "dbo.[Executable List]", "dbo.[Executable Schedule List]", "dbo.[Executable Schedule Snapshot]", "dbo.[Executable Schedule Log History]", "DW_SAFEGUARD.dbo.[Dimension Calendar Month]", "[dbo].[Executable Schedule Snapshot]", "[dbo].[Executable List]", "[dbo].[USP_Send_Email_On_Executable_Status]" **
CREATE OR REPLACE PROCEDURE dbo.USP_Prepare_Executable_Snapshot (AGENT_CODE STRING, EXECUTABLE_GROUP STRING DEFAULT NULL, EXECUTABLE_SUB_GROUP STRING DEFAULT NULL, EXECUTABLE_NAME STRING DEFAULT NULL, RUN_MANUALLY BOOLEAN DEFAULT 0)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 2,  "patch": "6.0" }, "attributes": {  "component": "transact",  "convertedOn": "06-27-2025",  "domain": "test" }}'
EXECUTE AS CALLER
AS
$$
	DECLARE
		TODAY_DATE DATE := CURRENT_TIMESTAMP() :: TIMESTAMP;
		EXECUTABLEID INT;
		SCHEDULEID INT;
		EXECUTABLENAME VARCHAR(100);
		FREQUENCY VARCHAR(10);
		PERIOD VARCHAR(20);
		CALENDARTYPE VARCHAR(10);
		MONTHENDDATE DATE;
		QUARTERMONTHNUMBER INT;
		REPORTRUNDATE DATE;
		CANITBERUNONHOLIDAY VARCHAR(1);
		--** SSC-FDM-TS0013 - SNOWFLAKE SCRIPTING CURSOR ROWS ARE NOT MODIFIABLE **
		Enabled_Reports CURSOR
		FOR

			SELECT
				EL."Executable ID",
				ESL."Schedule ID",
				EL."Executable Name"

				,
				ESL.Frequency,
				ESL."Day of Period",
				ESL."Calendar Type"

				,
				m."Calender Last Day of Month",
				m."Calendar Month Number of Quarter"

			    --,CASE WHEN ESL.[Can It Be Run On Holiday]='N' AND ESL.[Day of Period]='3' THEN  DATEADD(DD,1,m.[Report Run Date]) ELSE m.[Report Run Date] END AS [Report Run Date]
				,CASE WHEN ESL."Can It Be Run On Holiday" ='N' AND ESL."Day of Period" ='3'
				AND DATE_PART(dayofweek, m."Report Run Date" :: TIMESTAMP) IN ('Friday','Saturday','Sunday') THEN DATEADD(day, 3, m."Report Run Date")

					  WHEN ESL."Can It Be Run On Holiday" ='N' AND ESL."Day of Period" ='3' THEN DATEADD(day, 1, m."Report Run Date")

					  WHEN ESL."Can It Be Run On Holiday" ='N' AND ESL."Day of Period" ='2'
						AND DATE_PART(dayofweek, m."Report Run Date" :: TIMESTAMP) IN ('Saturday','Sunday') THEN DATEADD(day, 2, m."Report Run Date")

				 ELSE m."Report Run Date"
				END AS "Report Run Date"

				,
				ESL."Can It Be Run On Holiday"

			FROM
				dbo."Executable List" AS EL

			INNER JOIN
					  dbo."Executable Schedule List" AS ESL
					  ON EL."Executable ID" = ESL."Executable ID"

			LEFT JOIN
					  DW_SAFEGUARD.dbo."Dimension Calendar Month" m
					  ON m."Agent Code" = EL."Agent Code"

				AND m."Calendar Type" = ESL."Calendar Type"

				AND CASE WHEN ESL."Can It Be Run On Holiday" ='Y'	THEN m."Calendar Previous Month Code"

							ELSE m."Calendar Current Month Code"
					  END = m."Calendar Month Code" -----------------Added 

			WHERE
				EL."Enabled Flag" =1 AND ESL."Enabled Flag" =1
					AND EL."Agent Code" = :AGENT_CODE

					AND EL."Executable Group" = NVL(:EXECUTABLE_GROUP, EL."Executable Group")
					AND EL."Executable Sub Group" = NVL(:EXECUTABLE_SUB_GROUP, EL."Executable Sub Group")
					AND EL."Executable Name" = NVL(:EXECUTABLE_NAME, EL."Executable Name");
	BEGIN
		 
		CREATE OR REPLACE TEMPORARY TABLE T_TEMP_TABLE (
			"Executable ID" INT,
			Average_Start_Time BIGINT,
			Average_Total_Duration_Sec BIGINT
		);
		CREATE OR REPLACE TEMPORARY TABLE dbo.T_Temp

		(
			"Executable ID" INT,
			"Schedule ID" INT,
			"Executable Name" VARCHAR(100),
			Status VARCHAR(100)

);
		---Check are there any active snapshot entries available or not
		IF (EXISTS ( SELECT
				EL."Executable ID",
				ESL."Schedule ID"

					FROM
				dbo."Executable List" AS EL

					INNER JOIN
					  dbo."Executable Schedule List" AS ESL
					  ON EL."Executable ID" = ESL."Executable ID"

					INNER JOIN
					  dbo."Executable Schedule Snapshot" AS ESS
					  ON ESS."Executable ID" = EL."Executable ID"

									AND (ESS."Schedule ID" = ESL."Schedule ID"
					  OR ESS."Schedule ID" IS NULL)

					WHERE
				EL."Agent Code" = :AGENT_CODE
				AND EL."Executable Group" = NVL(:EXECUTABLE_GROUP, EL."Executable Group")
								AND EL."Executable Sub Group" = NVL(:EXECUTABLE_SUB_GROUP, EL."Executable Sub Group")
								AND EL."Executable Name" = NVL(:EXECUTABLE_NAME, EL."Executable Name")
								AND (ESS.Status ='Inprogress' OR to_timestamp_ntz(ESS."Initiated Time") = :TODAY_DATE)

				  )) THEN
			CALL THROW_UDP(50000, 'There are some active snapshot entries available with status "INPROGRESS", please wait for them to complete', 1);
		END IF;



----Insert previous snapshot data into Log history table 
INSERT INTO dbo."Executable Schedule Log History" ("Executable ID", "Agent Code", "Executable Name", "Schedule ID", "Schedule Description", "Initiated Time", Status, "Start Time", "End Time", Comment)

		SELECT
			EL."Executable ID",
			EL."Agent Code",
			EL."Executable Name"

			,
			ESL."Schedule ID",
			ESL."Day of Period Refers to",
			ESS."Initiated Time",
			ESS.Status

			,
			ESS."Start Time",
			ESS."End Time",
			ESS.Comment

		FROM
			dbo."Executable List" AS EL

			INNER JOIN
				dbo."Executable Schedule List" AS ESL
				ON EL."Executable ID" = ESL."Executable ID"

			INNER JOIN
				dbo."Executable Schedule Snapshot" AS ESS
				ON ESS."Executable ID" = EL."Executable ID"

				AND (ESS."Schedule ID" = ESL."Schedule ID"
				OR ESS."Schedule ID" IS NULL)

WHERE
			EL."Agent Code" = :AGENT_CODE
			AND EL."Executable Group" = NVL(:EXECUTABLE_GROUP, EL."Executable Group")
			AND EL."Executable Sub Group" = NVL(:EXECUTABLE_SUB_GROUP, EL."Executable Sub Group")
			AND EL."Executable Name" = NVL(:EXECUTABLE_NAME, EL."Executable Name");





----Calculate average timings
INSERT INTO :T_TEMP_TABLE ("Executable ID", Average_Start_Time, Average_Total_Duration_Sec)

		SELECT
			EL."Executable ID"

			,
			TRUNC((DATEDIFF(second, TO_TIME('00:00:00'), NVL(EL.Average_Start_Time, TO_TIME(ESS."Start Time"))) *9 + DATEDIFF(second, TO_TIME('00:00:00'), NVL(TO_TIME(ESS."Start Time"), EL.Average_Start_Time))

				 )/10),(NVL(EL.Average_Total_Duration_Sec, DATEDIFF(second, ESS."Start Time", ESS."End Time"))*9 + NVL(DATEDIFF(second, ESS."Start Time", ESS."End Time"), EL.Average_Total_Duration_Sec)

  )/10
FROM
			dbo."Executable List" AS EL

			INNER JOIN
				dbo."Executable Schedule List" AS ESL
				ON EL."Executable ID" = ESL."Executable ID"

			INNER JOIN
				dbo."Executable Schedule Snapshot" AS ESS
				ON ESS."Executable ID" = EL."Executable ID"

				AND ESS."Schedule ID" = ESL."Schedule ID"

		WHERE
			EL."Agent Code" = :AGENT_CODE
			AND EL."Executable Group" = NVL(:EXECUTABLE_GROUP, EL."Executable Group")
			AND EL."Executable Sub Group" = NVL(:EXECUTABLE_SUB_GROUP, EL."Executable Sub Group")
			AND EL."Executable Name" = NVL(:EXECUTABLE_NAME, EL."Executable Name");



--Update back to Executable table
UPDATE EL EL

			SET
				EL.Average_Start_Time = TO_TIME(DATEADD(second, T.Average_Start_Time, TO_TIME('00:00:00'))),
				EL.Average_Total_Duration_Sec = T.Average_Total_Duration_Sec
			FROM
				T_Temp_Table AS T
			WHERE
				EL."Executable ID" = T."Executable ID";
		--select * from [Executable List]



		----Delete records from Snapshot table
		DELETE FROM
			ESS
		WHERE
			EL."Agent Code" = :AGENT_CODE
			AND EL."Executable Group" = NVL(:EXECUTABLE_GROUP, EL."Executable Group")
					AND EL."Executable Sub Group" = NVL(:EXECUTABLE_SUB_GROUP, EL."Executable Sub Group")
					AND EL."Executable Name" = NVL(:EXECUTABLE_NAME, EL."Executable Name");
		 
		 
		OPEN Enabled_Reports;
		FETCH
			Enabled_Reports
		INTO
			:EXECUTABLEID,
			:SCHEDULEID,
			:EXECUTABLENAME,
			:FREQUENCY

			,
			:PERIOD,
			:CALENDARTYPE,
			:MONTHENDDATE,
			:QUARTERMONTHNUMBER

			,
			:REPORTRUNDATE,
			:CANITBERUNONHOLIDAY;
		WHILE (:FETCH_STATUS =0) LOOP
			IF (:FREQUENCY ='Daily') THEN
				BEGIN

					  INSERT INTO T_Temp ("Executable ID", "Schedule ID", "Executable Name", Status)

					  VALUES(:EXECUTABLEID, :SCHEDULEID, :EXECUTABLENAME,'Ready');
				END;
			END IF;
			IF (:FREQUENCY ='Weekly' AND DATE_PART(dayofweek, :TODAY_DATE :: TIMESTAMP) = :PERIOD) THEN
				BEGIN

					  INSERT INTO T_Temp ("Executable ID", "Schedule ID", "Executable Name", Status)

					  VALUES(:EXECUTABLEID, :SCHEDULEID, :EXECUTABLENAME,'Ready');
				END;
			END IF;
			IF (:FREQUENCY ='Monthly'
			AND ((DATEDIFF(day, :MONTHENDDATE, :TODAY_DATE) = :PERIOD
			AND :CANITBERUNONHOLIDAY ='Y')

					OR (:TODAY_DATE = :REPORTRUNDATE
			and :CANITBERUNONHOLIDAY ='N')

				)) THEN
				BEGIN

					  INSERT INTO T_Temp ("Executable ID", "Schedule ID", "Executable Name", Status)

					  VALUES(:EXECUTABLEID, :SCHEDULEID, :EXECUTABLENAME,'Ready');
				END;
			END IF;
			IF (:FREQUENCY ='Quarterly'
			AND (	(DATEDIFF(day, :MONTHENDDATE, :TODAY_DATE) = :PERIOD
			AND :CANITBERUNONHOLIDAY ='Y'
						AND :QUARTERMONTHNUMBER = 3

					)

					OR (:TODAY_DATE = :REPORTRUNDATE
			and :CANITBERUNONHOLIDAY ='N' AND :QUARTERMONTHNUMBER = 1)

				)) THEN
				BEGIN

					  INSERT INTO T_Temp ("Executable ID", "Schedule ID", "Executable Name", Status)

					  VALUES(:EXECUTABLEID, :SCHEDULEID, :EXECUTABLENAME,'Ready');
				END;
			END IF;
			--** SSC-PRF-0003 - FETCH INSIDE A LOOP IS CONSIDERED A COMPLEX PATTERN, THIS COULD DEGRADE SNOWFLAKE PERFORMANCE. **
			FETCH
				Enabled_Reports
			INTO
				:EXECUTABLEID,
				:SCHEDULEID,
				:EXECUTABLENAME,
				:FREQUENCY

				,
				:PERIOD,
				:CALENDARTYPE,
				:MONTHENDDATE,
				:QUARTERMONTHNUMBER

				,
				:REPORTRUNDATE,
				:CANITBERUNONHOLIDAY;
		END LOOP;
		CLOSE Enabled_Reports;
		!!!RESOLVE EWI!!! /*** SSC-EWI-0058 - FUNCTIONALITY FOR 'DEALLOCATE' IS NOT CURRENTLY SUPPORTED BY SNOWFLAKE SCRIPTING ***/!!!
DEALLOCATE Enabled_Reports;
	INSERT INTO dbo."Executable Schedule Snapshot" ("Executable ID", "Schedule ID", "Executable Name", Status)

		SELECT
			"Executable ID",
			MAX("Schedule ID"),
			"Executable Name",
			Status

		FROM
			T_Temp
		GROUP BY
			"Executable ID",
			"Executable Name",
			Status;

		-----Insert Manuall entries if @Run_Manually=1
	INSERT INTO dbo."Executable Schedule Snapshot" ("Executable ID", "Schedule ID", "Executable Name", Status, Comment)

		select
			EL."Executable ID"

			,NULL AS "Schedule ID"

			,
			EL."Executable Name"

			,'Ready','Manually Scheduled'
	FROM
			dbo."Executable List" EL

			LEFT JOIN
				dbo."Executable Schedule Snapshot" AS S
				ON EL."Executable ID" = S."Executable ID"

		WHERE
			S."Executable ID" IS NULL
		AND EL."Agent Code" = :AGENT_CODE

			AND EL."Executable Name" = NVL(:EXECUTABLE_NAME, EL."Executable Name")
		AND EL."Executable Sub Group" = NVL(:EXECUTABLE_SUB_GROUP, EL."Executable Sub Group")
		AND EL."Executable Group" = NVL(:EXECUTABLE_GROUP, EL."Executable Group")
		AND :RUN_MANUALLY =1;
		CALL dbo.USP_Send_Email_On_Executable_Status(:AGENT_CODE, :EXECUTABLE_GROUP, :EXECUTABLE_SUB_GROUP);
		DROP TABLE IF EXISTS T_Temp;
		DROP TABLE T_TEMP_TABLE;
	END;
$$;