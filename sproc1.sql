USE DATABASE DW_DATAACCESS;

--/****** Object:  StoredProcedure [dbo].[SP_FactContract_PART2]    Script Date: 6/27/2025 12:35:26 PM ******/
----** SSC-FDM-TS0027 - SET ANSI_NULLS ON STATEMENT MAY HAVE A DIFFERENT BEHAVIOR IN SNOWFLAKE **
--SET ANSI_NULLS ON

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

SET QUOTED_IDENTIFIER ON;

--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "dw_safeguard.dbo.[Fact Contract]", "[DW_DATAACCESS].ep.[Fact Contract Earned Premium V Partition]", "[DW_DATAACCESS].ep.[Fact Contract Cancel Earned Premium V Partition]", "[DW_SAFEGUARD].dbo.[Fact Contract]", "[DW_SAFEGUARD].dbo.[dimension agent partner]", "[DW_SAFEGUARD].dbo.[Dimension Product]", "[DW_DATAACCESS].dbo.DateParameter", "[DW_SAFEGUARD].dbo.[Dimension Carrier]", "[DW_SAFEGUARD].dbo.[Fact Contract VPartition Claims]" **
CREATE OR REPLACE PROCEDURE dbo.SP_FactContract_PART2 (START_VALUE INT, END_VALUE INT)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 2,  "patch": "6.0" }, "attributes": {  "component": "transact",  "convertedOn": "06-27-2025",  "domain": "test" }}'
EXECUTE AS CALLER
AS
$$
	DECLARE
		GETDATE TIMESTAMP_NTZ(3) := CURRENT_TIMESTAMP() :: TIMESTAMP;
	BEGIN
		!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

		SET NOCOUNT ON;
		 
		CREATE OR REPLACE TEMPORARY TABLE dbo.T_tempcube2 AS
			SELECT
				m."Fact Identifier"

		      ,
				"As of Yesterday Cancel Earned Premium"

		      ,
				"Dec Current Year Cancel Earned Premium"

		      ,
				"Nov Current Year Cancel Earned Premium"

		      ,
				"Oct Current Year Cancel Earned Premium"

		      ,
				"Sep Current Year Cancel Earned Premium"

		      ,
				"Aug Current Year Cancel Earned Premium"

		      ,
				"Jul Current Year Cancel Earned Premium"

		      ,
				"Jun Current Year Cancel Earned Premium"

		      ,
				"May Current Year Cancel Earned Premium"

		      ,
				"Apr Current Year Cancel Earned Premium"

		      ,
				"Mar Current Year Cancel Earned Premium"

		      ,
				"Feb Current Year Cancel Earned Premium"

		      ,
				"Jan Current Year Cancel Earned Premium"

		      ,
				"Year-1 Last Quarter Cancel Earned Premium"

		      ,
				"Year-1 Third Quarter Cancel Earned Premium"

		      ,
				"Year-1 Second Quarter Cancel Earned Premium"

		      ,
				"Year-1 First Quarter Cancel Earned Premium"

		      ,
				"Year-2 Last Quarter Cancel Earned Premium"

		      ,
				"Year-2 Third Quarter Cancel Earned Premium"

		      ,
				"Year-2 Second Quarter Cancel Earned Premium"

		      ,
				"Year-2 First Quarter Cancel Earned Premium"

		      ,
				"As of Yesterday Earned Premium"

		      ,
				"Dec Current Year Earned Premium"

		      ,
				"Nov Current Year Earned Premium"

		      ,
				"Oct Current Year Earned Premium"

		      ,
				"Sep Current Year Earned Premium"

		      ,
				"Aug Current Year Earned Premium"

		      ,
				"Jul Current Year Earned Premium"

		      ,
				"Jun Current Year Earned Premium"

		      ,
				"May Current Year Earned Premium"

		      ,
				"Apr Current Year Earned Premium"

		      ,
				"Mar Current Year Earned Premium"

		      ,
				"Feb Current Year Earned Premium"

		      ,
				"Jan Current Year Earned Premium"

		      ,
				"Year-1 Last Quarter Earned Premium"

		      ,
				"Year-1 Third Quarter Earned Premium"

		      ,
				"Year-1 Second Quarter Earned Premium"

		      ,
				"Year-1 First Quarter Earned Premium"

		      ,
				"Year-2 Last Quarter Earned Premium"

		      ,
				"Year-2 Third Quarter Earned Premium"

		      ,
				"Year-2 Second Quarter Earned Premium"

		      ,
				"Year-2 First Quarter Earned Premium"

		      ,
				"Year-3 Last Quarter Earned Premium"

		      ,
				"Year-4 Last Quarter Earned Premium"

		      ,
				"Year-5 Last Quarter Earned Premium"

		      ,
				"Year-6 Last Quarter Earned Premium"

		      ,
				"Dec Year-1 Cancel Earned Premium"

		      ,
				"Nov Year-1 Cancel Earned Premium"

		      ,
				"Oct Year-1 Cancel Earned Premium"

		      ,
				"Sep Year-1 Cancel Earned Premium"

		      ,
				"Aug Year-1 Cancel Earned Premium"

		      ,
				"Jul Year-1 Cancel Earned Premium"

		      ,
				"Jun Year-1 Cancel Earned Premium"

		      ,
				"May Year-1 Cancel Earned Premium"

		      ,
				"Apr Year-1 Cancel Earned Premium"

		      ,
				"Mar Year-1 Cancel Earned Premium"

		      ,
				"Feb Year-1 Cancel Earned Premium"

		      ,
				"Jan Year-1 Cancel Earned Premium"

		      ,
				"Dec Year-1 Earned Premium"

		      ,
				"Nov Year-1 Earned Premium"

		      ,
				"Oct Year-1 Earned Premium"

		      ,
				"Sep Year-1 Earned Premium"

		      ,
				"Aug Year-1 Earned Premium"

		      ,
				"Jul Year-1 Earned Premium"

		      ,
				"Jun Year-1 Earned Premium"

		      ,
				"May Year-1 Earned Premium"

		      ,
				"Apr Year-1 Earned Premium"

		      ,
				"Mar Year-1 Earned Premium"

		      ,
				"Feb Year-1 Earned Premium"

		      ,
				"Jan Year-1 Earned Premium"

			----Newly added columns on 16 Aug 2016
			,
				"Year-3 Last Quarter Cancel Earned Premium"

			,
				"Year-4 Last Quarter Cancel Earned Premium"

			,
				"Year-5 Last Quarter Cancel Earned Premium"

			,
				"Year-6 Last Quarter Cancel Earned Premium"

		  FROM
				dw_safeguard.dbo."Fact Contract" f

		  INNER JOIN
					DW_DATAACCESS.ep."Fact Contract Earned Premium V Partition" as m
					ON f."Fact Identifier" = m."Fact Identifier"

		  INNER JOIN
					DW_DATAACCESS.ep."Fact Contract Cancel Earned Premium V Partition" as c
					ON m."Fact Identifier" = c."Fact Identifier"

		  WHERE
				f."Primary Sale Flag" = 'Y' AND NVL(f."Delete Flag",'N') <> 'Y'
		and f."Contract Sale Date Identifier" between :START_VALUE AND :END_VALUE;

		SELECT
			f."Fact Identifier"

		      ,
			f.FunctionType

		      ,
			f."Contract Number"

		      ,
			f."Contract Vin"

		      ,
			f."Contract Expiry Date Identifier"

		      ,
			f."Contract Dealer Cost"

		      ,
			f."Contract Dealer Remittance"

		      ,
			f."Contract Agent Cost"

		      ,
			f."Contract Customer Cost"

		      ,
			f."Contract System Generated Cost"

		      ,
			f."Contract Admin Fees"

		      ,
			f."Contract Net Admin Fees"

		      ,
			f."Contract Agent Commission"

		      ,
			f."Contract Net Agent Commission"

		      ,
			f."Secondary Agent Commission"

		      ,
			f."Contract Standard Premium"

		      ,
			f."Contract Written Premium"

		      ,
			f."Contract Net Premium"

		      ,
			f."Contract Cancellation Premium"

		      ,
			f."Contract Earned Premium"

		      ,
			f."Contract Cancel Date Identifier"

		      ,
			f."Contract Paid Cancel Indicator"

		      ,
			f."Contract Dealer Refund Amount"

		      ,
			f."Contract Customer Refund Amount"

		      ,
			f."Contract Form Number"

		      ,
			f."Contract Batch Date Identifier"

		      ,
			f."Source Contract Create Date"

		      ,
			f."Contract Batch Number"

		      ,
			f."Primary Sale Flag"

		      ,
			f."Contract Finance Amount"

		      ,
			f."Contract Stencil"

		      ,
			f."Weighted Contract Count"

		      ,
			f."Contract End VIN"

		      ,
			f."Contract Expiry Miles"

		      ,
			f."Contract InService Date"

		      ,
			f."Contract Lien Entity"

		      ,
			f."Contract Vehicle Cost"

		      ,
			f."Contract Vehicle Mileage"

		      ,
			f."Load Date"

		      ,
			f."Update Date"

		      ,
			f."Load By"

		      ,
			f."Updated By"

		      ,
			f."Vehicle Identifier"

		      ,
			f."Contract Status Identifier"

		      ,
			f."Contract Business Date Identifier"

		      ,
			f."Contract Sale Date Identifier"

		      ,
			f."Dealer Identifier"

		      ,
			f."Contract Channel Identifier"

		      ,
			f."Agent Partner Identifier"

		      ,
			f."Customer Identifier"

		      ,
			f."Contract Term Identifier"

		      ,
			f."Product Identifier"

		      ,
			f."Project Phase Identifier"

		      ,
			f."Contract Risk Type Identifier"

		      ,
			f."Vehicle Type Identifier"

		      ,
			f."Dealer Type Identifier"

		      ,
			f."Coverage Identifier"

		      ,
			f."Carrier Identifier"

		      ,
			f."Delete Flag"

		      ,
			f."Contract Locked Flag"

			  ,
			cancprem."As of Yesterday Earned Premium"

		      --,f.[As of Yesterday Claim Amount]

		     -- ,cancprem.[As of Yesterday Earned Premium] as [Dec Current Year Earned Premium]
			  ,case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -0, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -0, YearEndingDate)
					then cancprem."As of Yesterday Earned Premium"
				else cancprem."Dec Current Year Earned Premium"
		     end "Dec Current Year Earned Premium"

		       --,f.[Dec Current Year Claim Amount]
			  ,case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -1, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -1, YearEndingDate)
					then cancprem."As of Yesterday Earned Premium"
				else cancprem."Nov Current Year Earned Premium"
		     end "Nov Current Year Earned Premium"

		      --,f.[Nov Current Year Claim Amount]
		      ,case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -2, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -2, YearEndingDate)
					then cancprem."As of Yesterday Earned Premium"
				else cancprem."Oct Current Year Earned Premium"
		     end "Oct Current Year Earned Premium"

		      --,f.[Oct Current Year Claim Amount]
		      ,case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -3, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -3, YearEndingDate)
					then cancprem."As of Yesterday Earned Premium"
				else cancprem."Sep Current Year Earned Premium"
		     end "Sep Current Year Earned Premium"

		      ,
		     f."Sep Current Year Claim Amount"

		      ,case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -4, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -4, YearEndingDate)
					then cancprem."As of Yesterday Earned Premium"
				else cancprem."Aug Current Year Earned Premium"
		     end "Aug Current Year Earned Premium"

		      --,f.[Aug Current Year Claim Amount]
		      ,case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -5, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -5, YearEndingDate)
					then cancprem."As of Yesterday Earned Premium"
				else cancprem."Jul Current Year Earned Premium"
		     end "Jul Current Year Earned Premium"

		      --,f.[Jul Current Year Claim Amount]
		      ,case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -6, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -6, YearEndingDate)
					then cancprem."As of Yesterday Earned Premium"
				else cancprem."Jun Current Year Earned Premium"
		     end "Jun Current Year Earned Premium"

		      --,f.[Jun Current Year Claim Amount]
		      ,case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -7, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -7, YearEndingDate)
					then cancprem."As of Yesterday Earned Premium"
				else cancprem."May Current Year Earned Premium"
		     end "May Current Year Earned Premium"

		      --,f.[May Current Year Claim Amount]
		      ,case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -8, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -8, YearEndingDate)
					then cancprem."As of Yesterday Earned Premium"
				else cancprem."Apr Current Year Earned Premium"
		     end "Apr Current Year Earned Premium"

		      --,f.[Apr Current Year Claim Amount]
		      ,case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -9, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -9, YearEndingDate)
					then cancprem."As of Yesterday Earned Premium"
				else cancprem."Mar Current Year Earned Premium"
		     end "Mar Current Year Earned Premium"

		      --,f.[Mar Current Year Claim Amount]
		      ,case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -10, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -10, YearEndingDate)
					then cancprem."As of Yesterday Earned Premium"
				else cancprem."Feb Current Year Earned Premium"
		     end "Feb Current Year Earned Premium"

		      --,f.[Feb Current Year Claim Amount]
		      ,case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -11, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -11, YearEndingDate)
					then cancprem."As of Yesterday Earned Premium"
				else cancprem."Jan Current Year Earned Premium"
		     end "Jan Current Year Earned Premium"

		      --,f.[Jan Current Year Claim Amount]



			 --monthly year-1
		 ,
		     cancprem."Dec Year-1 Earned Premium" as "Dec Year-1 Earned Premium"

		  ,
		     cancprem."Nov Year-1 Earned Premium" as "Nov Year-1 Earned Premium"

		  ,
		     cancprem."Oct Year-1 Earned Premium" as "Oct Year-1 Earned Premium"

		  ,
		     cancprem."Sep Year-1 Earned Premium" as "Sep Year-1 Earned Premium"

		  ,
		     cancprem."Aug Year-1 Earned Premium" as "Aug Year-1 Earned Premium"

		  ,
		     cancprem."Jul Year-1 Earned Premium" as "Jul Year-1 Earned Premium"

		  ,
		     cancprem."Jun Year-1 Earned Premium" as "Jun Year-1 Earned Premium"

		  ,
		     cancprem."May Year-1 Earned Premium" as "May Year-1 Earned Premium"

		  ,
		     cancprem."Apr Year-1 Earned Premium" as "Apr Year-1 Earned Premium"

		  ,
		     cancprem."Mar Year-1 Earned Premium" as "Mar Year-1 Earned Premium"

		  ,
		     cancprem."Feb Year-1 Earned Premium" as "Feb Year-1 Earned Premium"

		  ,
		     cancprem."Jan Year-1 Earned Premium" as "Jan Year-1 Earned Premium"

			  --,cancprem.[As of Yesterday Earned Premium] [Current Year Last Quarter Earned Premium]
			  ,case when month(TodaysDate :: TIMESTAMP) <= month(DATEADD(month, -3, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -0, YearEndingDate)
					then cancprem."As of Yesterday Earned Premium"
				else cancprem."Dec Current Year Earned Premium"
		     end "Current Year Last Quarter Earned Premium"

		      --,f.[Current Year Last Quarter Claim Amount]
		      ,case when month(TodaysDate :: TIMESTAMP) <= month(DATEADD(month, -6, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -3, YearEndingDate)
					then cancprem."As of Yesterday Earned Premium"
				else cancprem."Sep Current Year Earned Premium"
		     end "Current Year Third Quarter Earned Premium"

		      --,f.[Current Year Third Quarter Claim Amount]
		      ,case when month(TodaysDate :: TIMESTAMP) <= month(DATEADD(month, -9, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -6, YearEndingDate)
					then cancprem."As of Yesterday Earned Premium"
				else cancprem."Jun Current Year Earned Premium"
		     end "Current Year Second Quarter Earned Premium"

			  --,f.[Current Year Second Quarter Claim Amount]
		      ,case when month(TodaysDate :: TIMESTAMP) <= month(DATEADD(month, -9, YearEndingDate) :: TIMESTAMP)
					then cancprem."As of Yesterday Earned Premium"
				when TodaysDate < DATEADD(month, -9, YearEndingDate)
					then 0 else cancprem."Mar Current Year Earned Premium"
		     end "Current Year First Quarter Earned Premium"

			  --,f.[Current Year First Quarter Claim Amount]
		      ,
		     cancprem."Year-1 Last Quarter Earned Premium"

		      --,f.[Year-1 Last Quarter Claim Amount]
		      ,
		     cancprem."Year-1 Third Quarter Earned Premium"

		      --,f.[Year-1 Third Quarter Claim Amount]
		      ,
		     cancprem."Year-1 Second Quarter Earned Premium"

		      --,f.[Year-1 Second Quarter Claim Amount]
		      ,
		     cancprem."Year-1 First Quarter Earned Premium"

		      --,f.[Year-1 First Quarter Claim Amount]
		      ,
		     cancprem."Year-2 Last Quarter Earned Premium"

		      --,f.[Year-2 Last Quarter Claim Amount]
		      ,
		     cancprem."Year-2 Third Quarter Earned Premium"

		      --,f.[Year-2 Third Quarter Claim Amount]
		      ,
		     cancprem."Year-2 Second Quarter Earned Premium"

		      --,f.[Year-2 Second Quarter Claim Amount]
		      ,
		     cancprem."Year-2 First Quarter Earned Premium"

		      --,f.[Year-2 First Quarter Claim Amount]
		      ,CASE WHEN to_timestamp_ntz(CAST(f."Contract Business Date Identifier" AS VARCHAR(10))) BETWEEN to_timestamp_ntz(:GETDATE -29) AND :GETDATE
					THEN 1 ELSE 0 END "Last30 Days Contract Count"

		      ,CASE WHEN to_timestamp_ntz(CAST(f."Contract Business Date Identifier" AS VARCHAR(10))) BETWEEN to_timestamp_ntz(:GETDATE -89) AND :GETDATE
					THEN 1 ELSE 0 END "Last90 Days Contract Count"

			  ---new--
			  ,
		     cancprem."Year-3 Last Quarter Earned Premium"

			  ,
		     cancprem."Year-4 Last Quarter Earned Premium"

			  ,
		     cancprem."Year-5 Last Quarter Earned Premium"

			  ,
		     cancprem."Year-6 Last Quarter Earned Premium"

			  -------
		      ,
		     f."Claim Count"

		      ,
		     f."Contract Original Business Date Identifier"

		      ,
		     f."Cancel Paid Date"

		      ,
		     f."Cancellation Fee"

		      ,
		     f."Cancellation Reason"

		      ,
		     f."Cancellation Percentage"

		      ,
		     f."Contract Upsell Amount"

		      ,
		     f."Contract Customer Cancel Date Identifier"

		      --,f.[As Of Yesterday Claim Count]

		      --,f.[Current Year First Quarter Claim Count]

		      --,f.[Current Year Second Quarter Claim Count]

		      --,f.[Current Year Third Quarter Claim Count]

		      --,f.[Current Year Last Quarter Claim Count]

		      --,f.[Year-1 First Quarter Claim Count]

		      --,f.[Year-1 Second Quarter Claim Count]

		      --,f.[Year-1 Third Quarter Claim Count]

		      --,f.[Year-1 Last Quarter Claim Count]

		      --,f.[Year-2 First Quarter Claim Count]

		      --,f.[Year-2 Second Quarter Claim Count]

		      --,f.[Year-2 Third Quarter Claim Count]

		      --,f.[Year-2 Last Quarter Claim Count]

		      --,f.[Jan Current Year Claim Count]

		      --,f.[Feb Current Year Claim Count]

		      --,f.[Mar Current Year Claim Count]

		      --,f.[Apr Current Year Claim Count]

		      --,f.[May Current Year Claim Count]

		      --,f.[Jun Current Year Claim Count]

		      --,f.[Jul Current Year Claim Count]

		      --,f.[Aug Current Year Claim Count]

		      --,f.[Sep Current Year Claim Count]

		      --,f.[Oct Current Year Claim Count]

		      --,f.[Nov Current Year Claim Count]

		      --,f.[Dec Current Year Claim Count]
		      ,
		     f."Future Cancellation Rate"

		      ,
		     f."Retro Identifier",

		  /*

		case when [contract status identifier] = 2 then 0 else  ([Contract Written Premium] - cancprem.[As of Yesterday Earned Premium])* [Future Cancellation Rate] end [As of Yesterday Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0131' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0131' then [Contract Written Premium] else 0 end) - (case when  month(TodaysDate) < month(DATEADD(mm, -11, YearEndingDate)) then 0 when TodaysDate < DATEADD(mm, -11, YearEndingDate) then cancprem.[As of Yesterday Earned Premium] else  cancprem.[Jan Current Year Earned Premium] end))* [Future Cancellation Rate] end [Jan Current Year Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0331' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0331' then [Contract Written Premium] else 0 end) - (case when  month(TodaysDate) < month(DATEADD(mm, -9, YearEndingDate)) then 0 when TodaysDate < DATEADD(mm, -9, YearEndingDate) then cancprem.[As of Yesterday Earned Premium] else  cancprem.[Mar Current Year Earned Premium] end))* [Future Cancellation Rate] end [Mar Current Year Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0430' then 0 else ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0430' then [Contract Written Premium] else 0 end) - (case when  month(TodaysDate) < month(DATEADD(mm, -8, YearEndingDate)) then 0 when TodaysDate < DATEADD(mm, -8, YearEndingDate) then cancprem.[As of Yesterday Earned Premium] else  cancprem.[Apr Current Year Earned Premium] end))* [Future Cancellation Rate] end [Apr Current Year Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0531' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0531' then [Contract Written Premium] else 0 end) - (case when  month(TodaysDate) < month(DATEADD(mm, -7, YearEndingDate)) then 0 when TodaysDate < DATEADD(mm, -7, YearEndingDate) then cancprem.[As of Yesterday Earned Premium] else  cancprem.[May Current Year Earned Premium] end))* [Future Cancellation Rate] end [May Current Year Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0630' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0630' then [Contract Written Premium] else 0 end) - (case when  month(TodaysDate) < month(DATEADD(mm, -6, YearEndingDate)) then 0 when TodaysDate < DATEADD(mm, -6, YearEndingDate) then cancprem.[As of Yesterday Earned Premium] else  cancprem.[Jun Current Year Earned Premium] end))* [Future Cancellation Rate] end [Jun Current Year Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0731' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0731' then [Contract Written Premium] else 0 end) - (case when  month(TodaysDate) < month(DATEADD(mm, -5, YearEndingDate)) then 0 when TodaysDate < DATEADD(mm, -5, YearEndingDate) then cancprem.[As of Yesterday Earned Premium] else  cancprem.[Jul Current Year Earned Premium] end))* [Future Cancellation Rate] end [Jul Current Year Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + case WHEN ISDATE(CAST(year(@getdate) AS char(4)) + '0229') = 1 then '0229' else '0228' end then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + case WHEN ISDATE(CAST(year(@getdate) AS char(4)) + '0229') = 1 then '0229' else '0228' end then [Contract Written Premium] else 0 end) - (case when  month(TodaysDate) < month(DATEADD(mm, -10, YearEndingDate)) then 0 when TodaysDate < DATEADD(mm, -10, YearEndingDate) then cancprem.[As of Yesterday Earned Premium] else  cancprem.[Feb Current Year Earned Premium] end))* [Future Cancellation Rate] end [Feb Current Year Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0831' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0831' then [Contract Written Premium] else 0 end) - (case when  month(TodaysDate) < month(DATEADD(mm, -4, YearEndingDate)) then 0 when TodaysDate < DATEADD(mm, -4, YearEndingDate) then cancprem.[As of Yesterday Earned Premium] else  cancprem.[Aug Current Year Earned Premium] end))* [Future Cancellation Rate] end [Aug Current Year Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0930' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0930' then [Contract Written Premium] else 0 end) - (case when  month(TodaysDate) < month(DATEADD(mm, -3, YearEndingDate)) then 0 when TodaysDate < DATEADD(mm, -3, YearEndingDate) then cancprem.[As of Yesterday Earned Premium] else  cancprem.[Sep Current Year Earned Premium] end))* [Future Cancellation Rate] end [Sep Current Year Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '1031' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '1031' then [Contract Written Premium] else 0 end) - (case when  month(TodaysDate) < month(DATEADD(mm, -2, YearEndingDate)) then 0 when TodaysDate < DATEADD(mm, -2, YearEndingDate) then cancprem.[As of Yesterday Earned Premium] else  cancprem.[Oct Current Year Earned Premium] end))* [Future Cancellation Rate] end [Oct Current Year Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '1130' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '1130' then [Contract Written Premium] else 0 end) - (case when  month(TodaysDate) < month(DATEADD(mm, -1, YearEndingDate)) then 0 when TodaysDate < DATEADD(mm, -1, YearEndingDate) then cancprem.[As of Yesterday Earned Premium] else  cancprem.[Nov Current Year Earned Premium] end))* [Future Cancellation Rate] end [Nov Current Year Net],

		--case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '1231' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '1231' then [Contract Written Premium] else 0 end) - cancprem.[As of Yesterday Earned Premium])* [Future Cancellation Rate] end [Dec Current Year Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '1231' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '1231' then [Contract Written Premium] else 0 end) - (case when  month(TodaysDate) < month(DATEADD(mm, -0, YearEndingDate)) then 0 when TodaysDate < DATEADD(mm, -0, YearEndingDate) then cancprem.[As of Yesterday Earned Premium] else cancprem.[Dec Current Year Earned Premium] end)) * [Future Cancellation Rate] end [Dec Current Year Net],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0331' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0331' then [Contract Written Premium] else 0 end) - (case when  month(TodaysDate) <= month(DATEADD(mm, -9, YearEndingDate)) then cancprem.[As of Yesterday Earned Premium] when TodaysDate < DATEADD(mm, -9, YearEndingDate) then 0 else  cancprem.[Mar Current Year Earned Premium] end))* [Future Cancellation Rate] end [Current Year First Quarter Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0630' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0630' then [Contract Written Premium] else 0 end) - (case when  month(TodaysDate) < month(DATEADD(mm, -9, YearEndingDate)) then 0 when TodaysDate < DATEADD(mm, -6, YearEndingDate) then cancprem.[As of Yesterday Earned Premium] else  cancprem.[Jun Current Year Earned Premium] end))* [Future Cancellation Rate] end [Current Year Second Quarter Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0930' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0930' then [Contract Written Premium] else 0 end) - (case when  month(TodaysDate) <= month(DATEADD(mm, -6, YearEndingDate)) then 0 when TodaysDate < DATEADD(mm, -3, YearEndingDate) then cancprem.[As of Yesterday Earned Premium] else  cancprem.[Sep Current Year Earned Premium] end))* [Future Cancellation Rate] end [Current Year Third Quarter Net],

		--case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '1231' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '1231' then [Contract Written Premium] else 0 end) - f.[As of Yesterday Earned Premium])* [Future Cancellation Rate] end [Current Year Last Quarter Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '1231' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '1231' then [Contract Written Premium] else 0 end) - (case when  month(TodaysDate) <= month(DATEADD(mm, -3, YearEndingDate)) then 0 when TodaysDate < DATEADD(mm, -0, YearEndingDate) then cancprem.[As of Yesterday Earned Premium] else  cancprem.[Dec Current Year Earned Premium] end)) * [Future Cancellation Rate] end [Current Year Last Quarter Net],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast((year(@getdate) -1) as varchar) + '0331' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast((year(@getdate) -1) as varchar) + '0331' then [Contract Written Premium] else 0 end) - cancprem.[Year-1 First Quarter Earned Premium])* [Future Cancellation Rate] end [Year-1 First Quarter Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast((year(@getdate) -1) as varchar) + '0630' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast((year(@getdate) -1) as varchar) + '0630' then [Contract Written Premium] else 0 end) - cancprem.[Year-1 Second Quarter Earned Premium])* [Future Cancellation Rate] end [Year-1 Second Quarter Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast((year(@getdate) -1) as varchar) + '0930' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast((year(@getdate) -1) as varchar) + '0930' then [Contract Written Premium] else 0 end) - cancprem.[Year-1 Third Quarter Earned Premium])* [Future Cancellation Rate] end [Year-1 Third Quarter Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast((year(@getdate) -1) as varchar) + '1231' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast((year(@getdate) -1) as varchar) + '1231' then [Contract Written Premium] else 0 end) - cancprem.[Year-1 Last Quarter Earned Premium])* [Future Cancellation Rate] end [Year-1 Last Quarter Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast((year(@getdate) -2) as varchar) + '0331' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast((year(@getdate) -2) as varchar) + '0331' then [Contract Written Premium] else 0 end) - cancprem.[Year-2 First Quarter Earned Premium])* [Future Cancellation Rate] end [Year-2 First Quarter Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast((year(@getdate) -2) as varchar) + '0630' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast((year(@getdate) -2) as varchar) + '0630' then [Contract Written Premium] else 0 end) - cancprem.[Year-2 Second Quarter Earned Premium])* [Future Cancellation Rate] end [Year-2 Second Quarter Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast((year(@getdate) -2) as varchar) + '0930' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast((year(@getdate) -2) as varchar) + '0930' then [Contract Written Premium] else 0 end) - cancprem.[Year-2 Third Quarter Earned Premium])* [Future Cancellation Rate] end [Year-2 Third Quarter Net],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast((year(@getdate) -2) as varchar) + '1231' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast((year(@getdate) -2) as varchar) + '1231' then [Contract Written Premium] else 0 end) - cancprem.[Year-2 Last Quarter Earned Premium])* [Future Cancellation Rate] end [Year-2 Last Quarter Net],

		*/



		--------------------new logic as per request like [As of Yesterday Cancel Earned Premium] instade of [As of Yesterday Earned Premium]



		("Contract Written Premium" - cancprem."As of Yesterday Cancel Earned Premium")* "Future Cancellation Rate" as "As of Yesterday Net",

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0131' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0131' then "Contract Written Premium"
					else 0 end) - (case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -11, YearEndingDate) :: TIMESTAMP)
						then 0 when TodaysDate < DATEADD(month, -11, YearEndingDate)
						then cancprem."As of Yesterday Cancel Earned Premium"
					else cancprem."Jan Current Year Cancel Earned Premium"
				end))* "Future Cancellation Rate"
		     end "Jan Current Year Net",

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331' then "Contract Written Premium"
					else 0 end) - (case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -9, YearEndingDate) :: TIMESTAMP)
						then 0 when TodaysDate < DATEADD(month, -9, YearEndingDate)
						then cancprem."As of Yesterday Cancel Earned Premium"
					else cancprem."Mar Current Year Cancel Earned Premium"
				end))* "Future Cancellation Rate"
		     end "Mar Current Year Net",

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0430' then 0 else ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0430' then "Contract Written Premium"
					else 0 end) - (case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -8, YearEndingDate) :: TIMESTAMP)
						then 0 when TodaysDate < DATEADD(month, -8, YearEndingDate)
						then cancprem."As of Yesterday Cancel Earned Premium"
					else cancprem."Apr Current Year Cancel Earned Premium"
				end))* "Future Cancellation Rate"
		     end "Apr Current Year Net",

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0531' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0531' then "Contract Written Premium"
					else 0 end) - (case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -7, YearEndingDate) :: TIMESTAMP)
						then 0 when TodaysDate < DATEADD(month, -7, YearEndingDate)
						then cancprem."As of Yesterday Cancel Earned Premium"
					else cancprem."May Current Year Cancel Earned Premium"
				end))* "Future Cancellation Rate"
		     end "May Current Year Net",

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630' then "Contract Written Premium"
					else 0 end) - (case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -6, YearEndingDate) :: TIMESTAMP)
						then 0 when TodaysDate < DATEADD(month, -6, YearEndingDate)
						then cancprem."As of Yesterday Cancel Earned Premium"
					else cancprem."Jun Current Year Cancel Earned Premium"
				end))* "Future Cancellation Rate"
		     end "Jun Current Year Net",

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0731' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0731' then "Contract Written Premium"
					else 0 end) - (case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -5, YearEndingDate) :: TIMESTAMP)
						then 0 when TodaysDate < DATEADD(month, -5, YearEndingDate)
						then cancprem."As of Yesterday Cancel Earned Premium"
					else cancprem."Jul Current Year Cancel Earned Premium"
				end))* "Future Cancellation Rate"
		     end "Jul Current Year Net",

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || case WHEN ISDATE_UDF(CAST(year(:GETDATE :: TIMESTAMP) AS CHAR(4)) || '0229') = 1 then '0229' else '0228' end then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || case WHEN ISDATE_UDF(CAST(year(:GETDATE :: TIMESTAMP) AS CHAR(4)) || '0229') = 1 then '0229' else '0228' end then "Contract Written Premium"
					else 0 end) - (case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -10, YearEndingDate) :: TIMESTAMP)
						then 0 when TodaysDate < DATEADD(month, -10, YearEndingDate)
						then cancprem."As of Yesterday Cancel Earned Premium"
					else cancprem."Feb Current Year Cancel Earned Premium"
				end))* "Future Cancellation Rate"
		     end "Feb Current Year Net",

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0831' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0831' then "Contract Written Premium"
					else 0 end) - (case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -4, YearEndingDate) :: TIMESTAMP)
						then 0 when TodaysDate < DATEADD(month, -4, YearEndingDate)
						then cancprem."As of Yesterday Cancel Earned Premium"
					else cancprem."Aug Current Year Cancel Earned Premium"
				end))* "Future Cancellation Rate"
		     end "Aug Current Year Net",

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930' then "Contract Written Premium"
					else 0 end) - (case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -3, YearEndingDate) :: TIMESTAMP)
						then 0 when TodaysDate < DATEADD(month, -3, YearEndingDate)
						then cancprem."As of Yesterday Cancel Earned Premium"
					else cancprem."Sep Current Year Cancel Earned Premium"
				end))* "Future Cancellation Rate"
		     end "Sep Current Year Net",

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1031' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1031' then "Contract Written Premium"
					else 0 end) - (case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -2, YearEndingDate) :: TIMESTAMP)
						then 0 when TodaysDate < DATEADD(month, -2, YearEndingDate)
						then cancprem."As of Yesterday Cancel Earned Premium"
					else cancprem."Oct Current Year Cancel Earned Premium"
				end))* "Future Cancellation Rate"
		     end "Oct Current Year Net",

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1130' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1130' then "Contract Written Premium"
					else 0 end) - (case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -1, YearEndingDate) :: TIMESTAMP)
						then 0 when TodaysDate < DATEADD(month, -1, YearEndingDate)
						then cancprem."As of Yesterday Cancel Earned Premium"
					else cancprem."Nov Current Year Cancel Earned Premium"
				end))* "Future Cancellation Rate"
		     end "Nov Current Year Net",

		--case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '1231' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '1231' then [Contract Written Premium] else 0 end) - cancprem.[As of Yesterday Earned Premium])* [Future Cancellation Rate] end [Dec Current Year Net],

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1231' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1231' then "Contract Written Premium"
					else 0 end) - (case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -0, YearEndingDate) :: TIMESTAMP)
						then 0 when TodaysDate < DATEADD(month, -0, YearEndingDate)
						then cancprem."As of Yesterday Cancel Earned Premium"
					else cancprem."Dec Current Year Cancel Earned Premium"
				end)) * "Future Cancellation Rate"
		     end "Dec Current Year Net",



		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331' then "Contract Written Premium"
					else 0 end) - (case when month(TodaysDate :: TIMESTAMP) <= month(DATEADD(month, -9, YearEndingDate) :: TIMESTAMP)
						then cancprem."As of Yesterday Cancel Earned Premium"
					when TodaysDate < DATEADD(month, -9, YearEndingDate)
						then 0 else cancprem."Mar Current Year Cancel Earned Premium"
				end))* "Future Cancellation Rate"
		     end "Current Year First Quarter Net",

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630' then "Contract Written Premium"
					else 0 end) - (case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -9, YearEndingDate) :: TIMESTAMP)
						then 0 when TodaysDate < DATEADD(month, -6, YearEndingDate)
						then cancprem."As of Yesterday Cancel Earned Premium"
					else cancprem."Jun Current Year Cancel Earned Premium"
				end))* "Future Cancellation Rate"
		     end "Current Year Second Quarter Net",

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930' then "Contract Written Premium"
					else 0 end) - (case when month(TodaysDate :: TIMESTAMP) <= month(DATEADD(month, -6, YearEndingDate) :: TIMESTAMP)
						then 0 when TodaysDate < DATEADD(month, -3, YearEndingDate)
						then cancprem."As of Yesterday Cancel Earned Premium"
					else cancprem."Sep Current Year Cancel Earned Premium"
				end))* "Future Cancellation Rate"
		     end "Current Year Third Quarter Net",

		--case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '1231' then 0 else  ((case when [Contract Sale Date Identifier] <> 19000101 and cast([Contract Sale Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '1231' then [Contract Written Premium] else 0 end) - f.[As of Yesterday Earned Premium])* [Future Cancellation Rate] end [Current Year Last Quarter Net],

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1231' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1231' then "Contract Written Premium"
					else 0 end) - (case when month(TodaysDate :: TIMESTAMP) <= month(DATEADD(month, -3, YearEndingDate) :: TIMESTAMP)
						then 0 when TodaysDate < DATEADD(month, -0, YearEndingDate)
						then cancprem."As of Yesterday Cancel Earned Premium"
					else cancprem."Dec Current Year Cancel Earned Premium"
				end)) * "Future Cancellation Rate"
		     end "Current Year Last Quarter Net",



		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0331' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0331' then "Contract Written Premium"
					else 0 end) - cancprem."Year-1 First Quarter Cancel Earned Premium")* "Future Cancellation Rate"
		     end "Year-1 First Quarter Net",

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0630' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0630' then "Contract Written Premium"
					else 0 end) - cancprem."Year-1 Second Quarter Cancel Earned Premium")* "Future Cancellation Rate"
		     end "Year-1 Second Quarter Net",

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0930' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0930' then "Contract Written Premium"
					else 0 end) - cancprem."Year-1 Third Quarter Cancel Earned Premium")* "Future Cancellation Rate"
		     end "Year-1 Third Quarter Net",

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1231' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1231' then "Contract Written Premium"
					else 0 end) - cancprem."Year-1 Last Quarter Cancel Earned Premium")* "Future Cancellation Rate"
		     end "Year-1 Last Quarter Net",

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0331' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0331' then "Contract Written Premium"
					else 0 end) - cancprem."Year-2 First Quarter Cancel Earned Premium")* "Future Cancellation Rate"
		     end "Year-2 First Quarter Net",

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0630' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0630' then "Contract Written Premium"
					else 0 end) - cancprem."Year-2 Second Quarter Cancel Earned Premium")* "Future Cancellation Rate"
		     end "Year-2 Second Quarter Net",

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0930' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0930' then "Contract Written Premium"
					else 0 end) - cancprem."Year-2 Third Quarter Cancel Earned Premium")* "Future Cancellation Rate"
		     end "Year-2 Third Quarter Net",

		case when "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '1231' then 0 else  ((case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '1231' then "Contract Written Premium"
					else 0 end) - cancprem."Year-2 Last Quarter Cancel Earned Premium")* "Future Cancellation Rate"
		     end "Year-2 Last Quarter Net",
		     ----------------end
		     ag."Agent Code",

		CASE WHEN b."Carrier Code" IN ('HCI', 'HCIA','CNA') THEN f."Contract Admin Fees"

							  when ag."Agent Code" = '002300' then f."Contract Admin Fees"

							WHEN dp."Source Product Code" ='BLSE' THEN f."Contract Admin Fees"

							  ELSE f."Contract Dealer Cost"
		     END AS "Gross Revenue"

		--VPartition columns

		/*

		,isnull([Jan Current Year Written Premium],0) [Jan Current Year Written Premium]

		      ,isnull([Feb Current Year Written Premium],0) [Feb Current Year Written Premium]

		      ,isnull([Mar Current Year Written Premium],0) [Mar Current Year Written Premium]

		      ,isnull([Apr Current Year Written Premium],0) [Apr Current Year Written Premium]

		      ,isnull([May Current Year Written Premium],0) [May Current Year Written Premium]

		      ,isnull([Jun Current Year Written Premium],0) [Jun Current Year Written Premium]

		      ,isnull([Jul Current Year Written Premium],0) [Jul Current Year Written Premium]

		      ,isnull([Aug Current Year Written Premium],0) [Aug Current Year Written Premium]

		      ,isnull([Sep Current Year Written Premium],0) [Sep Current Year Written Premium]

		      ,isnull([Oct Current Year Written Premium],0) [Oct Current Year Written Premium]

		      ,isnull([Nov Current Year Written Premium],0) [Nov Current Year Written Premium]

		      ,isnull([Dec Current Year Written Premium],0) [Dec Current Year Written Premium]

			  ,isnull([Mar Current Year Written Premium],0) as [Current Year First Quarter Written Premium]

			  ,isnull([Jun Current Year Written Premium],0) as [Current Year Second Quarter Written Premium]

			  ,isnull([Sep Current Year Written Premium],0) as [Current Year Third Quarter Written Premium]

			  ,isnull([Dec Current Year Written Premium],0) as [Current Year Last Quarter Written Premium]

		      ,isnull([Year-1 First Quarter Written Premium],0) [Year-1 First Quarter Written Premium]

		      ,isnull([Year-1 Second Quarter Written Premium],0) [Year-1 Second Quarter Written Premium]

		      ,isnull([Year-1 Third Quarter Written Premium],0) [Year-1 Third Quarter Written Premium]

		      ,isnull([Year-1 Last Quarter Written Premium],0) [Year-1 Last Quarter Written Premium]

		      ,isnull([Year-2 First Quarter Written Premium],0) [Year-2 First Quarter Written Premium]

		      ,isnull([Year-2 Second Quarter Written Premium],0) [Year-2 Second Quarter Written Premium]

		      ,isnull([Year-2 Third Quarter Written Premium],0) [Year-2 Third Quarter Written Premium]

		      ,isnull([Year-2 Last Quarter Written Premium],0) [Year-2 Last Quarter Written Premium]

		*/

			  --new values
		,case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0131' then "Contract Written Premium"
				else 0 end "Jan Current Year Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331' then "Contract Written Premium"
				else 0 end "Mar Current Year Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0430' then "Contract Written Premium"
				else 0 end "Apr Current Year Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0531' then "Contract Written Premium"
				else 0 end "May Current Year Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630' then "Contract Written Premium"
				else 0 end "Jun Current Year Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0731' then "Contract Written Premium"
				else 0 end "Jul Current Year Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || case WHEN ISDATE_UDF(CAST(year(:GETDATE :: TIMESTAMP) AS CHAR(4)) || '0229') = 1 then '0229' else '0228' end then "Contract Written Premium"
				else 0 end "Feb Current Year Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0831' then "Contract Written Premium"
				else 0 end "Aug Current Year Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930' then "Contract Written Premium"
				else 0 end "Sep Current Year Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1031' then "Contract Written Premium"
				else 0 end "Oct Current Year Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1130' then "Contract Written Premium"
				else 0 end "Nov Current Year Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1231' then "Contract Written Premium"
				else 0 end "Dec Current Year Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331' then "Contract Written Premium"
				else 0 end "Current Year First Quarter Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630' then "Contract Written Premium"
				else 0 end "Current Year Second Quarter Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930' then "Contract Written Premium"
				else 0 end "Current Year Third Quarter Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1231' then "Contract Written Premium"
				else 0 end "Current Year Last Quarter Written Premium",



		--monthly year-1

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0131' then "Contract Written Premium"
				else 0 end "Jan Year-1 Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0331' then "Contract Written Premium"
				else 0 end "Mar Year-1 Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0430' then "Contract Written Premium"
				else 0 end "Apr Year-1 Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0531' then "Contract Written Premium"
				else 0 end "May Year-1 Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0630' then "Contract Written Premium"
				else 0 end "Jun Year-1 Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0731' then "Contract Written Premium"
				else 0 end "Jul Year-1 Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || case WHEN ISDATE_UDF(CAST((year(:GETDATE :: TIMESTAMP) -1) AS CHAR(4)) || '0229') = 1 then '0229' else '0228' end then "Contract Written Premium"
				else 0 end "Feb Year-1 Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0831' then "Contract Written Premium"
				else 0 end "Aug Year-1 Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0930' then "Contract Written Premium"
				else 0 end "Sep Year-1 Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1031' then "Contract Written Premium"
				else 0 end "Oct Year-1 Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1130' then "Contract Written Premium"
				else 0 end "Nov Year-1 Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1231' then "Contract Written Premium"
				else 0 end "Dec Year-1 Written Premium",







		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0331' then "Contract Written Premium"
				else 0 end "Year-1 First Quarter Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0630' then "Contract Written Premium"
				else 0 end "Year-1 Second Quarter Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0930' then "Contract Written Premium"
				else 0 end "Year-1 Third Quarter Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1231' then "Contract Written Premium"
				else 0 end "Year-1 Last Quarter Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0331' then "Contract Written Premium"
				else 0 end "Year-2 First Quarter Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0630' then "Contract Written Premium"
				else 0 end "Year-2 Second Quarter Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0930' then "Contract Written Premium"
				else 0 end "Year-2 Third Quarter Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '1231' then "Contract Written Premium"
				else 0 end "Year-2 Last Quarter Written Premium",

		----------new

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -3) as VARCHAR) || '1231' then "Contract Written Premium"
				else 0 end "Year-3 Last Quarter Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -4) as VARCHAR) || '1231' then "Contract Written Premium"
				else 0 end "Year-4 Last Quarter Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -5) as VARCHAR) || '1231' then "Contract Written Premium"
				else 0 end "Year-5 Last Quarter Written Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -6) as VARCHAR) || '1231' then "Contract Written Premium"
				else 0 end "Year-6 Last Quarter Written Premium",





			 --old

			 /*

		      ,isnull([Jan Current Year Net Premium],0) [Jan Current Year Net Premium]

		      ,isnull([Feb Current Year Net Premium],0) [Feb Current Year Net Premium]

		      ,isnull([Mar Current Year Net Premium],0) [Mar Current Year Net Premium]

		      ,isnull([Apr Current Year Net Premium],0) [Apr Current Year Net Premium]

		      ,isnull([May Current Year Net Premium],0) [May Current Year Net Premium]

		      ,isnull([Jun Current Year Net Premium],0) [Jun Current Year Net Premium]

		      ,isnull([Jul Current Year Net Premium],0) [Jul Current Year Net Premium]

		      ,isnull([Aug Current Year Net Premium],0) [Aug Current Year Net Premium]

		      ,isnull([Sep Current Year Net Premium],0) [Sep Current Year Net Premium]

		      ,isnull([Oct Current Year Net Premium],0) [Oct Current Year Net Premium]

		      ,isnull([Nov Current Year Net Premium],0) [Nov Current Year Net Premium]

		      ,isnull([Dec Current Year Net Premium],0) [Dec Current Year Net Premium]

			  ,isnull([Mar Current Year Net Premium],0) as [Current Year First Quarter Net Premium]

			  ,isnull([Jun Current Year Net Premium],0) as [Current Year Second Quarter Net Premium]

			  ,isnull([Sep Current Year Net Premium],0) as [Current Year Third Quarter Net Premium]

			  ,isnull([Dec Current Year Net Premium],0) as [Current Year Last Quarter Net Premium]

		      ,isnull([Year-1 First Quarter Net Premium],0) [Year-1 First Quarter Net Premium]

		      ,isnull([Year-1 Second Quarter Net Premium],0) [Year-1 Second Quarter Net Premium]

		      ,isnull([Year-1 Third Quarter Net Premium],0) [Year-1 Third Quarter Net Premium]

		      ,isnull([Year-1 Last Quarter Net Premium],0) [Year-1 Last Quarter Net Premium]

		      ,isnull([Year-2 First Quarter Net Premium],0) [Year-2 First Quarter Net Premium]

		      ,isnull([Year-2 Second Quarter Net Premium],0) [Year-2 Second Quarter Net Premium]

		      ,isnull([Year-2 Third Quarter Net Premium],0) [Year-2 Third Quarter Net Premium]

		      ,isnull([Year-2 Last Quarter Net Premium],0) [Year-2 Last Quarter Net Premium]

			  */

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0131' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0131' then "Contract Cancellation Premium"
				else 0 end "Jan Current Year Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331' then "Contract Cancellation Premium"
				else 0 end "Mar Current Year Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0430' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0430' then "Contract Cancellation Premium"
				else 0 end "Apr Current Year Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0531' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0531' then "Contract Cancellation Premium"
				else 0 end "May Current Year Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630' then "Contract Cancellation Premium"
				else 0 end "Jun Current Year Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0731' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0731' then "Contract Cancellation Premium"
				else 0 end "Jul Current Year Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || case WHEN ISDATE_UDF(CAST(year(:GETDATE :: TIMESTAMP) AS CHAR(4)) || '0229') = 1 then '0229' else '0228' end then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || case WHEN ISDATE_UDF(CAST(year(:GETDATE :: TIMESTAMP) AS CHAR(4)) || '0229') = 1 then '0229' else '0228' end then "Contract Cancellation Premium"
				else 0 end "Feb Current Year Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0831' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0831' then "Contract Cancellation Premium"
				else 0 end "Aug Current Year Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930' then "Contract Cancellation Premium"
				else 0 end "Sep Current Year Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1031' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1031' then "Contract Cancellation Premium"
				else 0 end "Oct Current Year Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1130' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1130' then "Contract Cancellation Premium"
				else 0 end "Nov Current Year Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1231' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1231' then "Contract Cancellation Premium"
				else 0 end "Dec Current Year Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331' then "Contract Cancellation Premium"
				else 0 end "Current Year First Quarter Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630' then "Contract Cancellation Premium"
				else 0 end "Current Year Second Quarter Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930' then "Contract Cancellation Premium"
				else 0 end "Current Year Third Quarter Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1231' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1231' then "Contract Cancellation Premium"
				else 0 end "Current Year Last Quarter Net Premium",



		---monthly year-1

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0131' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0131' then "Contract Cancellation Premium"
				else 0 end "Jan Year-1 Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0331' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <=cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0331' then "Contract Cancellation Premium"
				else 0 end "Mar Year-1 Net Premium",



		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0430' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0430' then "Contract Cancellation Premium"
				else 0 end "Apr Year-1 Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0531' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0531' then "Contract Cancellation Premium"
				else 0 end "May Year-1 Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0630' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0630' then "Contract Cancellation Premium"
				else 0 end "Jun Year-1 Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0731' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0731' then "Contract Cancellation Premium"
				else 0 end "Jul Year-1 Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || case WHEN ISDATE_UDF(cast((year(:GETDATE :: TIMESTAMP) -1) AS CHAR(4)) || '0229') = 1 then '0229' else '0228' end then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <=cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || case WHEN ISDATE_UDF(cast((year(:GETDATE :: TIMESTAMP) -1) AS CHAR(4)) || '0229') = 1 then '0229' else '0228' end then "Contract Cancellation Premium"
				else 0 end "Feb Year-1 Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0831' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0831' then "Contract Cancellation Premium"
				else 0 end "Aug Year-1 Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0930' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0930' then "Contract Cancellation Premium"
				else 0 end "Sep Year-1 Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1031' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1031' then "Contract Cancellation Premium"
				else 0 end "Oct Year-1 Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1130' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1130' then "Contract Cancellation Premium"
				else 0 end "Nov Year-1 Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1231' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1231' then "Contract Cancellation Premium"
				else 0 end "Dec Year-1 Net Premium",





		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0331' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0331' then "Contract Cancellation Premium"
				else 0 end "Year-1 First Quarter Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0630' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0630' then "Contract Cancellation Premium"
				else 0 end "Year-1 Second Quarter Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0930' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0930' then "Contract Cancellation Premium"
				else 0 end "Year-1 Third Quarter Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1231' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1231' then "Contract Cancellation Premium"
				else 0 end "Year-1 Last Quarter Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0331' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0331' then "Contract Cancellation Premium"
				else 0 end "Year-2 First Quarter Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0630' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0630' then "Contract Cancellation Premium"
				else 0 end "Year-2 Second Quarter Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0930' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0930' then "Contract Cancellation Premium"
				else 0 end "Year-2 Third Quarter Net Premium",

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '1231' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '1231' then "Contract Cancellation Premium"
				else 0 end "Year-2 Last Quarter Net Premium",

		----------new

		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -3) as VARCHAR) || '1231' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -3) as VARCHAR) || '1231' then "Contract Cancellation Premium"
				else 0 end "Year-3 Last Quarter Net Premium",



		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -4) as VARCHAR) || '1231' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -4) as VARCHAR) || '1231' then "Contract Cancellation Premium"
				else 0 end "Year-4 Last Quarter Net Premium",



		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -5) as VARCHAR) || '1231' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -5) as VARCHAR) || '1231' then "Contract Cancellation Premium"
				else 0 end "Year-5 Last Quarter Net Premium",



		case when "Contract Sale Date Identifier" <> 19000101 and cast("Contract Sale Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -6) as VARCHAR) || '1231' then "Contract Written Premium"
				else 0 end +

		case when "contract status identifier" = 2 and "Contract Cancel Date Identifier" <> 19000101 and cast("Contract Cancel Date Identifier" as VARCHAR)  <= cast((year(:GETDATE :: TIMESTAMP) -6) as VARCHAR) || '1231' then "Contract Cancellation Premium"
				else 0 end "Year-6 Last Quarter Net Premium",
		     --new canc premium
		     cancprem."As of Yesterday Cancel Earned Premium",

		--cancprem.[As of Yesterday Cancel Earned Premium] as [Dec Current Year Cancel Earned Premium],

		case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -0, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -0, YearEndingDate)
					then cancprem."As of Yesterday Cancel Earned Premium"
				else cancprem."Dec Current Year Cancel Earned Premium"
		     end "Dec Current Year Cancel Earned Premium",



		 case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -11, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -11, YearEndingDate)
					then cancprem."As of Yesterday Cancel Earned Premium"
				else cancprem."Jan Current Year Cancel Earned Premium"
		     end "Jan Current Year Cancel Earned Premium",

		 case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -10, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -10, YearEndingDate)
					then cancprem."As of Yesterday Cancel Earned Premium"
				else cancprem."Feb Current Year Cancel Earned Premium"
		     end "Feb Current Year Cancel Earned Premium",



		 case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -9, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -9, YearEndingDate)
					then cancprem."As of Yesterday Cancel Earned Premium"
				else cancprem."Mar Current Year Cancel Earned Premium"
		     end "Mar Current Year Cancel Earned Premium",

		 case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -8, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -8, YearEndingDate)
					then cancprem."As of Yesterday Cancel Earned Premium"
				else cancprem."Apr Current Year Cancel Earned Premium"
		     end "Apr Current Year Cancel Earned Premium",



		 case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -7, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -7, YearEndingDate)
					then cancprem."As of Yesterday Cancel Earned Premium"
				else cancprem."May Current Year Cancel Earned Premium"
		     end "May Current Year Cancel Earned Premium",

		 case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -6, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -6, YearEndingDate)
					then cancprem."As of Yesterday Cancel Earned Premium"
				else cancprem."Jun Current Year Cancel Earned Premium"
		     end "Jun Current Year Cancel Earned Premium",



		case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -5, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -5, YearEndingDate)
					then cancprem."As of Yesterday Cancel Earned Premium"
				else cancprem."Jul Current Year Cancel Earned Premium"
		     end "Jul Current Year Cancel Earned Premium",

		 case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -4, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -4, YearEndingDate)
					then cancprem."As of Yesterday Cancel Earned Premium"
				else cancprem."Aug Current Year Cancel Earned Premium"
		     end "Aug Current Year Cancel Earned Premium",



		 case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -3, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -3, YearEndingDate)
					then cancprem."As of Yesterday Cancel Earned Premium"
				else cancprem."Sep Current Year Cancel Earned Premium"
		     end "Sep Current Year Cancel Earned Premium",

		 case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -2, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -2, YearEndingDate)
					then cancprem."As of Yesterday Cancel Earned Premium"
				else cancprem."Oct Current Year Cancel Earned Premium"
		     end "Oct Current Year Cancel Earned Premium",

		 case when month(TodaysDate :: TIMESTAMP) < month(DATEADD(month, -1, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -1, YearEndingDate)
					then cancprem."As of Yesterday Cancel Earned Premium"
				else cancprem."Nov Current Year Cancel Earned Premium"
		     end "Nov Current Year Cancel Earned Premium",
		     --monthly year-1
		     cancprem."Jan Year-1 Cancel Earned Premium" as "Jan Year-1 Cancel Earned Premium",
		     cancprem."Feb Year-1 Cancel Earned Premium" as "Feb Year-1 Cancel Earned Premium",
		     cancprem."Mar Year-1 Cancel Earned Premium" as "Mar Year-1 Cancel Earned Premium",
		     cancprem."Apr Year-1 Cancel Earned Premium" as "Apr Year-1 Cancel Earned Premium",
		     cancprem."May Year-1 Cancel Earned Premium" as "May Year-1 Cancel Earned Premium",
		     cancprem."Jun Year-1 Cancel Earned Premium" as "Jun Year-1 Cancel Earned Premium",
		     cancprem."Jul Year-1 Cancel Earned Premium" as "Jul Year-1 Cancel Earned Premium",
		     cancprem."Aug Year-1 Cancel Earned Premium" as "Aug Year-1 Cancel Earned Premium",
		     cancprem."Sep Year-1 Cancel Earned Premium" as "Sep Year-1 Cancel Earned Premium",
		     cancprem."Oct Year-1 Cancel Earned Premium" as "Oct Year-1 Cancel Earned Premium",
		     cancprem."Nov Year-1 Cancel Earned Premium" as "Nov Year-1 Cancel Earned Premium",
		     cancprem."Dec Year-1 Cancel Earned Premium" as "Dec Year-1 Cancel Earned Premium",



		-- cancprem.[As of Yesterday Cancel Earned Premium] as [Current Year last Quarter Cancel Earned Premium],

		  case when month(TodaysDate :: TIMESTAMP) <= month(DATEADD(month, -3, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -0, YearEndingDate)
					then cancprem."As of Yesterday Cancel Earned Premium"
				else cancprem."Dec Current Year Cancel Earned Premium"
		     end "Current Year last Quarter Cancel Earned Premium",

		 case when month(TodaysDate :: TIMESTAMP) <= month(DATEADD(month, -6, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -3, YearEndingDate)
					then cancprem."As of Yesterday Cancel Earned Premium"
				else cancprem."Sep Current Year Cancel Earned Premium"
		     end "Current Year Third Quarter Cancel Earned Premium",

		 case when month(TodaysDate :: TIMESTAMP) <= month(DATEADD(month, -9, YearEndingDate) :: TIMESTAMP)
					then 0 when TodaysDate < DATEADD(month, -6, YearEndingDate)
					then cancprem."As of Yesterday Cancel Earned Premium"
				else cancprem."Jun Current Year Cancel Earned Premium"
		     end "Current Year Second Quarter Cancel Earned Premium",

		 case when month(TodaysDate :: TIMESTAMP) <= month(DATEADD(month, -9, YearEndingDate) :: TIMESTAMP)
					then cancprem."As of Yesterday Cancel Earned Premium"
				when TodaysDate < DATEADD(month, -9, YearEndingDate)
					then 0 else cancprem."Mar Current Year Cancel Earned Premium"
		     end "Current Year First Quarter Cancel Earned Premium",
		     cancprem."Year-1 Last Quarter Cancel Earned Premium",
		     cancprem."Year-1 Third Quarter Cancel Earned Premium",
		     cancprem."Year-1 Second Quarter Cancel Earned Premium",
		     cancprem."Year-1 First Quarter Cancel Earned Premium",
		     cancprem."Year-2 Last Quarter Cancel Earned Premium",
		     cancprem."Year-2 Third Quarter Cancel Earned Premium",
		     cancprem."Year-2 Second Quarter Cancel Earned Premium",
		     cancprem."Year-2 First Quarter Cancel Earned Premium",



		  -----------------------In-force Premium and contract start------------------

		  case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0131' and  cast("Contract Expiry Date Identifier" as VARCHAR) >= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0101' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0101') and ("Contract Cancel Date Identifier" <>'19000101')  then 0 else 1 end else 0 end as "Current Year M01 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0131' and  cast("Contract Expiry Date Identifier" as VARCHAR) >= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0101' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0101')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Current Year M01 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || case WHEN ISDATE_UDF(CAST(year(:GETDATE :: TIMESTAMP) AS CHAR(4)) || '0229') = 1 then '0229' else '0228' end  and  cast("Contract Expiry Date Identifier" as VARCHAR) >= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0131' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0131')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Current Year M02 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || case WHEN ISDATE_UDF(CAST(year(:GETDATE :: TIMESTAMP) AS CHAR(4)) || '0229') = 1 then '0229' else '0228' end  and  cast("Contract Expiry Date Identifier" as VARCHAR) >= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0131' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0131')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Current Year M02 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331'  and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || case WHEN ISDATE_UDF(CAST(year(:GETDATE :: TIMESTAMP) AS CHAR(4)) || '0229') = 1 then '0229' else '0228' end then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || case WHEN ISDATE_UDF(CAST(year(:GETDATE :: TIMESTAMP) AS CHAR(4)) || '0229') = 1 then '0229' else '0228' end)  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Current Year M03 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331' and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || case WHEN ISDATE_UDF(CAST(year(:GETDATE :: TIMESTAMP) AS CHAR(4)) || '0229') = 1 then '0229' else '0228' end  then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || case WHEN ISDATE_UDF(CAST(year(:GETDATE :: TIMESTAMP) AS CHAR(4)) || '0229') = 1 then '0229' else '0228' end )  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Current Year M03 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0430'  and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Current Year M04 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0430' and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Current Year M04 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0531'  and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0430' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0430')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Current Year M05 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0531' and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0430' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0430')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Current Year M05 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630'  and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0531' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0531')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Current Year M06 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630' and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0531' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0531')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Current Year M06 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0731'  and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Current Year M07 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0731' and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Current Year M07 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0831'  and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0731' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0731')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Current Year M08 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0831' and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0731' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0731')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Current Year M08 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930'  and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0831' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0831')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Current Year M09 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930' and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0831' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0831')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Current Year M09 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1031'  and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Current Year M10 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1031' and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Current Year M10 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1130'  and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1031' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1031')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Current Year M11 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1130' and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1031' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1031')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Current Year M11 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1231'  and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1130' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1130')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Current Year M12 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1231' and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1130' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1130')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Current Year M12 In-force Premium",





		--------------------new added in monthly year-1 in force

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0131' and  cast("Contract Expiry Date Identifier" as VARCHAR) >= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0101' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0101') and ("Contract Cancel Date Identifier" <>'19000101')  then 0 else 1 end else 0 end as "Year-1 M01 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0131' and  cast("Contract Expiry Date Identifier" as VARCHAR) >= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0101' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0101')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Year-1 M01 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || case WHEN ISDATE_UDF(cast((year(:GETDATE :: TIMESTAMP) -1) AS CHAR(4)) || '0229') = 1 then '0229' else '0228' end  and  cast("Contract Expiry Date Identifier" as VARCHAR) >= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0131' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0131')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Year-1 M02 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || case WHEN ISDATE_UDF(cast((year(:GETDATE :: TIMESTAMP) -1) AS CHAR(4)) || '0229') = 1 then '0229' else '0228' end  and  cast("Contract Expiry Date Identifier" as VARCHAR) >= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0131' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0131')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Year-1 M02 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0331'  and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || case WHEN ISDATE_UDF(cast((year(:GETDATE :: TIMESTAMP) -1) AS CHAR(4)) || '0229') = 1 then '0229' else '0228' end then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || case WHEN ISDATE_UDF(cast((year(:GETDATE :: TIMESTAMP) -1) AS CHAR(4)) || '0229') = 1 then '0229' else '0228' end)  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Year-1 M03 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0331' and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || case WHEN ISDATE_UDF(cast((year(:GETDATE :: TIMESTAMP) -1) AS CHAR(4)) || '0229') = 1 then '0229' else '0228' end  then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || case WHEN ISDATE_UDF(cast((year(:GETDATE :: TIMESTAMP) -1) AS CHAR(4)) || '0229') = 1 then '0229' else '0228' end )  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Year-1 M03 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0430'  and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0331' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0331')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Year-1 M04 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0430' and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0331' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0331')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Year-1 M04 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0531'  and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0430' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0430')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Year-1 M05 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0531' and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0430' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0430')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Year-1 M05 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0630'  and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0531' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0531')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Year-1 M06 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0630' and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0531' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0531')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Year-1 M06 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0731'  and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0630' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0630')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Year-1 M07 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0731' and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0630' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0630')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Year-1 M07 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0831'  and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0731' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0731')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Year-1 M08 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0831' and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0731' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0731')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Year-1 M08 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0930'  and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0831' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0831')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Year-1 M09 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0930' and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0831' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0831')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Year-1 M09 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1031'  and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0930' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0930')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Year-1 M10 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1031' and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0930' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0930')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Year-1 M10 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1130'  and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1031' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1031')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Year-1 M11 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1130' and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1031' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1031')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Year-1 M11 In-force Premium",



		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1231'  and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1130' then case when  (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1130')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Year-1 M12 In-force Contract Count",

		case when  cast("Contract Sale Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1231' and  cast("Contract Expiry Date Identifier" as VARCHAR) > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1130' then case when (cast("Contract Cancel Date Identifier" as VARCHAR) <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1130')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Year-1 M12 In-force Premium",







		---------Current Year Q



		--select cast(year(@getdate) as varchar) + '0630'

		case when "Contract Sale Date Identifier" <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331'  and "Contract Expiry Date Identifier" >= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0101' then case when ("Contract Cancel Date Identifier" <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0101')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Current Year Q1 In-force Contract Count",

		case when "Contract Sale Date Identifier" <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331' and "Contract Expiry Date Identifier" >= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0101' then case when ("Contract Cancel Date Identifier" <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0101')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Current Year Q1 In-force Premium",



		case when "Contract Sale Date Identifier" <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630'  and "Contract Expiry Date Identifier" > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331' then case when ("Contract Cancel Date Identifier" <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Current Year Q2 In-force Contract Count",

		case when "Contract Sale Date Identifier" <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630' and "Contract Expiry Date Identifier" > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331' then case when ("Contract Cancel Date Identifier" <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0331')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Current Year Q2 In-force Premium",



		case when "Contract Sale Date Identifier" <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930'  and "Contract Expiry Date Identifier" > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630' then case when ("Contract Cancel Date Identifier" <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Current Year Q3 In-force Contract Count",

		case when "Contract Sale Date Identifier" <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930' and "Contract Expiry Date Identifier" > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630' then case when ("Contract Cancel Date Identifier" <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0630')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Current Year Q3 In-force Premium",



		case when "Contract Sale Date Identifier" <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1231'  and "Contract Expiry Date Identifier" > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930' then case when ("Contract Cancel Date Identifier" <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Current Year Q4 In-force Contract Count",

		case when "Contract Sale Date Identifier" <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '1231' and "Contract Expiry Date Identifier" > cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930' then case when ("Contract Cancel Date Identifier" <= cast(year(:GETDATE :: TIMESTAMP) as VARCHAR) || '0930')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Current Year Q4 In-force Premium",





		---------------year 1



		case when "Contract Sale Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0331'  and "Contract Expiry Date Identifier" >= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0101' then case when ("Contract Cancel Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0101')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Year-1 Q1 In-force Contract Count",

		case when "Contract Sale Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0331' and "Contract Expiry Date Identifier" >= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0101' then case when ("Contract Cancel Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0101')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Year-1 Q1 In-force Premium",



		case when "Contract Sale Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0630'  and "Contract Expiry Date Identifier" > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0331' then case when ("Contract Cancel Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0331')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Year-1 Q2 In-force Contract Count",

		case when "Contract Sale Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0630' and "Contract Expiry Date Identifier" > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0331' then case when ("Contract Cancel Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0331')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Year-1 Q2 In-force Premium",



		case when "Contract Sale Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0930'  and "Contract Expiry Date Identifier" > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0630' then case when ("Contract Cancel Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0630')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Year-1 Q3 In-force Contract Count",

		case when "Contract Sale Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0930' and "Contract Expiry Date Identifier" > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0630' then case when ("Contract Cancel Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0630')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Year-1 Q3 In-force Premium",



		case when "Contract Sale Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1231'  and "Contract Expiry Date Identifier" > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0930' then case when ("Contract Cancel Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0930')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Year-1 Q4 In-force Contract Count",

		case when "Contract Sale Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '1231' and "Contract Expiry Date Identifier" > cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0930' then case when ("Contract Cancel Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -1) as VARCHAR) || '0930')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Year-1 Q4 In-force Premium",



		---------------year 2



		case when "Contract Sale Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0331'  and "Contract Expiry Date Identifier" >= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0101' then case when ("Contract Cancel Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0101')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Year-2 Q1 In-force Contract Count",

		case when "Contract Sale Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0331' and "Contract Expiry Date Identifier" >= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0101' then case when ("Contract Cancel Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0101')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Year-2 Q1 In-force Premium",



		case when "Contract Sale Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0630'  and "Contract Expiry Date Identifier" > cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0331' then case when ("Contract Cancel Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0331')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Year-2 Q2 In-force Contract Count",

		case when "Contract Sale Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0630' and "Contract Expiry Date Identifier" > cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0331' then case when ("Contract Cancel Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0331')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Year-2 Q2 In-force Premium",



		case when "Contract Sale Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0930'  and "Contract Expiry Date Identifier" > cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0630' then case when ("Contract Cancel Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0630')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Year-2 Q3 In-force Contract Count",

		case when "Contract Sale Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0930' and "Contract Expiry Date Identifier" > cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0630' then case when ("Contract Cancel Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0630')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Year-2 Q3 In-force Premium",



		case when "Contract Sale Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '1231'  and "Contract Expiry Date Identifier" > cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0930' then case when ("Contract Cancel Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0930')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else 1 end else 0 end as "Year-2 Q4 In-force Contract Count",

		case when "Contract Sale Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '1231' and "Contract Expiry Date Identifier" > cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0930' then case when ("Contract Cancel Date Identifier" <= cast((year(:GETDATE :: TIMESTAMP) -2) as VARCHAR) || '0930')  and ("Contract Cancel Date Identifier" <>'19000101') then 0 else "Contract Written Premium"
					end else 0 end as "Year-2 Q4 In-force Premium",



		  ------------------------   In-force Premium and contract end ------- 



		/*



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0131' then [Jan Current Year Cancel Earned Premium] else 0 end [Jan Current Year Cancel Earned Premium],

		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + case WHEN ISDATE(CAST(year(@getdate) AS char(4)) + '0229') = 1 then '0229' else '0228' end then [Feb Current Year Cancel Earned Premium] else 0 end [Feb Current Year Cancel Earned Premium],





		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0331' then [Mar Current Year Cancel Earned Premium] else 0 end [Mar Current Year Cancel Earned Premium],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0430' then [Apr Current Year Cancel Earned Premium] else 0 end [Apr Current Year Cancel Earned Premium],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0531' then [May Current Year Cancel Earned Premium] else 0 end [May Current Year Cancel Earned Premium],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0630' then [Jun Current Year Cancel Earned Premium] else 0 end [Jun Current Year Cancel Earned Premium],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0731' then [Jul Current Year Cancel Earned Premium] else 0 end [Jul Current Year Cancel Earned Premium],





		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0831' then [Aug Current Year Cancel Earned Premium] else 0 end [Aug Current Year Cancel Earned Premium],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0930' then [Sep Current Year Cancel Earned Premium] else 0 end [Sep Current Year Cancel Earned Premium],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '1031' then [Oct Current Year Cancel Earned Premium] else 0 end [Oct Current Year Cancel Earned Premium],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '1130' then [Nov Current Year Cancel Earned Premium] else 0 end [Nov Current Year Cancel Earned Premium],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '1231' then [Dec Current Year Cancel Earned Premium] else 0 end [Dec Current Year Cancel Earned Premium],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0331' then [Current Year First Quarter Cancel Earned Premium] else 0 end [Current Year First Quarter Cancel Earned Premium],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0630' then [Current Year Second Quarter Cancel Earned Premium] else 0 end [Current Year Second Quarter Cancel Earned Premium],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '0930' then [Current Year Third Quarter Cancel Earned Premium] else 0 end [Current Year Third Quarter Cancel Earned Premium],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast(year(@getdate) as varchar) + '1231' then [Current Year Last Quarter Cancel Earned Premium] else 0 end [Current Year Last Quarter Cancel Earned Premium],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast((year(@getdate) -1) as varchar) + '0331' then [Year-1 First Quarter Cancel Earned Premium] else 0 end [Year-1 First Quarter Cancel Earned Premium],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast((year(@getdate) -1) as varchar) + '0630' then [Year-1 Second Quarter Cancel Earned Premium] else 0 end [Year-1 Second Quarter Cancel Earned Premium],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast((year(@getdate) -1) as varchar) + '0930' then [Year-1 Third Quarter Cancel Earned Premium] else 0 end [Year-1 Third Quarter Cancel Earned Premium],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast((year(@getdate) -1) as varchar) + '1231' then [Year-1 Last Quarter Cancel Earned Premium] else 0 end [Year-1 Last Quarter Cancel Earned Premium],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast((year(@getdate) -2) as varchar) + '0331' then [Year-2 First Quarter Cancel Earned Premium] else 0 end [Year-2 First Quarter Cancel Earned Premium],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast((year(@getdate) -2) as varchar) + '0630' then [Year-2 Second Quarter Cancel Earned Premium] else 0 end [Year-2 Second Quarter Cancel Earned Premium],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast((year(@getdate) -2) as varchar) + '0930' then [Year-2 Third Quarter Cancel Earned Premium] else 0 end [Year-2 Third Quarter Cancel Earned Premium],



		case when [contract status identifier] = 2 and [Contract Cancel Date Identifier] <> 19000101 and cast([Contract Cancel Date Identifier] as varchar)  <= cast((year(@getdate) -2) as varchar) + '1231' then [Year-2 Last Quarter Cancel Earned Premium] else 0 end [Year-2 Last Quarter Cancel Earned Premium],

		*/

		--end



			  --Cleaned Dealer Commission

			  case when f."Contract Customer Cost" is not null and f."Contract Customer Cost" >= f."Contract Dealer Cost"
				and f."Contract Customer Cost" > 1 then f."Contract Customer Cost" - f."Contract Dealer Cost"
				else 0 end "Contract Cleaned Dealer Commission"

			 , case when "contract status identifier" = 2 then

				CASE WHEN b."Carrier Code" IN ('HCI', 'HCIA','CNA') THEN f."Contract Admin Fees"

							  when ag."Agent Code" = '002300' then f."Contract Admin Fees"

							  WHEN dp."Source Product Code" ='BLSE' THEN f."Contract Admin Fees"

							  ELSE f."Contract Dealer Cost"
					END

							  else 0

							  end  AS "Cancelled Revenue",

		CASE WHEN "contract status identifier" = 2 then 0 else 1 end as "Net Contract Count"

		--,SUM( CASE WHEN [contract status identifier] = 2 AND cast([Contract Cancel Date Identifier] as varchar)  <= mth.[Calender Last Day of Month] THEN       

		--                CASE WHEN ISNULL(f.[Contract Dealer Cost],0) = ISNULL(f.[Contract Dealer Refund Amount],0) THEN      

		--                    CASE WHEN  (cast(cast([Contract Sale Date Identifier] as varchar) as date) BETWEEN mth.[Calender First Day of Month]       

		--                                        AND mth.[Calender Last Day of Month] )     

		--                    THEN 0      

		--                    ELSE -1 END      

		--                ELSE 0      

		--                END      

		--            ELSE 1      

		--            END )
		,
		     f."Cancellation reason Identifier"

		--[Fact Contract VPartition Claims] fileds start
		,
		     vpclm."As Of yesterday Claim Amount" as "As Of yesterday Claim Amount New"

		           ,
		     vpclm."Jan Current Year Claim Amount" as "Jan Current Year Claim Amount New"

		           ,
		     vpclm."Feb Current Year Claim Amount" as "Feb Current Year Claim Amount New"

		           ,
		     vpclm."Mar Current Year Claim Amount" as "Mar Current Year Claim Amount New"

		           ,
		     vpclm."Apr Current Year Claim Amount" as "Apr Current Year Claim Amount New"

		           ,
		     vpclm."May Current Year Claim Amount" as "May Current Year Claim Amount New"

		           ,
		     vpclm."Jun Current Year Claim Amount" as "Jun Current Year Claim Amount New"

		           ,
		     vpclm."Jul Current Year Claim Amount" as "Jul Current Year Claim Amount New"

		           ,
		     vpclm."Aug Current Year Claim Amount" as "Aug Current Year Claim Amount New"

		           ,
		     vpclm."Sep Current Year Claim Amount" as "Sep Current Year Claim Amount New"

		           ,
		     vpclm."Oct Current Year Claim Amount" as "Oct Current Year Claim Amount New"

		           ,
		     vpclm."Nov Current Year Claim Amount" as "Nov Current Year Claim Amount New"

		           ,
		     vpclm."Dec Current Year Claim Amount" as "Dec Current Year Claim Amount New"

				   ,
		     vpclm."Current Year First Quarter Claim Amount" as "Current Year First Quarter Claim Amount New"

						,
		     vpclm."Current Year Second Quarter Claim Amount" as "Current Year Second Quarter Claim Amount New"

						,
		     vpclm."Current Year Third Quarter Claim Amount" as "Current Year Third Quarter Claim Amount New"

						,
		     vpclm."Current Year Last Quarter Claim Amount" as "Current Year Last Quarter Claim Amount New"

						,
		     vpclm."Current Year First Quarter Claim Count" as "Current Year First Quarter Claim Count New"

						,
		     vpclm."Current Year Second Quarter Claim Count" as "Current Year Second Quarter Claim Count New"

						,
		     vpclm."Current Year Third Quarter Claim Count" as "Current Year Third Quarter Claim Count New"

						,
		     vpclm."Current Year Last Quarter Claim Count" as "Current Year Last Quarter Claim Count New"

		           ,
		     vpclm."Year-1 First Quarter Claim Amount" as "Year-1 First Quarter Claim Amount New"

		           ,
		     vpclm."Year-1 Second Quarter Claim Amount" as "Year-1 Second Quarter Claim Amount New"

		           ,
		     vpclm."Year-1 Third Quarter Claim Amount" as "Year-1 Third Quarter Claim Amount New"

		           ,
		     vpclm."Year-1 Last Quarter Claim Amount" as "Year-1 Last Quarter Claim Amount New"

		           ,
		     vpclm."Year-2 First Quarter Claim Amount" as "Year-2 First Quarter Claim Amount New"

		           ,
		     vpclm."Year-2 Second Quarter Claim Amount" as "Year-2 Second Quarter Claim Amount New"

		           ,
		     vpclm."Year-2 Third Quarter Claim Amount" as "Year-2 Third Quarter Claim Amount New"

		           ,
		     vpclm."Year-2 Last Quarter Claim Amount" as "Year-2 Last Quarter Claim Amount New"

				   ,
		     vpclm."Year-3 Last Quarter Claim Amount" as "Year-3 Last Quarter Claim Amount New"

				   ,
		     vpclm."Year-4 Last Quarter Claim Amount" as "Year-4 Last Quarter Claim Amount New"

				   ,
		     vpclm."Year-5 Last Quarter Claim Amount" as "Year-5 Last Quarter Claim Amount New"

				   ,
		     vpclm."Year-6 Last Quarter Claim Amount" as "Year-6 Last Quarter Claim Amount New"

				   --monthly year-1
				   ,
		     vpclm."Jan Year-1 Claim Amount" as "Jan Year-1 Claim Amount New"

		           ,
		     vpclm."Feb Year-1 Claim Amount" as "Feb Year-1 Claim Amount New"

		           ,
		     vpclm."Mar Year-1 Claim Amount" as "Mar Year-1 Claim Amount New"

		           ,
		     vpclm."Apr Year-1 Claim Amount" as "Apr Year-1 Claim Amount New"

		           ,
		     vpclm."May Year-1 Claim Amount" as "May Year-1 Claim Amount New"

		           ,
		     vpclm."Jun Year-1 Claim Amount" as "Jun Year-1 Claim Amount New"

		           ,
		     vpclm."Jul Year-1 Claim Amount" as "Jul Year-1 Claim Amount New"

		           ,
		     vpclm."Aug Year-1 Claim Amount" as "Aug Year-1 Claim Amount New"

		           ,
		     vpclm."Sep Year-1 Claim Amount" as "Sep Year-1 Claim Amount New"

		           ,
		     vpclm."Oct Year-1 Claim Amount" as "Oct Year-1 Claim Amount New"

		           ,
		     vpclm."Nov Year-1 Claim Amount" as "Nov Year-1 Claim Amount New"

		           ,
		     vpclm."Dec Year-1 Claim Amount" as "Dec Year-1 Claim Amount New"

				   ,
		     NVL(f."Contract Max Locked Date Identifier",19000101) "Contract Activity Max Locked Date Identifier"

				   ,
		     NVL(f."Contract Min Locked Date Identifier",19000101) "Contract Activity Min Locked Date Identifier"

		--[Fact Contract VPartition Claims] fields end
		,CAST(CAST(NVL("Cancel Paid Date", to_timestamp_ntz('19000101')) AS VARCHAR(8)) AS INT)  AS "Cancel Paid Date Identifier"

		,
		     vpclm."As Of yesterday Claim Count" as "As Of Yesterday Claim Count New"

		           ,
		     vpclm."Jan Current Year Claim Count" as "Jan Current Year Claim Count New"

		           ,
		     vpclm."Feb Current Year Claim Count" as "Feb Current Year Claim Count New"

		           ,
		     vpclm."Mar Current Year Claim Count" as "Mar Current Year Claim Count New"

		           ,
		     vpclm."Apr Current Year Claim Count" as "Apr Current Year Claim Count New"

		           ,
		     vpclm."May Current Year Claim Count" as "May Current Year Claim Count New"

		           ,
		     vpclm."Jun Current Year Claim Count" as "Jun Current Year Claim Count New"

		           ,
		     vpclm."Jul Current Year Claim Count" as "Jul Current Year Claim Count New"

		           ,
		     vpclm."Aug Current Year Claim Count" as "Aug Current Year Claim Count New"

		           ,
		     vpclm."Sep Current Year Claim Count" as "Sep Current Year Claim Count New"

		           ,
		     vpclm."Oct Current Year Claim Count" as "Oct Current Year Claim Count New"

		           ,
		     vpclm."Nov Current Year Claim Count" as "Nov Current Year Claim Count New"

		           ,
		     vpclm."Dec Current Year Claim Count" as "Dec Current Year Claim Count New"

		           ,
		     vpclm."Year-1 First Quarter Claim Count" as "Year-1 First Quarter Claim Count New"

		           ,
		     vpclm."Year-1 Second Quarter Claim Count" as "Year-1 Second Quarter Claim Count New"

		           ,
		     vpclm."Year-1 Third Quarter Claim Count" as "Year-1 Third Quarter Claim Count New"

		           ,
		     vpclm."Year-1 Last Quarter Claim Count" as "Year-1 Last Quarter Claim Count New"

		           ,
		     vpclm."Year-2 First Quarter Claim Count" as "Year-2 First Quarter Claim Count New"

		           ,
		     vpclm."Year-2 Second Quarter Claim Count" as "Year-2 Second Quarter Claim Count New"

		           ,
		     vpclm."Year-2 Third Quarter Claim Count" as "Year-2 Third Quarter Claim Count New"

		           ,
		     vpclm."Year-2 Last Quarter Claim Count" as "Year-2 Last Quarter Claim Count New"

				   --monthly year-1
				   ,
		     vpclm."Jan Year-1 Claim Count" as "Jan Year-1 Claim Count New"

		           ,
		     vpclm."Feb Year-1 Claim Count" as "Feb Year-1 Claim Count New"

		           ,
		     vpclm."Mar Year-1 Claim Count" as "Mar Year-1 Claim Count New"

		           ,
		     vpclm."Apr Year-1 Claim Count" as "Apr Year-1 Claim Count New"

		           ,
		     vpclm."May Year-1 Claim Count" as "May Year-1 Claim Count New"

		           ,
		     vpclm."Jun Year-1 Claim Count" as "Jun Year-1 Claim Count New"

		           ,
		     vpclm."Jul Year-1 Claim Count" as "Jul Year-1 Claim Count New"

		           ,
		     vpclm."Aug Year-1 Claim Count" as "Aug Year-1 Claim Count New"

		           ,
		     vpclm."Sep Year-1 Claim Count" as "Sep Year-1 Claim Count New"

		           ,
		     vpclm."Oct Year-1 Claim Count" as "Oct Year-1 Claim Count New"

		           ,
		     vpclm."Nov Year-1 Claim Count" as "Nov Year-1 Claim Count New"

		           ,
		     vpclm."Dec Year-1 Claim Count" as "Dec Year-1 Claim Count New"

				   ,
		     f."Form Deductible Amount"

				   ,
		     f."Vehicle Detail Identifier"

				   ,
		     f.CAN_Dealer_HST_GST

			   ,
		     f.CAN_Dealer_PST

			   ,
		     f.CAN_Dealer_QST

			   ,
		     f.CAN_Customer_HST_GST

			   ,
		     f.CAN_Customer_PST

			   ,
		     f.CAN_Customer_QST

			   ,
		     f.CON_Dealer_HST_GST

			   ,
		     f.CON_Dealer_PST

			   ,
		     f.CON_Dealer_QST

			   ,
		     f.CON_Customer_HST_GST

			   ,
		     f.CON_Customer_PST

			   ,
		     f.CON_Customer_QST

			   ,
		     f."TW Identifier"

			   ,
		     f."Contract Release Date Identifier"

			   ,
		     f."Rate System Identifier"

			   -----added 4 column for cancellation on 16 Aug 2016
		,
		     cancprem."Year-3 Last Quarter Cancel Earned Premium"

		,
		     cancprem."Year-4 Last Quarter Cancel Earned Premium"

		,
		     cancprem."Year-5 Last Quarter Cancel Earned Premium"

		,
		     cancprem."Year-6 Last Quarter Cancel Earned Premium"

		,
		     f."Contract Batch Identifier"

		,
		     f."Contract Create Date Identifier"

		,
		     f."Batch Create Date Identifier"

		,
		     NVL("Original Contract Number", f."Contract Number") as "Original Contract Number"

		,
		     "Contract EAS Info Identifier"

		FROM
		     DW_SAFEGUARD.dbo."Fact Contract" f

		join
				DW_SAFEGUARD.dbo."dimension agent partner" ag
				on ag."Agent Partner Identifier" = f."Agent Partner Identifier"

		INNER JOIN
				DW_SAFEGUARD.dbo."Dimension Product" dp
				on dp."Product Identifier" = f."Product Identifier"

		JOIN
				DW_DATAACCESS.dbo.DateParameter D
				ON 1 = 1
		LEFT OUTER JOIN
				DW_SAFEGUARD.dbo."Dimension Carrier" AS b
				ON f."Carrier Identifier" = b."Carrier Identifier"

		left outer join
				DW_SAFEGUARD.dbo."Fact Contract VPartition Claims" vpclm
				on vpclm."Fact Identifier" = f."Fact Identifier"

		left outer join
				T_tempcube2 cancprem
				on cancprem."Fact Identifier" = f."Fact Identifier"

		WHERE
		     f."Primary Sale Flag" = 'Y' AND NVL("Delete Flag",'N') <> 'Y'
		and "Contract Sale Date Identifier" between :START_VALUE AND :END_VALUE;
	END;
$$;