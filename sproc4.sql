USE DATABASE DW_CONFIGURATION;

--/****** Object:  StoredProcedure [dbo].[Usp_URS_Audit_HCI_VCI_integration]    Script Date: 6/27/2025 12:40:35 PM ******/
----** SSC-FDM-TS0027 - SET ANSI_NULLS ON STATEMENT MAY HAVE A DIFFERENT BEHAVIOR IN SNOWFLAKE **
--SET ANSI_NULLS ON

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

SET QUOTED_IDENTIFIER ON;

--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "[DW_CONFIGURATION].dbo.URS_Prog_Calendar", "DW_Configuration.dbo.URS_Prog_Calendar", "DW_CONFIGURATION.dbo.URS_Premium_Extract", "ODS.cms.bi_hci_contracts_export", "ODS.dbo.VCI_Accounting_Contracts_Ext", "ODS.cms.bi_hci_ppes_contracts_export", "src", "[dw_dataaccess].urs.URS_Claims_Extract", "ODS.cms.bi_hci_claims_export", "[DW_CONFIGURATION].dbo.URS_Claims_Extract", "ODS.dbo.VCI_Accounting_Claim_Ext", "ODS.cms.bi_hci_ppes_claims_export", "[DW_CONFIGURATION].dbo.[URS_Earnings_Extract]", "[DW_CONFIGURATION].dbo.URS_Premium_Extract", "[DW_CONFIGURATION].dbo.Wrk_URS_Earnings_Extract", "dw_stg.dbo.[usp_Send_Email]" **
CREATE OR REPLACE PROCEDURE dbo.Usp_URS_Audit_HCI_VCI_integration ()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 2,  "patch": "6.0" }, "attributes": {  "component": "transact",  "convertedOn": "06-27-2025",  "domain": "test" }}'
EXECUTE AS CALLER
AS
$$
	DECLARE

		-----Declare @monthcode varchar(4) = '0919'                
		MONTHCODE VARCHAR(4) := (Select
				Month_Code
			from
				DW_CONFIGURATION.dbo.URS_Prog_Calendar
			where
				Month_Code = Current_Month_Code
				and Month_Type = 'Present'
AND Agent_Code IN ('001900','001950','002000','003000','003100','003200','003300','003600','003700','003800','003900')
			GROUP BY
				Month_Code
		);
		STARTDATE_HCI DATE := (select
				Start_Date
			from
				T_URS_Prog_Calendar
			where
				Agent_Code IN ('001900'));
		ENDDATE_HCI DATE := (select
				End_Date
			from
				T_URS_Prog_Calendar
			where
				Agent_Code IN ('001900'));
		STARTDATE_VCI DATE := (select
				Start_Date
			from
				T_URS_Prog_Calendar
			where
				Agent_Code IN ('003000'));
		ENDDATE_VCI DATE := (select
				End_Date
			from
				T_URS_Prog_Calendar
			where
				Agent_Code IN ('003000'));
		URS_VS_HCI_CONTRACT_COUNT INT;
		HCI_VS_URS_CONTRACT_COUNT INT;
		URS_VS_VCI_CONTRACT_COUNT INT;
		VCI_VS_URS_CONTRACT_COUNT INT;
		URS_VS_KIA_CONTRACT_COUNT INT;
		KIA_VS_URS_CONTRACT_COUNT INT;
		URS_VS_HCI_CLAIM_COUNT INT;
		HCI_VS_URS_CLAIM_COUNT INT;
		URS_VS_VCI_CLAIM_COUNT INT;
		VCI_VS_URS_CLAIM_COUNT INT;
		URS_VS_KIA_CLAIM_COUNT INT;
		KIA_VS_URS_CLAIM_COUNT INT;
		PREMIUM_VS_EARNING_COUNT INT;
		CNT INT;
		BODY VARCHAR;
		XMLVOID VARCHAR;
	BEGIN
		 
		CREATE OR REPLACE TEMPORARY TABLE dbo.T_URS_Prog_Calendar AS
			SELECT
				Agent_Code,
				Start_Date,
				End_Date,
				Current_Month_Enddate,
				month_code
			from
				DW_Configuration.dbo.URS_Prog_Calendar

			where
				Agent_Code IN ('001900','001950','002000','003000','003100','003200','003300','003600','003700','003800','003900')  AND Month_Code = :MONTHCODE;
		 
		 
		 
		 
		 
		 
		 
		 
		 
		 
		 
		 
		 
		 
		 
		 
		 
		CREATE OR REPLACE TEMPORARY TABLE dbo.T_Contract_validation (
			Extract_Name VARCHAR(50),
			Contract_Count INT,
			Dealer_Cost NUMERIC(10, 2),
			Customer_Cost NUMERIC(10, 2),
			Reserve_amount NUMERIC(10, 2)

		);
		CREATE OR REPLACE TEMPORARY TABLE dbo.T_Claim_validation (
			Extract_Name VARCHAR(50),
			Claim_Count INT,
			Losses_paid_amount NUMERIC(10, 2)

);
		 
		 
		 
		BODY := '<html><body>

                <H2> <span style="color:#A52A2A">URS Audit for HCI and VCI Tables for Monthcode - ' || :MONTHCODE || ' </span>  </H2>   

                <H3> <span > HCI/KIA Cession month starting from - ' || cast(:STARTDATE_HCI as VARCHAR(11)) || ' to ' || cast(:ENDDATE_HCI as VARCHAR(11)) || ' </span>  </H3>

                <H3> <span > VCI Cession month starting from - ' || cast(:STARTDATE_VCI as VARCHAR(11)) || ' to ' || cast(:ENDDATE_VCI as VARCHAR(11)) || ' </span>  </H3>

                </body></html>';
Insert INTO T_Contract_validation

		select 'HCI data in URS Table' as label,
			NVL(COUNT(Ucon.policy),0) Contract_count,
			NVL(SUM(SG_DEALER_COST),0.0) dealer_cost

			,
			NVL(SUM(SG_CUSTOMER_COST),0.0) Customer_cost,
			NVL(SUM(WRITTEN_PREM),0.0) reserve_amt

		from
			DW_CONFIGURATION.dbo.URS_Premium_Extract Ucon

		where
			Ucon.Current_Flag = 'Y' and ucon.SG_CESSION_MONTH = :MONTHCODE

			and ucon.SG_AGENT_CODE in ('001900','001950');
Insert INTO T_Contract_validation

		select 'HCI data' as label,
			COUNT(Hcon.contract_num) ,
			NVL(SUM(Hcon.dealer_cost),0.0) as dealer_cost

			,
			NVL(SUM(Hcon.customer_cost),0.0) as Customer_Cost,
			NVL(SUM(Hcon.reserve_amt),0.0) as reserve_amt

		from
			ODS.cms.bi_hci_contracts_export Hcon

		where
			Hcon.agent_code in ('001900','001950') and NVL(Hcon.rejected,'N') <> 'Y'
AND Hcon.accounting_date BETWEEN :STARTDATE_HCI AND :ENDDATE_HCI;

		Insert INTO T_Contract_validation

		select 'VCI data in URS Table' as label,
			COUNT(Ucon.policy) Contract_count,
			NVL(SUM(SG_DEALER_COST),0.0) dealer_cost

			,
			NVL(SUM(SG_CUSTOMER_COST),0.0) Customer_cost,
			NVL(SUM(WRITTEN_PREM),0.0) reserve_amt

		from
			DW_CONFIGURATION.dbo.URS_Premium_Extract Ucon

		where
			Ucon.Current_Flag = 'Y' and ucon.SG_CESSION_MONTH = :MONTHCODE

			and ucon.SG_AGENT_CODE in ('003000','003100','003200','003300','003600','003700','003800','003900');
Insert INTO T_Contract_validation

		Select  'VCI data' as label,
			COUNT(vcon.contract_num) as Contract_count,
			NVL(SUM(Vcon.dealer_cost),0.0) as dealer_cost

			,
			NVL(SUM(Vcon.customer_cost),0.0) as customer_cost,
			NVL(SUM(vcon.sg_con4_reserve),0.0) Reserve_amt

		From
			ODS.dbo.VCI_Accounting_Contracts_Ext Vcon

		where
			vcon.Agent_Code IN ('003000','003100','003200','003300','003600','003700','003800','003900') AND VCON.Current_Flag = 'Y'
AND vcon.Accounting_MonthEnd_Date = :ENDDATE_VCI;

		Insert INTO T_Contract_validation

		select 'KIA data in URS Table' as label,
			NVL(COUNT(Ucon.policy),0) Contract_count,
			NVL(SUM(SG_DEALER_COST),0.0) dealer_cost

			,
			NVL(SUM(SG_CUSTOMER_COST),0.0) Customer_cost,
			NVL(SUM(WRITTEN_PREM),0.0) reserve_amt

		from
			DW_CONFIGURATION.dbo.URS_Premium_Extract Ucon

		where
			Ucon.Current_Flag = 'Y' and ucon.SG_CESSION_MONTH = :MONTHCODE
			and ucon.SG_AGENT_CODE in ('002000');
Insert INTO T_Contract_validation

		select 'KIA data' as label,
			COUNT(Kcon.contract_num)     ,
			NVL(SUM(Kcon.dealer_cost),0.0) as dealer_cost

			,
			NVL(SUM(Kcon.customer_cost),0.0) as Customer_Cost,
			NVL(SUM(Kcon.reserve_amt),0.0) as reserve_amt

		from
			ODS.cms.bi_hci_ppes_contracts_export Kcon

		where
			Kcon.agent_code in ('002000') and NVL(Kcon.rejected,'N') <> 'Y' AND Kcon.accounting_date BETWEEN :STARTDATE_HCI AND :ENDDATE_HCI;

		---------------- column and Row transposition contract--------------
		CREATE OR REPLACE TEMPORARY TABLE dbo.T_transpose_table_Contract AS
			SELECT
				name,
				SUM(case when Extract_Name = 'HCI data in URS Table' then value
					else 0 end) "HCI data in URS Table",
				SUM(case when Extract_Name = 'HCI data' then value
					else 0 end) "HCI data",
				SUM(case when Extract_Name = 'VCI data in URS Table' then value
					else 0 end) "VCI data in URS Table",
				SUM(case when Extract_Name = 'VCI data' then value
					else 0 end) "VCI data",
				SUM(case when Extract_Name = 'KIA data in URS Table' then value
					else 0 end) "KIA data in URS Table",
				SUM(case when Extract_Name = 'KIA data' then value
					else 0 end) "KIA data"

			from

(

	select
						Extract_Name,
						Contract_Count VALUE, 'Contract_Count' name

	from
						T_Contract_validation

	union all

	select
						Extract_Name,
						Dealer_Cost value, 'Dealer_Cost' name

	from
						T_Contract_validation

	union all

	select
						Extract_Name,
						Customer_Cost value, 'Customer_Cost' name

	from
						T_Contract_validation

	union all

	select
						Extract_Name,
						Reserve_amount value, 'Reserve_amount' name

	from
						T_Contract_validation

				) src
			GROUP BY
				name;
		BEGIN
			XMLVOID := CAST((

				        SELECT
	--** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
	FOR_XML_UDF(OBJECT_CONSTRUCT('"td"', Name, '"td"', "HCI data in URS Table", '"td"', "HCI data", '"td"', CASE WHEN "HCI data in URS Table" = "HCI data"
							THEN '<font Color="Green">' || 'MATCHED' || '</font>' ELSE '<font Color="Red">' || 'UNMATCHED' || '</Font>'  END, '"td"', "VCI data in URS Table", '"td"', "VCI data", '"td"', CASE WHEN "VCI data in URS Table" = "VCI data"
							THEN '<font Color="Green"  >' || 'MATCHED' || '</font>' ELSE '<font Color="Red">' || 'UNMATCHED' || '</Font>'  END, '"td"', "KIA data in URS Table", '"td"', "KIA data", '"td"', CASE WHEN "KIA data in URS Table" = "KIA data"
							THEN '<font Color="Green"  >' || 'MATCHED' || '</font>' ELSE '<font Color="Red">' || 'UNMATCHED' || '</Font>'  END), 'tr')

				                        FROM
	T_transpose_table_Contract
			) AS VARCHAR);
			BODY := :BODY || '<html><body><H3><font Color="Red"> URS Contract validation against HCI, VCI and KIA data.</font> </H3> 

        <table border = 1> 

           <col>

                           <colgroup span="2"></colgroup>

                           <colgroup span="2"></colgroup>

           <tr>

                           <td rowspan="1"></td>

                           <th colspan="3" scope="colgroup">HCI</th>

                           <th colspan="3" scope="colgroup">VCI</th>

						   <th colspan="3" scope="colgroup">KIA</th>

           </tr>     

           <tr>

                           <th> Measure </th>  

                           <th> URS </th> 

                           <th> &nbsp HCI Contracts Extract &nbsp </th> 

                           <th> &nbsp URS vs HCI Extract &nbsp </th> 

                           <th> URS </th> 

                           <th> &nbsp VCI Accounting Contracts Ext &nbsp </th> 

                           <th> &nbsp URS vs VCI Extract &nbsp </th> 

						   <th> URS </th> 

						   <th> &nbsp KIA Accounting Contracts Ext &nbsp </th> 

						   <th> &nbsp URS vs KIA Extract &nbsp </th> 

           </tr>';
			BODY := :BODY || :XMLVOID || '</table></body></html>';
		END;
		CREATE OR REPLACE TEMPORARY TABLE dbo.T_URS_vs_HCI_Contract AS
			SELECT
				ucon.POLICY AS "CONTRACT NUMBER"

			from
				DW_CONFIGURATION.dbo.URS_Premium_Extract Ucon

			where
				Ucon.Current_Flag = 'Y' and ucon.SG_CESSION_MONTH = :MONTHCODE

				and ucon.SG_AGENT_CODE in ('001900','001950')
EXCEPT

select
				hcon.contract_num

			from
				ODS.cms.bi_hci_contracts_export Hcon

			where
				Hcon.agent_code in ('001900','001950') and NVL(Hcon.rejected,'N') <> 'Y'
AND Hcon.accounting_date BETWEEN :STARTDATE_HCI AND :ENDDATE_HCI;
		CREATE OR REPLACE TEMPORARY TABLE dbo.T_HCI_vs_URS_Contract AS
			SELECT
				hcon.contract_num as "CONTRACT NUMBER"

			from
				ODS.cms.bi_hci_contracts_export Hcon

			where
				Hcon.agent_code in ('001900','001950') and NVL(Hcon.rejected,'N') <> 'Y'
AND Hcon.accounting_date BETWEEN :STARTDATE_HCI AND :ENDDATE_HCI

			EXCEPT

select
				ucon.POLICY

			from
				DW_CONFIGURATION.dbo.URS_Premium_Extract Ucon

			where
				Ucon.Current_Flag = 'Y' and ucon.SG_CESSION_MONTH = :MONTHCODE

				and ucon.SG_AGENT_CODE in ('001900','001950');
select
			COUNT(*)
		INTO
			:URS_VS_HCI_CONTRACT_COUNT
 from
			T_URS_vs_HCI_Contract;

		select
			COUNT(*)
		INTO
			:HCI_VS_URS_CONTRACT_COUNT
 from
			T_HCI_vs_URS_Contract;
		IF ((:URS_VS_HCI_CONTRACT_COUNT > 0)) THEN
			BEGIN
				XMLVOID := CAST((

							SELECT
						--** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
						FOR_XML_UDF(OBJECT_CONSTRUCT('"td"', ROW_NUMBER() over (order by (select 1)), '"td"', "CONTRACT NUMBER"), 'tr')

							FROM
						T_URS_vs_HCI_Contract
				) AS VARCHAR);
				BODY := :BODY || '<html><body><H3> URS contract not in HCI table. </H3> <table border = 1> 

								<tr> 

										<th> Sr No </th> 

										<th> CONTRACT NUMBER </th> 

								</tr>';
				BODY := :BODY || :XMLVOID || '</table></body></html>';
			END;
		ELSE
			BEGIN
				BODY := :BODY || '<html><body><H3> URS contract not in HCI table. </H3>

                                                <H4> <font Color="Green"> No such contract found.This check is successful. </font></H4></body></html>';
			END;
		END IF;
		IF ((:HCI_VS_URS_CONTRACT_COUNT > 0)) THEN
			BEGIN
				XMLVOID := CAST((

	                SELECT
						--** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
						FOR_XML_UDF(OBJECT_CONSTRUCT('"td"', ROW_NUMBER() over (order by (select 1)), '"td"', "CONTRACT NUMBER"), 'tr')

	                FROM
						T_HCI_vs_URS_Contract
				) AS VARCHAR);
				BODY := :BODY || '<html><body><H3> HCI contract not in URS table. </H3> <table border = 1> 

							<tr>

                                         <th> Sr No </th> 

                                         <th> CONTRACT NUMBER </th> 

                            </tr>';
				BODY := :BODY || :XMLVOID || '</table></body></html>';
			END;
		ELSE
			BEGIN
				BODY := :BODY || '<html><body><H3> HCI contract not in URS table. </H3>

                                                <H4> <font Color="Green"> No such contract found.This check is successful. </font></H4></body></html>';
			END;
		END IF;
		CREATE OR REPLACE TEMPORARY TABLE dbo.T_URS_vs_VCI_Contract AS
			SELECT
				NVL(ucon.POLICY,0)  AS "CONTRACT NUMBER"

			from
				DW_CONFIGURATION.dbo.URS_Premium_Extract Ucon

			where
				Ucon.Current_Flag = 'Y' and ucon.SG_CESSION_MONTH = :MONTHCODE

				and ucon.SG_AGENT_CODE in ('003000','003100','003200','003300','003600','003700','003800','003900')
EXCEPT

select
				vcon.contract_num

			From
				ODS.dbo.VCI_Accounting_Contracts_Ext Vcon

			where
				vcon.Agent_Code IN ('003000','003100','003200','003300','003600','003700','003800','003900')
                AND vcon.Accounting_MonthEnd_Date = :ENDDATE_VCI
				AND VCON.Current_Flag = 'Y';
		CREATE OR REPLACE TEMPORARY TABLE dbo.T_VCI_vs_URS_Contract AS
			SELECT
				NVL(vcon.contract_num,0) as "CONTRACT NUMBER"

			From
				ODS.dbo.VCI_Accounting_Contracts_Ext Vcon

			where
				vcon.Agent_Code IN ('003000','003100','003200','003300','003600','003700','003800','003900')
                AND vcon.Accounting_MonthEnd_Date = :ENDDATE_VCI
				AND VCON.Current_Flag = 'Y'
EXCEPT

select
				ucon.POLICY

			from
				DW_CONFIGURATION.dbo.URS_Premium_Extract Ucon

			where
				Ucon.Current_Flag = 'Y' and ucon.SG_CESSION_MONTH = :MONTHCODE

				and ucon.SG_AGENT_CODE in ('003000','003100','003200','003300','003600','003700','003800','003900');
select
			COUNT(*)
		INTO
			:URS_VS_VCI_CONTRACT_COUNT
 from
			T_URS_vs_VCI_Contract;

		select
			COUNT(*)
		INTO
			:VCI_VS_URS_CONTRACT_COUNT
 from
			T_VCI_vs_URS_Contract;
		IF ((NVL(:URS_VS_VCI_CONTRACT_COUNT,0) > 0)) THEN
			BEGIN
				XMLVOID := CAST((

	                                SELECT
						--** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
						FOR_XML_UDF(OBJECT_CONSTRUCT('"td"', ROW_NUMBER() over (order by (select 1)), '"td"', "CONTRACT NUMBER"), 'tr')

	                                                FROM
						T_URS_vs_VCI_Contract
				) AS VARCHAR);
				BODY := :BODY || '<html><body><H3> URS contract not in VCI table. </H3> <table border = 1> 

					<tr>

                                   <th> Sr No </th>

                                   <th> CONTRACT NUMBER </th> 

                    </tr>';
				BODY := :BODY || :XMLVOID || '</table></body></html>';
			END;
		ELSE
			BEGIN
				BODY := :BODY || '<html><body><H3> URS contract not in VCI table. </H3>

                                                <H4> <font Color="Green"> No such contract found.This check is successful. </font></H4></body></html>';
			END;
		END IF;
		IF ((NVL(:VCI_VS_URS_CONTRACT_COUNT,0) > 0)) THEN
			BEGIN
				XMLVOID := CAST((

	                                SELECT
						--** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
						FOR_XML_UDF(OBJECT_CONSTRUCT('"td"', ROW_NUMBER() over (order by (select 1)), '"td"', "CONTRACT NUMBER"), 'tr')

	                                                FROM
						T_VCI_vs_URS_Contract
				) AS VARCHAR);
				BODY := :BODY || '<html><body><H3> VCI contract not in URS table. </H3><table border = 1> 

				<tr>

                               <th> Sr No </th>

                               <th> CONTRACT NUMBER </th> 

                </tr>';
				BODY := :BODY || :XMLVOID || '</table></body></html>';
			END;
		ELSE
			BEGIN
				BODY := :BODY || '<html><body><H3> VCI contract not in URS table. </H3>

                                                <H4> <font Color="Green"> No such contract found.This check is successful. </font></H4></body></html>';
			END;
		END IF;
		CREATE OR REPLACE TEMPORARY TABLE dbo.T_URS_vs_KIA_Contract AS
			SELECT
				ucon.POLICY AS "CONTRACT NUMBER"

			from
				DW_CONFIGURATION.dbo.URS_Premium_Extract Ucon

			where
				Ucon.Current_Flag = 'Y' and ucon.SG_CESSION_MONTH = :MONTHCODE

				and ucon.SG_AGENT_CODE in ('002000')
EXCEPT

select
				kcon.contract_num

			from
				ODS.cms.bi_hci_ppes_contracts_export Kcon

			where
				kcon.agent_code in ('002000') and NVL(kcon.rejected,'N') <> 'Y'
AND Kcon.accounting_date BETWEEN :STARTDATE_HCI AND :ENDDATE_HCI;
		CREATE OR REPLACE TEMPORARY TABLE dbo.T_KIA_vs_URS_Contract AS
			SELECT
				kcon.contract_num "CONTRACT NUMBER"

			from
				ODS.cms.bi_hci_ppes_contracts_export Kcon

			where
				kcon.agent_code in ('002000') and NVL(kcon.rejected,'N') <> 'Y'
AND Kcon.accounting_date BETWEEN :STARTDATE_HCI AND :ENDDATE_HCI

			EXCEPT

select
				ucon.POLICY

			from
				DW_CONFIGURATION.dbo.URS_Premium_Extract Ucon

			where
				Ucon.Current_Flag = 'Y' and ucon.SG_CESSION_MONTH = :MONTHCODE

				and ucon.SG_AGENT_CODE in ('002000');
select
			COUNT(*)
		INTO
			:URS_VS_KIA_CONTRACT_COUNT
 from
			T_URS_vs_KIA_Contract;

		select
			COUNT(*)
		INTO
			:KIA_VS_URS_CONTRACT_COUNT
 from
			T_KIA_vs_URS_Contract;
		IF ((:URS_VS_KIA_CONTRACT_COUNT > 0)) THEN
			BEGIN
				XMLVOID := CAST((

	                                SELECT
						--** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
						FOR_XML_UDF(OBJECT_CONSTRUCT('"td"', ROW_NUMBER() over (order by (select 1)), '"td"', "CONTRACT NUMBER"), 'tr')

	                                                FROM
						T_URS_vs_KIA_Contract
				) AS VARCHAR);
				BODY := :BODY || '<html><body><H3> URS contract not in KIA table. </H3> <table border = 1> 

					<tr> 

					        <th> Sr No </th> 

					        <th> CONTRACT NUMBER </th> 

					</tr>';
				BODY := :BODY || :XMLVOID || '</table></body></html>';
			END;
		ELSE
			BEGIN
				BODY := :BODY || '<html><body><H3> URS contract not in KIA table. </H3>

                                                <H4> <font Color="Green"> No such contract found.This check is successful. </font></H4></body></html>';
			END;
		END IF;
		IF ((:KIA_VS_URS_CONTRACT_COUNT > 0)) THEN
			BEGIN
				XMLVOID := CAST((

	                                SELECT
						--** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
						FOR_XML_UDF(OBJECT_CONSTRUCT('"td"', ROW_NUMBER() over (order by (select 1)), '"td"', "CONTRACT NUMBER"), 'tr')

	                                                FROM
						T_KIA_vs_URS_Contract
				) AS VARCHAR);
				BODY := :BODY || '<html><body><H3> KIA contract not in URS table. </H3>  <table border = 1> 

					<tr>

                             <th> Sr No </th> 

                             <th> CONTRACT NUMBER </th> 

					</tr>';
				BODY := :BODY || :XMLVOID || '</table></body></html>';
			END;
		ELSE
			BEGIN
				BODY := :BODY || '<html><body><H3> KIA contract not in URS table. </H3>

                                                <H4> <font Color="Green"> No such contract found.This check is successful. </font></H4></body></html>';
			END;
		END IF;

		--------------------------------------------------URS Claim Extract Part-------------------------------------------------------



----------------------URS with HCI table for '001900','001950'--------------------------------
insert INTO T_Claim_validation

select  'HCI data in URS Table' AS "Label",
			COUNT(uclm.CLAIM_DIRECT) Claim_Count,
			NVL(SUM(uclm.CURR_PAID_LOSS),0.0) Losses_paid

from
			dw_dataaccess.urs.URS_Claims_Extract uclm

where
			uclm.SG_AGENT_CODE in ('001900','001950') and uclm.Inclusion_Reason <> 'Creation of LAE paid records'
and uclm.SG_CESSION_MONTH = :MONTHCODE
			and uclm.Current_Flag = 'Y';
insert INTO T_Claim_validation

select 'HCI data' as Label,
			COUNT(hclm.claim_number) Claim_count,
			NVL(SUM(Hclm.payment_amt),0.0) losses_paid

from
			ODS.cms.bi_hci_claims_export Hclm

where
			Hclm.agent_code in ('001900','001950') and NVL(Hclm.rejected,'N') <> 'Y'
AND Hclm.export_date BETWEEN dateadd(day, 1, :STARTDATE_HCI) AND dateadd(day, 1, :ENDDATE_HCI)
			and hclm.payment_seq <> '';



------------- URS with VCI table for '003000','003100','003200','003300','003600','003700','003800','003900'--------------------------------
insert INTO T_Claim_validation

select 'VCI data in URS Table' AS "Label",
			COUNT(uclm.CLAIM_DIRECT) Claim_Count,
			NVL(SUM(uclm.CURR_PAID_LOSS),0.0) Losses_paid

from
			DW_CONFIGURATION.dbo.URS_Claims_Extract Uclm

where
			uclm.SG_AGENT_CODE in  ('003000','003100','003200','003300','003600','003700','003800','003900')
and uclm.SG_CESSION_MONTH = :MONTHCODE
			and uclm.Current_Flag = 'Y';
insert INTO T_Claim_validation

select  'VCI data' as Label,
			COUNT(vci.claim_number) Claim_Count,
			NVL(SUM(vci.payment_amount),0.0) Losses_paid

from
			ODS.dbo.VCI_Accounting_Claim_Ext VCI ---VCI Claim Table

where
			VCI.Accounting_monthend_date = :ENDDATE_VCI
			and VCi.Agent_Code in ('003000','003100','003200','003300','003600','003700','003800','003900')
AND VCI.Current_flag = 'y' and vci.payment_seq <> '';



 -------------------------------URS KIA for '002000'-----------------------------
insert INTO T_Claim_validation

select  'KIA data in URS Table' AS "Label",
			COUNT(uclm.CLAIM_DIRECT) Claim_Count,
			NVL(SUM(uclm.CURR_PAID_LOSS),0.0) Losses_paid

from
			dw_dataaccess.urs.URS_Claims_Extract uclm

where
			uclm.SG_AGENT_CODE in ('002000') and uclm.Inclusion_Reason <> 'Creation of LAE paid records'
and uclm.SG_CESSION_MONTH = :MONTHCODE
			and uclm.Current_Flag = 'Y';
insert INTO T_Claim_validation

select 'KIA data' as Label,
			COUNT(Kclm.claim_number) Claim_count,
			NVL(SUM(Kclm.payment_amt),0.0) losses_paid

from
			ODS.cms.bi_hci_ppes_claims_export Kclm

where
			Kclm.agent_code in ('002000') and NVL(Kclm.rejected,'N') <> 'Y' and kclm.payment_seq <> ''
AND Kclm.export_date BETWEEN dateadd(day, 1, :STARTDATE_HCI) AND dateadd(day, 1, :ENDDATE_HCI);

----------------column and Row transposition Claim---------
CREATE OR REPLACE TEMPORARY TABLE dbo.T_transpose_table_claim AS
			SELECT
				name,
				SUM(case when Extract_Name = 'HCI data in URS Table' then value
	else 0 end) "HCI data in URS Table",
				SUM(case when Extract_Name = 'HCI data' then value
	else 0 end) "HCI data",
				SUM(case when Extract_Name = 'VCI data in URS Table' then value
	else 0 end) "VCI data in URS Table",
				SUM(case when Extract_Name = 'VCI data' then value
	else 0 end) "VCI data",
				SUM(case when Extract_Name = 'KIA data in URS Table' then value
	else 0 end) "KIA data in URS Table",
				SUM(case when Extract_Name = 'KIA data' then value
	else 0 end) "KIA data"

			from

(

  select
						Extract_Name,
						Claim_Count value, 'Claim_Count' name

	from
						T_Claim_validation

	union all

  select
						Extract_Name,
						Losses_paid_amount value, 'Losses_paid_amount' name

	from
						T_Claim_validation

				) src
			GROUP BY
				name;
BEGIN
			XMLVOID := CAST((

				        SELECT
	--** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
	FOR_XML_UDF(OBJECT_CONSTRUCT('"td"', Name, '"td"', "HCI data in URS Table", '"td"', "HCI data", '"td"', CASE WHEN "HCI data in URS Table" = "HCI data"
							THEN '<font Color="Green">' || 'MATCHED' || '</font>' ELSE '<font Color="Red">' || 'UNMATCHED' || '</Font>'  END, '"td"', "VCI data in URS Table", '"td"', "VCI data", '"td"', CASE WHEN "VCI data in URS Table" = "VCI data"
							THEN '<font Color="Green">' || 'MATCHED' || '</font>' ELSE '<font Color="Red">' || 'UNMATCHED' || '</Font>'  END, '"td"', "KIA data in URS Table", '"td"', "KIA data", '"td"', CASE WHEN "KIA data in URS Table" = "KIA data"
							THEN '<font Color="Green">' || 'MATCHED' || '</font>' ELSE '<font Color="Red">' || 'UNMATCHED' || '</Font>'  END), 'tr')

				                                    FROM
	T_transpose_table_claim
			) AS VARCHAR);
			BODY := :BODY || '<html><body><H3><font Color="red"> URS Claim validation against HCI, VCI and KIA data.</font> </H3> 

            <table border = 1> 

            <col>

					<colgroup span="2"></colgroup>

					<colgroup span="2"></colgroup>

			<tr>

                    <td rowspan="1"></td>

                    <th colspan="3" scope="colgroup">HCI</th>

                    <th colspan="3" scope="colgroup">VCI</th>

					<th colspan="3" scope="colgroup">KIA</th>

            </tr>     

            <tr>

                    <th> Measure </th>  

                    <th> URS </th> 

                    <th> &nbsp HCI Claim Extract &nbsp </th> 

                    <th> &nbsp URS vs HCI Extract &nbsp </th> 

                    <th> URS </th> 

                    <th> &nbsp VCI Accounting Claim Ext &nbsp </th> 

                    <th> &nbsp URS vs VCI Extract &nbsp </th> 

			<th> URS </th> 

                    <th> &nbsp KIA Accounting Claim Ext &nbsp </th> 

                    <th> &nbsp URS vs KIA Extract &nbsp </th> 

               </tr>';
			BODY := :BODY || :XMLVOID || '</table></body></html>';
			BODY := REPLACE(REPLACE(:BODY,'&lt;','<'),'&gt;','>');
END;
CREATE OR REPLACE TEMPORARY TABLE dbo.T_URS_vs_HCI_Claim AS
			SELECT
				(uclm.CLAIM_DIRECT) "CLAIM NUMBER"

		from
				dw_dataaccess.urs.URS_Claims_Extract uclm

		where
				uclm.SG_AGENT_CODE in ('001900','001950') and uclm.Inclusion_Reason <> 'Creation of LAE paid records'
		and uclm.SG_CESSION_MONTH = :MONTHCODE
				and uclm.Current_Flag = 'Y'
EXCEPT

select
				hclm.claim_number "CLAIM NUMBER"

			from
				ODS.cms.bi_hci_claims_export Hclm

			where
				Hclm.agent_code in ('001900','001950') and NVL(Hclm.rejected,'N') <> 'Y'
AND Hclm.export_date BETWEEN dateadd(day, 1, :STARTDATE_HCI) AND dateadd(day, 1, :ENDDATE_HCI)
				and hclm.payment_seq <> '';
CREATE OR REPLACE TEMPORARY TABLE dbo.T_HCI_vs_URS_Claim AS
			SELECT
				hclm.claim_number as "CLAIM NUMBER"

			from
				ODS.cms.bi_hci_claims_export Hclm

			where
				Hclm.agent_code in ('001900','001950') and NVL(Hclm.rejected,'N') <> 'Y'
AND Hclm.export_date BETWEEN dateadd(day, 1, :STARTDATE_HCI) AND dateadd(day, 1, :ENDDATE_HCI)
				and hclm.payment_seq <> ''
EXCEPT

select (uclm.CLAIM_DIRECT) "CLAIM NUMBER"

			from
				dw_dataaccess.urs.URS_Claims_Extract uclm

			where
				uclm.SG_AGENT_CODE in ('001900','001950') and uclm.Inclusion_Reason <> 'Creation of LAE paid records'
and uclm.SG_CESSION_MONTH = :MONTHCODE
				and uclm.Current_Flag = 'Y';
select
			COUNT(*)
INTO
			:URS_VS_HCI_CLAIM_COUNT
 from
			T_URS_vs_HCI_Claim;

select
			COUNT(*)
INTO
			:HCI_VS_URS_CLAIM_COUNT
 from
			T_HCI_vs_URS_Claim;
IF ((:URS_VS_HCI_CLAIM_COUNT > 0)) THEN
			BEGIN
				XMLVOID := CAST((

	                                SELECT
						--** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
						FOR_XML_UDF(OBJECT_CONSTRUCT('"td"', ROW_NUMBER() over (order by (select 1)), '"td"', "CLAIM NUMBER"), 'tr')

	                                                FROM
						T_URS_vs_HCI_Claim
				) AS VARCHAR);
				BODY := :BODY || '<html><body><H3> URS claim not in HCI table. </H3><table border = 1> 

					<tr>

                                   <th> Sr No </th>

                                   <th> CLAIM NUMBER </th> 

                    </tr>';
				BODY := :BODY || :XMLVOID || '</table></body></html>';
			END;
ELSE
			BEGIN
				BODY := :BODY || '<html><body><H3> URS claim not in HCI table. </H3>

                                                <H4> <font Color="Green"> No such claim found.This check is successful. </font></H4></body></html>';
			END;
END IF;
IF ((:HCI_VS_URS_CLAIM_COUNT > 0)) THEN
			BEGIN
				XMLVOID := CAST((

	                                SELECT
						--** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
						FOR_XML_UDF(OBJECT_CONSTRUCT('"td"', ROW_NUMBER() over (order by (select 1)), '"td"', "CLAIM NUMBER"), 'tr')

	                                                FROM
						T_HCI_vs_URS_Claim
				) AS VARCHAR);
				BODY := :BODY || '<html><body><H3> HCI claim not in URS table. </H3> <table border = 1> 

					<tr>

                                   <th> Sr No </th>

                                   <th> CLAIM NUMBER </th> 

                    </tr>';
				BODY := :BODY || :XMLVOID || '</table></body></html>';
			END;
ELSE
			BEGIN
				BODY := :BODY || '<html><body><H3> HCI claim not in URS table. </H3>

                                                <H4> <font Color="Green"> No such claim found.This check is successful. </font></H4></body></html>';
			END;
END IF;
CREATE OR REPLACE TEMPORARY TABLE dbo.T_URS_vs_VCI_Claim AS
			SELECT
				NVL(uclm.CLAIM_DIRECT,0)  AS "CLAIM NUMBER"

			from
				DW_CONFIGURATION.dbo.URS_Claims_Extract Uclm

			where
				uclm.SG_AGENT_CODE in  ('003000','003100','003200','003300','003600','003700','003800','003900')
and uclm.SG_CESSION_MONTH = :MONTHCODE
				and uclm.Current_Flag = 'Y'
EXCEPT

select
				NVL(vci.claim_number,0) as "CLAIM NUMBER"

			from
				ODS.dbo.VCI_Accounting_Claim_Ext VCI

			where
				VCI.Accounting_monthend_date = :ENDDATE_VCI

				and VCi.Agent_Code in ('003000','003100','003200','003300','003600','003700','003800','003900') AND VCI.Current_flag = 'y'
 and vci.payment_seq <> '';
CREATE OR REPLACE TEMPORARY TABLE dbo.T_VCI_vs_URS_Claim AS
			SELECT
				NVL(vci.claim_number,0) as "CLAIM NUMBER"

			from
				ODS.dbo.VCI_Accounting_Claim_Ext VCI

			where
				VCI.Accounting_monthend_date = :ENDDATE_VCI

				and VCi.Agent_Code in ('003000','003100','003200','003300','003600','003700','003800','003900') AND VCI.Current_flag = 'y'
 and vci.payment_seq <> ''
EXCEPT

select
				NVL(uclm.CLAIM_DIRECT,0)  AS "CLAIM NUMBER"

			from
				DW_CONFIGURATION.dbo.URS_Claims_Extract Uclm

			where
				uclm.SG_AGENT_CODE in  ('003000','003100','003200','003300','003600','003700','003800','003900')
and uclm.SG_CESSION_MONTH = :MONTHCODE
				and uclm.Current_Flag = 'Y';
select
			COUNT(*)
INTO
			:URS_VS_VCI_CLAIM_COUNT
 from
			T_URS_vs_VCI_Claim;

select
			COUNT(*)
INTO
			:VCI_VS_URS_CLAIM_COUNT
 from
			T_VCI_vs_URS_Claim;
IF ((NVL(:URS_VS_VCI_CLAIM_COUNT,0) > 0)) THEN
			BEGIN
				XMLVOID := CAST((

	                                SELECT
						--** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
						FOR_XML_UDF(OBJECT_CONSTRUCT('"td"', ROW_NUMBER() over (order by (select 1)), '"td"', "CLAIM NUMBER"), 'tr')

	                                                FROM
						T_URS_vs_VCI_Claim
				) AS VARCHAR);
				BODY := :BODY || '<html><body><H3> URS claim not in VCI table. </H3> 

				<table border = 1> 

				<tr>

                               <th> Sr No </th>

                               <th> CLAIM NUMBER </th> 

                </tr>';
				BODY := :BODY || :XMLVOID || '</table></body></html>';
			END;
ELSE
			BEGIN
				BODY := :BODY || '<html><body><H3> URS claim not in VCI table. </H3>

                                             <H4> <font Color="Green"> No such claim found.This check is successful. </font></H4></body></html>';
			END;
END IF;
IF ((NVL(:VCI_VS_URS_CLAIM_COUNT,0) > 0)) THEN
			BEGIN
				XMLVOID := CAST((

	                                SELECT
						--** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
						FOR_XML_UDF(OBJECT_CONSTRUCT('"td"', ROW_NUMBER() over (order by (select 1)), '"td"', "CLAIM NUMBER"), 'tr')

	                                                FROM
						T_VCI_vs_URS_Claim
				) AS VARCHAR);
				BODY := :BODY || '<html><body><H3> VCI claim not in URS table. </H3> 

					<table border = 1> 

					<tr>

                                   <th> Sr No </th>

                                   <th> CLAIM NUMBER </th> 

                    </tr>';
				BODY := :BODY || :XMLVOID || '</table></body></html>';
			END;
ELSE
			BEGIN
				BODY := :BODY || '<html><body><H3> VCI claim not in URS table. </H3>

                                                <H4> <font Color="Green"> No such claim found.This check is successful. </font></H4></body></html>';
			END;
END IF;
CREATE OR REPLACE TEMPORARY TABLE dbo.T_URS_vs_KIA_Claim AS
			SELECT
				(uclm.CLAIM_DIRECT) "CLAIM NUMBER"

		from
				dw_dataaccess.urs.URS_Claims_Extract uclm

		where
				uclm.SG_AGENT_CODE in ('002000') and uclm.Inclusion_Reason <> 'Creation of LAE paid records'
		and uclm.SG_CESSION_MONTH = :MONTHCODE
				and uclm.Current_Flag = 'Y'
EXCEPT

select
				Kclm.claim_number "CLAIM NUMBER"

			from
				ODS.cms.bi_hci_ppes_claims_export Kclm

			where
				Kclm.agent_code in ('002000') and NVL(Kclm.rejected,'N') <> 'Y'
AND Kclm.export_date BETWEEN dateadd(day, 1, :STARTDATE_HCI) AND dateadd(day, 1, :ENDDATE_HCI)
				and Kclm.payment_seq <> '';
CREATE OR REPLACE TEMPORARY TABLE dbo.T_KIA_vs_URS_Claim AS
			SELECT
				Kclm.claim_number as "CLAIM NUMBER"

			from
				ODS.cms.bi_hci_ppes_claims_export Kclm

			where
				Kclm.agent_code in ('002000') and NVL(Kclm.rejected,'N') <> 'Y'
AND Kclm.export_date BETWEEN dateadd(day, 1, :STARTDATE_HCI) AND dateadd(day, 1, :ENDDATE_HCI)
				and Kclm.payment_seq <> ''
EXCEPT

select (uclm.CLAIM_DIRECT) "CLAIM NUMBER"

			from
				dw_dataaccess.urs.URS_Claims_Extract uclm

			where
				uclm.SG_AGENT_CODE in ('002000') and uclm.Inclusion_Reason <> 'Creation of LAE paid records'
and uclm.SG_CESSION_MONTH = :MONTHCODE
				and uclm.Current_Flag = 'Y';
select
			COUNT(*)
INTO
			:KIA_VS_URS_CLAIM_COUNT
 from
			T_URS_vs_KIA_Claim;

select
			COUNT(*)
INTO
			:URS_VS_KIA_CLAIM_COUNT
 from
			T_KIA_vs_URS_Claim;
IF ((:KIA_VS_URS_CLAIM_COUNT > 0)) THEN
			BEGIN
				XMLVOID := CAST((

	                                SELECT
						--** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
						FOR_XML_UDF(OBJECT_CONSTRUCT('"td"', ROW_NUMBER() over (order by (select 1)), '"td"', "CLAIM NUMBER"), 'tr')

	                                                FROM
						T_URS_vs_KIA_Claim
				) AS VARCHAR);
				BODY := :BODY || '<html><body><H3> URS claim not in KIA table. </H3>   <table border = 1> 

					<tr>

                             <th> Sr No </th>

                             <th> CLAIM NUMBER </th> 

					</tr>';
				BODY := :BODY || :XMLVOID || '</table></body></html>';
			END;
ELSE
			BEGIN
				BODY := :BODY || '<html><body><H3> URS claim not in KIA table. </H3>

                                                <H4> <font Color="Green"> No such claim found.This check is successful. </font></H4></body></html>';
			END;
END IF;
IF ((:URS_VS_KIA_CLAIM_COUNT > 0)) THEN
			BEGIN
				XMLVOID := CAST((

	                                SELECT
						--** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
						FOR_XML_UDF(OBJECT_CONSTRUCT('"td"', ROW_NUMBER() over (order by (select 1)), '"td"', "CLAIM NUMBER"), 'tr')

	                                                FROM
						T_KIA_vs_URS_Claim
				) AS VARCHAR);
				BODY := :BODY || '<html><body><H3> KIA claim not in URS table. </H3>  <table border = 1> 

				<tr>

                              <th> Sr No </th>

                              <th> CLAIM NUMBER </th> 

               </tr>';
				BODY := :BODY || :XMLVOID || '</table></body></html>';
			END;
ELSE
			BEGIN
				BODY := :BODY || '<html><body><H3> KIA claim not in URS table. </H3>

                                                <H4> <font Color="Green"> No such claim found.This check is successful. </font></H4></body></html>';
			END;
END IF;

------------------------------------------URS Earing part--------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE dbo.T_Earning AS
			SELECT
				*
			from
				DW_CONFIGURATION.dbo.URS_Earnings_Extract Ear

			where
				ear.SG_CESSION_MONTH = :MONTHCODE
				and ear.Current_Flag = 'Y';
CREATE OR REPLACE TEMPORARY TABLE dbo.T_Premium AS
			SELECT
				*
			from
				DW_CONFIGURATION.dbo.URS_Premium_Extract PRM

			where
				PRM.SG_CESSION_MONTH = :MONTHCODE
				and PRM.Current_Flag = 'Y';
CREATE OR REPLACE TEMPORARY TABLE dbo.T_Earning_validation AS
			SELECT
				POLICY "Contract Number"

			from
				DW_CONFIGURATION.dbo.Wrk_URS_Earnings_Extract WRK

			Where
				WRK.POLICY IN(

			Select
						POLICY
	from
						T_Premium

	Except

			select
						POLICY
	from
						T_Earning

				) And (NVL(WRK.PRV_UNEARNED_PREM,0)<> 0 or wrk.is_first_Earn_Record <> 0);
select
			COUNT(*)
INTO
			:PREMIUM_VS_EARNING_COUNT
 from
			T_Earning_validation;
IF ((:PREMIUM_VS_EARNING_COUNT > 0)) THEN
			BEGIN
				XMLVOID := CAST((

	                                SELECT
						--** SSC-FDM-TS0016 - XML COLUMNS IN SNOWFLAKE MIGHT HAVE A DIFFERENT FORMAT **
						FOR_XML_UDF(OBJECT_CONSTRUCT('"td"', ROW_NUMBER() over (order by (select 1)), '"td"', "Contract Number"), 'tr')

	                                                FROM
						T_Earning_validation
				) AS VARCHAR);
				BODY := :BODY || '<html><body><H3><font Color="red"> URS Premium Contract vs URS Earning Contract Validation. </font></H3> 

				<table border = 1> 

				<tr>

                             <th> Sr No </th>

                             <th> CONTRACT NUMBER </th> 

				</tr>';
				BODY := :BODY || :XMLVOID || '</table></body></html>';
			END;
ELSE
			BEGIN
				BODY := :BODY || '<html><body><H3><font Color="red"> URS Premium Contract vs URS Earning Contract Validation.</font> </H3>

                                                <H4> <font Color="Green"> No difference found, the check is successful. </font></H4></body></html>';
			END;
END IF;
BODY := REPLACE(REPLACE(:BODY,'&lt;','<'),'&gt;','>');
CALL dw_stg.dbo.usp_Send_Email('URS_Integration_Audit', 'N', :BODY);
DROP TABLE IF EXISTS T_URS_Prog_Calendar;
DROP TABLE IF EXISTS T_Contract_validation;
DROP TABLE IF EXISTS T_transpose_table_contract;
DROP TABLE IF EXISTS T_URS_vs_HCI_Contract;
DROP TABLE IF EXISTS T_HCI_vs_URS_Contract;
DROP TABLE IF EXISTS T_URS_vs_VCI_Contract;
DROP TABLE IF EXISTS T_VCI_vs_URS_Contract;
DROP TABLE IF EXISTS T_URS_vs_KIA_Contract;
DROP TABLE IF EXISTS T_KIA_vs_URS_Contract;
DROP TABLE IF EXISTS T_Claim_validation;
DROP TABLE IF EXISTS T_transpose_table_claim;
DROP TABLE IF EXISTS T_URS_vs_HCI_Claim;
DROP TABLE IF EXISTS T_HCI_vs_URS_Claim;
DROP TABLE IF EXISTS T_URS_vs_VCI_Claim;
DROP TABLE IF EXISTS T_VCI_vs_URS_Claim;
DROP TABLE IF EXISTS T_URS_vs_KIA_Claim;
DROP TABLE IF EXISTS T_KIA_vs_URS_Claim;
DROP TABLE IF EXISTS T_Earning;
DROP TABLE IF EXISTS T_Premium;
DROP TABLE IF EXISTS T_Earning_validation;
	END;
$$;