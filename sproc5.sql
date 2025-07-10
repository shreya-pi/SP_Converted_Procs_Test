USE DATABASE DW_Safeguard;

--/****** Object:  StoredProcedure [dbo].[sp_an_mgmt_fee_report_audit]    Script Date: 6/27/2025 12:42:04 PM ******/
----** SSC-FDM-TS0027 - SET ANSI_NULLS ON STATEMENT MAY HAVE A DIFFERENT BEHAVIOR IN SNOWFLAKE **
--SET ANSI_NULLS ON

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

SET QUOTED_IDENTIFIER ON;

/*

==================================================================================================================

Name		: sp_an_mgmt_fee_report_audit

Type		: Stored Procedure

Purpose		: This Stored Procedure is used to compare the data between CMS VS BI for AN Management Fee Report 

Created Date: 21-JULY-2015

Created By	: Malay

Version		: 0.1

==================================================================================================================

Version		Modified By	  Modified On	  Changes/Remarks

==================================================================================================================

0.1										  Initial Version

0.2			Malay          Sep-06-2016     Added New Bently Product BEPM in Audit Query

0.3			Amit           Jan-31-2017     Added VCI Products

0.4			Amit           Mar-03-2017     Added AUTV Product

0.5			Amit           Mar-22-2017     Added QSVS Product

0.6			Amit           Jun-05-2017     Added AKEY and BEMS Product

0.7			VIKRAM         Jun-30-2017     Added ANDD ANF BEGP Product

0.8			Thirmal		  Aug-23-2017	  Added HYPM Product.

0.9			Swapnil		  Feb-05-2018	  Added Porsche Dealer (PORS0260) for cross business 

1.0			Kumar			2018-03-05		Added GEVS, GETC Products.

1.1			Veda prakash	2018-04-06		Added product code AUCE

1.2			Swapnil Alle	2019-11-20		Added filter sort not in (2000)

1.3			Swapnil Alle	2020-04-01		Added additional dealer's information (sg_con4_dealer) in case condition of CMS open query

1.4			Sreeni			2020-06-03		Added BMLS and MILS,'BMGP','BMPL','MIPL','MIGP'

1.5			Vikram B		2020-06-05		Added agent 19/20 buckets in cms sql in filter and select for CMS and BI sql.

1.6			Swapnil Alle	2020-08-04		Added HYBW Product code.

1.7			Rohit Sharma	2020-10-20		Added New Product Codes for HYSW,Porsche(Key,Tire & Wheel) and Benley(Key,Tire & Wheel,Windshield and Dent)

1.8			Rohit Sharma	2020-12-02		Added New Product Codes for ('BMMC','MIMC','BMMD')

1.9			Prateek Mishra	2021-06-16		Added new Product code VWTS

2.0			Prateek Mishra	2021-06-22		Added new Product Code AUTS

2.1			Swapnil Alle	2021-11-03		Added 3 case statements for dealer HYUGA090,HYUSC050,AU407C22 in postgres open query.

2.2			Swapnil Alle	2021-12-02		Added 3 case statements for dealer 0GFGA722,LRV00674,VW407351 in postgres open query. Also added 2 new products 'GESW','GEBW' in check #7.

2.3			Swapnil Alle	2022-01-04		Added 6 case statements for dealer 0GFSC711,PORS0752,AU409A16,G5094811,G5100911,G5100921 in postgres open query.

2.4			Prateek Mishra  2022-02-01		Added 4 case statements for dealer H5100934,JAG05474,LRV00681,PORS1451 in postgres open query

2.5			Prateek Mishra	2022-03-01		Added 1 case statement for the dealer LRV00675 in postgres open query

2.6			Swapnil Alle	2022-04-04		Added 1 case statement for the dealer VW405115 in postgres open query.

2.7			Swapnil Alle	2022-05-03		Added 2 case statement for the dealer HYUGA075 and HYUSC049 in postgres open query.

2.8			Atul Shelke		2022-06-02		Added new Product code HYVO

2.9			Atul Shelke  	2022-10-05		Added 1 case statement for the dealer JAG05485 in postgres open query.

===================================================================================================================

*/
--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "[DW_HIST].[dbo].sg_con_d4", "[DW_HIST].[dbo].[sg_con_m1]", "[DW_HIST].[dbo].[sg_dlr_m1]", "[DW_HIST].[dbo].[sg_grp_d1]", "DW_HIST.dbo.sg_dgrp_d1", "DW_HIST.dbo.sg_dgrp_m1", "[DW_HIST].[dbo].[sg_con_d4]", "[DW_HIST].[dbo].[an_dealer_mapping]", "DW_SAFEGUARD.dbo.Stencil_data", "DW_SAFEGUARD.dbo.Stencil_data_audit", "[DW_STG].[dbo].[usp_Send_Email]" **
CREATE OR REPLACE PROCEDURE dbo.sp_an_mgmt_fee_report_audit ()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 2,  "patch": "6.0" }, "attributes": {  "component": "transact",  "convertedOn": "06-27-2025",  "domain": "test" }}'
EXECUTE AS CALLER

--EXEC qtsdevbidw.msdb.dbo.Sp_send_dbmail @profile_name='Safeguard dbmail', @recipients ='salle@sgintl.com;vechelim@sgintl.com', @copy_recipients =NULL, @subject =@SUBJ, @body_format = 'HTML', @body = @BodyMain
AS
$$
	DECLARE
		BIDLRCOUNT VARCHAR(20);
		CMSDLRCOUNT VARCHAR(20);
		BICONCOUNT VARCHAR(20);
		CMSCONCOUNT VARCHAR(20);
		BIPRODUCT VARCHAR(20);
		CMSPRODUCT VARCHAR(20);
		BIAMOUNT VARCHAR(50);
		CMSAMOUNT VARCHAR(50);
		DLRCNTSTS VARCHAR(20);
		CONCNTSTS VARCHAR(20);
		PRODCTCNTSTS VARCHAR(20);
		AMTSTS VARCHAR(20);
		GPSTENCILDLR VARCHAR(20);
		BISTENCILDLR VARCHAR(20);
		GPSAFEPARTSDLR VARCHAR(20);
		BISAFEPARTSDLR VARCHAR(20);
		GPSTENCILSAMT VARCHAR(50);
		BISTENCILSAMT VARCHAR(50);
		GPSAFEPARTSAMT VARCHAR(50);
		BISAFEPARTSAMT VARCHAR(50);
		GPTOTALMGMTFEE VARCHAR(50);
		BITOTALMGMTFEE VARCHAR(50);
		STNCLDLRSTS VARCHAR(20);
		STNCLAMTSTS VARCHAR(20);
		SFPTDLRSTS VARCHAR(20);
		SFPTAMTSTS VARCHAR(20);
		TMGMFSTS VARCHAR(20);
		TSQL VARCHAR;
		SERVERNAME VARCHAR(50);
		LINKEDSERVERNAME VARCHAR(50);
		BODYMAIN VARCHAR;
		MONTHYEAR VARCHAR(50);
		STARTDATE DATE := (select
				DATEADD(MONTH, DATEDIFF(MONTH, ('01/01/1900' :: DATE), CURRENT_TIMESTAMP() :: TIMESTAMP) -1, ('01/01/1900' :: DATE))
		);
		ENDDATE DATE := (select
				DATEADD(MONTH, DATEDIFF(MONTH, ('01/01/1900' :: DATE + INTERVAL '-1 days'), CURRENT_TIMESTAMP() :: TIMESTAMP) -1, ('01/01/1900' :: DATE + INTERVAL '-1 days'))
		);

		---select @BodyMain
		SUBJ VARCHAR(100) := :SERVERNAME || 'AUDIT ON AN MANAGEMENT FEE REPORT AS OF ' || :MONTHYEAR;
	BEGIN
		 
		 
		 
		 
		MONTHYEAR := (select
				UPPER(DATE_PART(month, :ENDDATE :: TIMESTAMP) +' '+cast(DATE_PART(year, :ENDDATE :: TIMESTAMP) as VARCHAR)));
		---To store the cms data
		IF (OBJECT_ID_UDF('TEMPDB.DBO.T_CMS_TEMP') IS NOT NULL) THEN
			DROP TABLE IF EXISTS T_CMS_temp;
		END IF;
		CREATE OR REPLACE TEMPORARY TABLE dbo.T_CMS_temp (
			sg_con4_dealer VARCHAR(8) NULL,
			sg_con4_plc VARCHAR(4) NULL,
			sg_con4_busdate TIMESTAMP_NTZ(3) NULL,
			sg_con4_rectype CHAR(1) NULL,
			sg_con_cover VARCHAR(8) NULL,
			sg_con_plc VARCHAR(4) NULL,
			sg_con_contract VARCHAR(10) NOT NULL,
			sg_con_term INT NULL,
			sg_con_form VARCHAR(15) NULL,
			sg_con4_seq VARCHAR(2) NULL,
			sg_con4_agent_0 VARCHAR(6) NULL,
			sg_con4_amt_0 NUMERIC(8, 2) NULL,
			sg_con4_agent_4 VARCHAR(6) NULL,
			sg_con4_amt_4 NUMERIC(8, 2) NULL,
			sg_con4_agent_6 VARCHAR(6) NULL,
			sg_con4_amt_6 NUMERIC(8, 2) NULL,
			sg_con4_agent_7 VARCHAR(6) NULL,
			sg_con4_amt_7 NUMERIC(8, 2) NULL,
			sg_con4_agent_8 VARCHAR(6) NULL,
			sg_con4_amt_8 NUMERIC(8, 2) NULL,
			sg_con4_agent_9 VARCHAR(6) NULL,
			sg_con4_amt_9 NUMERIC(8, 2) NULL,
			sg_con4_agent_10 VARCHAR(6) NULL,
			sg_con4_amt_10 NUMERIC(8, 2) NULL,
			sg_con4_agent_11 VARCHAR(6) NULL,
			sg_con4_amt_11 NUMERIC(8, 2) NULL,
			sg_con4_agent_12 VARCHAR(6) NULL,
			sg_con4_amt_12 NUMERIC(8, 2) NULL,
			sg_con4_agent_13 VARCHAR(6) NULL,
			sg_con4_amt_13 NUMERIC(8, 2) NULL,
			sg_con4_agent_14 VARCHAR(6) NULL,
			sg_con4_amt_14 NUMERIC(8, 2) NULL,
			sg_con4_agent_15 VARCHAR(6) NULL,
			sg_con4_amt_15 NUMERIC(8, 2) NULL,
			sg_con4_agent_16 VARCHAR(6) NULL,
			sg_con4_amt_16 NUMERIC(8, 2) NULL,
			sg_con4_agent_17 VARCHAR(6) NULL,
			sg_con4_amt_17 NUMERIC(8, 2) NULL,
			sg_con4_agent_18 VARCHAR(6) NULL,
			sg_con4_amt_18 NUMERIC(8, 2) NULL,
			sg_con4_agent_19 VARCHAR(6) NULL,
			sg_con4_amt_19 NUMERIC(8, 2) NULL,
			sg_con4_agent_20 VARCHAR(6) NULL,
			sg_con4_amt_20 NUMERIC(8, 2) NULL

	--[Source] char(3) NULL

);
		---to get the servername in subjectline
		SERVERNAME := (select case

												when CONCAT('app.snowflake.com/', CURRENT_ACCOUNT()) ='SGDW-DEV\SGDWDEV' then 'BI DEV:-'

												when CONCAT('app.snowflake.com/', CURRENT_ACCOUNT()) ='QTSQABIDB-1' then 'BI QA:-'

												else

												''

												end

									);
		---to get the linked servername in subjectline
		LINKEDSERVERNAME := (select case

																			when CONCAT('app.snowflake.com/', CURRENT_ACCOUNT()) ='SGDW-DEV\SGDWDEV' then 'POSTGRE32_ANSI'

																			when CONCAT('app.snowflake.com/', CURRENT_ACCOUNT()) ='QTSQABIDB-1' then 'POSTGRE32_ANSI'

																			else

																			'POSTGRE32_ANSI'

																			end

																);
		---Linkedserver connection using OpenQuery to load the cms data
		TSQL := 'SELECT a.* FROM OpenQuery(' || :LINKEDSERVERNAME || ','' select CASE 

WHEN trim(sg_con_d4.sg_con4_dealer)=''''BENT3233'''' THEN ''''N1004559''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''HYUAZ046'''' THEN ''''N0391569''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''HYUCO027'''' THEN ''''N0905550''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''HYUGA065'''' THEN ''''N0191539''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''HYUIL085'''' THEN ''''N0291572''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''HYUTX081'''' THEN ''''N0791521''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''HYUTX181'''' THEN ''''N0191621''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''HYUTX182'''' THEN ''''N0191610''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''HYUWA047'''' THEN ''''N0291537''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''PORS0254'''' THEN ''''N1004559''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''PORS0670'''' THEN ''''N0391560''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''PORS0917'''' THEN ''''N0691507''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''PORS1966'''' THEN ''''N0591576''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''AU402A20'''' THEN ''''N0413660''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''AU405A56'''' THEN ''''N0491574''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''AU409A02'''' THEN ''''N0191589''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''AU419D98'''' THEN ''''N0291532''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''AU422A98'''' THEN ''''N0191555''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''AU422C04'''' THEN ''''N0291541''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''AU422G05'''' THEN ''''N0291510''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''AU423D37'''' THEN ''''N0591578''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''AU425A72'''' THEN ''''N0391561''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''VW405137'''' THEN ''''N0191607''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''VW405146'''' THEN ''''N0191580''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''VW419303'''' THEN ''''N0291571''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''VW422607'''' THEN ''''N0228523''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''PORS0260'''' THEN ''''N0191633'''' 

WHEN trim(sg_con_d4.sg_con4_dealer)=''''LRV00643'''' THEN ''''N0191640''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''JAG05356'''' THEN ''''N0191630''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''LRV00146'''' THEN ''''N1004556''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''JAG05408'''' THEN ''''N0191640''''

wHEN trim(sg_con_d4.sg_con4_dealer)=''''LRV00615'''' THEN ''''N0191625''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''LRV00342'''' THEN ''''N0591520''''

	

WHEN trim(sg_con_d4.sg_con4_dealer)=''''JAG05335'''' THEN ''''N0191586''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''JAG05343'''' THEN ''''N0191622''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''JAG05345'''' THEN ''''N0191625''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''JAG05349'''' THEN ''''N0191628''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''JAG05368'''' THEN ''''N1004556''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''LRV00104'''' THEN ''''N1004635''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''LRV00605'''' THEN ''''N0191586''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''LRV00614'''' THEN ''''N0191624''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''LRV00616'''' THEN ''''N0191622''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''LRV00619'''' THEN ''''N0191628''''

	 

WHEN trim(sg_con_d4.sg_con4_dealer)=''''G0492311'''' THEN ''''N0304645''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''G1073211'''' THEN ''''N0346529''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''G2129421'''' THEN ''''N0228521''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''G2129411'''' THEN ''''N0228520''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''G2147111'''' THEN ''''N0742592''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''G2161811'''' THEN ''''N0304640''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''G2218011'''' THEN ''''N1004632''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''G2485611'''' THEN ''''N0304734''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''G2498711'''' THEN ''''N0413661''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''G2673111'''' THEN ''''N0891508''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''G3095611'''' THEN ''''N0391523''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''G3208211'''' THEN ''''N0291526''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''G4399411'''' THEN ''''N0191623''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''G4461011'''' THEN ''''N0191627''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''G4528911'''' THEN ''''N0791503''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''G4658611'''' THEN ''''N0191637''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''G4704611'''' THEN ''''N0191639''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''H2129444'''' THEN ''''N0491540''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''H2147144'''' THEN ''''N0791504''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''H2673114'''' THEN ''''N0891508''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''H3626914'''' THEN ''''N0591542''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''H3944814'''' THEN ''''N0691568''''



WHEN trim(sg_con_d4.sg_con4_dealer)=''''HYUGA090'''' THEN ''''N0191650''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''HYUSC050'''' THEN ''''N0191647''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''AU407C22'''' THEN ''''N0191645''''





WHEN trim(sg_con_d4.sg_con4_dealer)=''''0GFGA722'''' THEN ''''N0191650''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''LRV00674'''' THEN ''''N0191642''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''VW407351'''' THEN ''''N0191648''''



WHEN trim(sg_con_d4.sg_con4_dealer)=''''0GFSC711'''' THEN ''''N0191647''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''PORS0752'''' THEN ''''N0191644''''

	 

WHEN trim(sg_con_d4.sg_con4_dealer)=''''AU409A16'''' THEN ''''N0191660''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''G5094811'''' THEN ''''N0191664''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''G5100911'''' THEN ''''N0191661''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''G5100921'''' THEN ''''N0191662''''



WHEN trim(sg_con_d4.sg_con4_dealer)=''''H5100934'''' THEN ''''N0191663''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''JAG05474'''' THEN ''''N0191642''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''LRV00681'''' THEN ''''N0191666''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''PORS1451'''' THEN ''''N0191667''''



WHEN trim(sg_con_d4.sg_con4_dealer)=''''LRV00675'''' THEN ''''N0191651''''



WHEN trim(sg_con_d4.sg_con4_dealer)=''''VW405115'''' THEN ''''N0191649''''



WHEN trim(sg_con_d4.sg_con4_dealer)=''''HYUGA075'''' THEN ''''N0191606''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''HYUSC049'''' THEN ''''N0191652''''

WHEN trim(sg_con_d4.sg_con4_dealer)=''''JAG05485'''' THEN ''''N0191666''''





ELSE

sg_con_d4.sg_con4_dealer

END sg_con4_dealer,sg_con_d4.sg_con4_plc,sg_con_d4.sg_con4_busdate, sg_con_d4.sg_con4_rectype,sg_con_m1.sg_con_cover

,sg_con_m1.sg_con_plc,sg_con_m1.sg_con_contract,sg_con_m1.sg_con_term,sg_con_m1.sg_con_form,sg_con_d4.sg_con4_seq,sg_con4_agent_0,sg_con4_amt_0

,sg_con4_agent_4,sg_con4_amt_4,sg_con4_agent_6,sg_con4_amt_6,sg_con4_agent_7,sg_con4_amt_7,sg_con4_agent_8,sg_con4_amt_8,sg_con4_agent_9,sg_con4_amt_9,sg_con4_agent_10,sg_con4_amt_10

,sg_con4_agent_11,sg_con4_amt_11,sg_con4_agent_12,sg_con4_amt_12,sg_con4_agent_13,sg_con4_amt_13,sg_con4_agent_14,sg_con4_amt_14,sg_con4_agent_15,sg_con4_amt_15

,sg_con4_agent_16,sg_con4_amt_16,sg_con4_agent_17,sg_con4_amt_17,sg_con4_agent_18,sg_con4_amt_18

,sg_con4_agent_19,sg_con4_amt_19,sg_con4_agent_20,sg_con4_amt_20

FROM sg_con_d4 as sg_con_d4

INNER JOIN sg_con_m1 sg_con_m1 ON sg_con_d4.sg_con4_contract=sg_con_m1.sg_con_contract AND sg_con_d4.sg_con4_posted=''''y'''' 

where sg_con_d4.sg_con4_plc not in (''''VWPL'''') and (sg_con4_agent_0=''''000001'''' or sg_con4_agent_1=''''000001'''' or sg_con4_agent_2=''''000001'''' or sg_con4_agent_3=''''000001'''' or sg_con4_agent_4=''''000001'''' or sg_con4_agent_5=''''000001'''' 

or sg_con4_agent_6=''''000001''''or sg_con4_agent_7=''''000001'''' or sg_con4_agent_8=''''000001'''' or sg_con4_agent_9=''''000001'''' or sg_con4_agent_10=''''000001'''' or sg_con4_agent_11=''''000001'''' 

or sg_con4_agent_12=''''000001'''' or sg_con4_agent_13=''''000001'''' or sg_con4_agent_14=''''000001'''' or sg_con4_agent_15=''''000001'''' or sg_con4_agent_16=''''000001''''or sg_con4_agent_17=''''000001'''' 

or sg_con4_agent_18=''''000001''''

or sg_con4_agent_19=''''000001''''

or sg_con4_agent_20=''''000001''''

) and (sg_con_d4.sg_con4_busdate>= (select cast(date_trunc(''''month'''', NOW()) - ''''1 month''''::interval as date)) AND sg_con_d4.sg_con4_busdate<= (select (date_trunc(''''month'''',current_date) - ''''1 month''''::interval + interval ''''1 month'''' - interval ''''1 day'''')::date))''

		  )a;';
		!!!RESOLVE EWI!!! /*** SSC-EWI-0030 - THE STATEMENT BELOW HAS USAGES OF DYNAMIC SQL. ***/!!!!!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'INSERT WITH EXECUTE' NODE ***/!!!

INSERT INTO T_CMS_temp EXECUTE IMMEDIATE :TSQL;

		------To Filter out the BI data for specific date range
		IF (OBJECT_ID_UDF('TEMPDB.DBO.T_BI_TEMP') IS NOT NULL) THEN
			DROP TABLE IF EXISTS T_BI_temp;
		END IF

;
		CREATE OR REPLACE TEMPORARY TABLE dbo.T_BI_temp AS
			WITH dataset as

(

---AN Contract List

select --count(*) 35596
												sg_con_d4.sg_con4_dealer,
												sg_con_d4.sg_con4_plc,
												sg_con_d4.sg_con4_busdate,
												sg_con_d4.sg_con4_rectype,
												sg_con_m1.sg_con_cover

	  ,
												sg_con_m1.sg_con_plc,
												sg_con_m1.sg_con_contract,
												sg_con_m1.sg_con_term,
												sg_con_m1.sg_con_form,
												sg_con_d4.sg_con4_seq,
												sg_con4_agent_0,
												sg_con4_amt_0

	  ,
												sg_con4_agent_4,
												sg_con4_amt_4,
												sg_con4_agent_6,
												sg_con4_amt_6,
												sg_con4_agent_7,
												sg_con4_amt_7,
												sg_con4_agent_8,
												sg_con4_amt_8,
												sg_con4_agent_9,
												sg_con4_amt_9,
												sg_con4_agent_10,
												sg_con4_amt_10

         ,
												sg_con4_agent_11,
												sg_con4_amt_11,
												sg_con4_agent_12,
												sg_con4_amt_12,
												sg_con4_agent_13,
												sg_con4_amt_13,
												sg_con4_agent_14,
												sg_con4_amt_14,
												sg_con4_agent_15,
												sg_con4_amt_15

         ,
												sg_con4_agent_16,
												sg_con4_amt_16,
												sg_con4_agent_17,
												sg_con4_amt_17,
												sg_con4_agent_18,
												sg_con4_amt_18

		 ,
												sg_con4_agent_19,
												sg_con4_amt_19,
												sg_con4_agent_20,
												sg_con4_amt_20

from
												DW_HIST.dbo.sg_con_d4 sg_con_d4

INNER JOIN
													DW_HIST.dbo.sg_con_m1 sg_con_m1
													ON sg_con_d4.sg_con4_contract = sg_con_m1.sg_con_contract
													AND sg_con_m1.current_flag ='y' AND sg_con_d4.current_flag ='y' AND sg_con_d4.sg_con4_posted ='y'
INNER JOIN
													DW_HIST.dbo.sg_dlr_m1 sg_dlr_m1
													ON sg_con_d4.sg_con4_dealer = sg_dlr_m1.sg_dlr_dealer
													AND sg_dlr_m1.current_flag ='y' and sg_dlr_m1.sg_dlr_agent ='000001'
LEFT OUTER JOIN
													DW_HIST.dbo.sg_grp_d1 as sg_grp_d1
													ON sg_grp_d1.sg_grp1_dealer = sg_con_d4.sg_con4_dealer
													AND sg_grp_d1.current_flag ='y' and sg_grp_d1.sg_grp1_group ='ANDIST'
LEFT OUTER JOIN (select
								sg_dgrp1_dealer,
								sg_dgrp_desc
							from

                                  (

                                         Select
                                         	sg_dgrp_desc,
                                         	sg_dgrp_d1.sg_dgrp1_dealer,
                                         	ROW_NUMBER() over (Partition By
                                         		sg_dgrp_d1.sg_dgrp1_dealer
                                         	Order by sg_dgrp_d1.sg_dgrp1_code Desc ) "rownum"

                                         FROM
                                         	DW_HIST.dbo.sg_dgrp_d1 sg_dgrp_d1

                                         inner join
                                         		DW_HIST.dbo.sg_dgrp_m1 sg_dgrp_m1

                                         on LTRIM(RTRIM(sg_dgrp_d1.sg_dgrp1_code))= LTRIM(RTRIM(sg_dgrp_m1.sg_dgrp_code)) and sg_dgrp_m1.current_flag ='Y' and sg_dgrp_m1.sg_dgrp_rm ='Y'

                                         where
                                         	sg_dgrp_d1.current_flag ='Y' and sg_dgrp_d1.sg_dgrp1_code like 'R%'



                                  ) RecentDlrGroup

                                  where
								rownum =1

				) dlr_RM
													on dlr_RM.sg_dgrp1_dealer = sg_con_d4.sg_con4_dealer

where
												sg_con_d4.sg_con4_busdate between :STARTDATE and :ENDDATE

UNION

------HYU, BEN, POR Bucket

SELECT --- count(*) 40
												map.an_dlr sg_con4_dealer,
												sg_con_d4.sg_con4_plc,
												sg_con_d4.sg_con4_busdate,
												sg_con_d4.sg_con4_rectype,
												sg_con_m1.sg_con_cover

	  ,
												sg_con_m1.sg_con_plc,
												sg_con_m1.sg_con_contract,
												sg_con_m1.sg_con_term,
												sg_con_m1.sg_con_form,
												sg_con_d4.sg_con4_seq,
												sg_con4_agent_0,
												sg_con4_amt_0

	  ,
												sg_con4_agent_4,
												sg_con4_amt_4,
												sg_con4_agent_6,
												sg_con4_amt_6,
												sg_con4_agent_7,
												sg_con4_amt_7,
												sg_con4_agent_8,
												sg_con4_amt_8,
												sg_con4_agent_9,
												sg_con4_amt_9,
												sg_con4_agent_10,
												sg_con4_amt_10

         ,
												sg_con4_agent_11,
												sg_con4_amt_11,
												sg_con4_agent_12,
												sg_con4_amt_12,
												sg_con4_agent_13,
												sg_con4_amt_13,
												sg_con4_agent_14,
												sg_con4_amt_14,
												sg_con4_agent_15,
												sg_con4_amt_15

         ,
												sg_con4_agent_16,
												sg_con4_amt_16,
												sg_con4_agent_17,
												sg_con4_amt_17,
												sg_con4_agent_18,
												sg_con4_amt_18

		  ,
												sg_con4_agent_19,
												sg_con4_amt_19,
												sg_con4_agent_20,
												sg_con4_amt_20

FROM
												DW_HIST.dbo.sg_con_d4 as sg_con_d4

INNER JOIN
													DW_HIST.dbo.sg_con_m1 sg_con_m1
													ON sg_con_d4.sg_con4_contract = sg_con_m1.sg_con_contract
													AND sg_con_m1.current_flag ='y' AND sg_con_d4.current_flag ='y' AND sg_con_d4.sg_con4_posted ='y'
INNER JOIN
													DW_HIST.dbo.sg_dlr_m1 sg_dlr_m1
													ON sg_con_d4.sg_con4_dealer = sg_dlr_m1.sg_dlr_dealer
													AND sg_dlr_m1.current_flag ='y'
INNER JOIN
													DW_HIST.dbo.an_dealer_mapping map
													ON sg_con_d4.sg_con4_dealer = map.con4_dlr
													and map.current_flag ='Y'
LEFT OUTER JOIN
													DW_HIST.dbo.sg_grp_d1 as sg_grp_d1
													ON sg_grp_d1.sg_grp1_dealer = map.an_dlr
													AND sg_grp_d1.current_flag ='y' and sg_grp_d1.sg_grp1_group ='ANDIST'
LEFT OUTER JOIN (select
								sg_dgrp1_dealer,
								sg_dgrp_desc
							from

                                  (

                                         Select
                                         	sg_dgrp_desc,
                                         	sg_dgrp_d1.sg_dgrp1_dealer,
                                         	ROW_NUMBER() over (Partition By
                                         		sg_dgrp_d1.sg_dgrp1_dealer
                                         	Order by sg_dgrp_d1.sg_dgrp1_code Desc ) "rownum"

                                         FROM
                                         	DW_HIST.dbo.sg_dgrp_d1 sg_dgrp_d1

                                         inner join
                                         		DW_HIST.dbo.sg_dgrp_m1 sg_dgrp_m1

                                         on LTRIM(RTRIM(sg_dgrp_d1.sg_dgrp1_code))= LTRIM(RTRIM(sg_dgrp_m1.sg_dgrp_code)) and sg_dgrp_m1.current_flag ='Y' and sg_dgrp_m1.sg_dgrp_rm ='Y'

                                         where
                                         	sg_dgrp_d1.current_flag ='Y' and sg_dgrp_d1.sg_dgrp1_code like 'R%'



                                  ) RecentDlrGroup

                                  where
								rownum =1

				) dlr_RM
													on dlr_RM.sg_dgrp1_dealer = map.an_dlr

WHERE
												sg_con_m1.sg_con_plc in ('HYVS','HYCP','HYTC','PPVS','POMF','POMO','POMS','POLS','POVS','BELS','BEMD','BEPM','VWVS','VWCP','VWGP','VWLS','AUVS','AUCP','AUGP','AULS','QPVS','AUTV','QSVS','BEMS','BEGP','HYPM','GEVS','GETC','AUCE','JPVS','JPCP','LPVS','LPCP','MILS','BMLS','BMGP','BMPL','MIPL','MIGP','HYBW','BEKY','BETW','POKY','POTW','BEWS','BEDD','HYSW','BEPL','BMMC','MIMC','BMMD','VWTS','AUTS','GESW','GEBW','HYVO')		--added AUCE on 20180604
AND sg_con_d4.sg_con4_busdate between :STARTDATE and :ENDDATE

)
			SELECT
				*
			from
				dataset;

		---Stencil Data
		IF (OBJECT_ID_UDF('TEMPDB.DBO.T_BI_STENCIL') IS NOT NULL) THEN
			DROP TABLE IF EXISTS T_BI_stencil;
		END IF;
		CREATE OR REPLACE TEMPORARY TABLE dbo.T_BI_stencil AS
			SELECT
				Store,
				Level2,
				Level1,
				DataValue,
				Region,
				MktShortDesc,
				MktLongDesc,
				StoreName,
				RegionalManager,
				sort,
				DINVPDOF_DATE,
				"GL Post Date",
				DOCDATE

FROM
				DW_SAFEGUARD.dbo.Stencil_data

where
				Store <>'' and DOCDATE between :STARTDATE and :ENDDATE
				and sort not in (2000);

----select count(*) from #BI_stencil





----Audit on CMS(SOURCE:POSTGRES) VS BI(SOURCE:DW_SAFEGUARD) Data in AN Management Fee Report
select ---count(*)
			NVL(COUNT(distinct CT.sg_con4_dealer),'0'),
			NVL(COUNT(distinct BT.sg_con4_dealer),'0'),
			CASE WHEN COUNT(distinct CT.sg_con4_dealer)= COUNT(distinct BT.sg_con4_dealer) then 'MATCHED' ELSE 'MISMATCHED' end,
			NVL(COUNT(distinct CT.sg_con_contract),'0'),
			NVL(COUNT(distinct BT.sg_con_contract),'0'),
			CASE WHEN COUNT(distinct CT.sg_con_contract)= COUNT(distinct BT.sg_con_contract) then 'MATCHED' ELSE 'MISMATCHED' end,
			NVL(COUNT(distinct CT.sg_con4_plc),'0'),
			NVL(COUNT(distinct BT.sg_con4_plc),'0'),
			CASE WHEN COUNT(distinct CT.sg_con4_plc)= COUNT(distinct BT.sg_con4_plc) then 'MATCHED' ELSE 'MISMATCHED' end,
			NVL(SUM(CT.sg_con4_amt_0 + CT.sg_con4_amt_4 + CT.sg_con4_amt_6 + CT.sg_con4_amt_7),'0.00'),
			NVL(SUM(BT.sg_con4_amt_0 + BT.sg_con4_amt_4 + BT.sg_con4_amt_6 + BT.sg_con4_amt_7),'0.00'),
			CASE WHEN NVL(SUM(CT.sg_con4_amt_0 + CT.sg_con4_amt_4 + CT.sg_con4_amt_6 + CT.sg_con4_amt_7),'0.00')= NVL(SUM(BT.sg_con4_amt_0 + BT.sg_con4_amt_4 + BT.sg_con4_amt_6 + BT.sg_con4_amt_7),'0.00')

												  then 'MATCHED' ELSE 'MISMATCHED' end
INTO
			:CMSDLRCOUNT,
			:BIDLRCOUNT,
			:DLRCNTSTS,
			:CMSCONCOUNT,
			:BICONCOUNT,
			:CONCNTSTS,
			:CMSPRODUCT,
			:BIPRODUCT,
			:PRODCTCNTSTS,
			:CMSAMOUNT,
			:BIAMOUNT,
			:AMTSTS
												from
			T_CMS_temp CT

												LEFT JOIN
				T_BI_temp BT
				ON LTRIM(RTRIM(BT.sg_con4_dealer))= LTRIM(RTRIM(CT.sg_con4_dealer)) and CT.sg_con_contract = BT.sg_con_contract
				and CT.sg_con4_seq = BT.sg_con4_seq;

---Audit on Stencil Data (SOURCE:Great Planes) VS BI(SOURCE:DW_SAFEGUARD) in AN Management Fee Report 

 ---Audit on GP Data
GPSTENCILDLR := (select
				NVL(COUNT(distinct Store),'0') from
				DW_SAFEGUARD.dbo.Stencil_data_audit
			where
				Level2 ='Stencils');
GPSTENCILSAMT := (select
				NVL(SUM(Datavalue),'0') from
				DW_SAFEGUARD.dbo.Stencil_data_audit
			where
				Level2 ='Stencils');
GPSAFEPARTSDLR := (select
				NVL(COUNT(distinct Store),'0') from
				DW_SAFEGUARD.dbo.Stencil_data_audit
			where
				Level2 ='Safe Parts');
GPSAFEPARTSAMT := (select
				NVL(SUM(Datavalue),'0.00') from
				DW_SAFEGUARD.dbo.Stencil_data_audit
			where
				Level2 ='Safe Parts');
GPTOTALMGMTFEE := (select
				NVL(SUM(Datavalue),'0.00') from
				DW_SAFEGUARD.dbo.Stencil_data_audit
			where
				Level1 ='Total Management Fee');
----Audit On BI Data
BISTENCILDLR := (select
				NVL(COUNT(distinct Store),'0') from
				T_BI_stencil
			where
				Level2 ='Stencils');
BISTENCILSAMT := (select
				NVL(SUM(Datavalue),'0.00') from
				T_BI_stencil
			where
				Level2 ='Stencils');
BISAFEPARTSDLR := (select
				NVL(COUNT(distinct Store),'0') from
				T_BI_stencil
			where
				Level2 ='Safe Parts');
BISAFEPARTSAMT := (select
				NVL(SUM(Datavalue),'0.00') from
				T_BI_stencil
			where
				Level2 ='Safe Parts');
BITOTALMGMTFEE := (select
				NVL(SUM(Datavalue),'0.00') from
				T_BI_stencil
			where
				Level1 ='Total Management Fee');
---Audit Status
STNCLDLRSTS := (select case when :GPSTENCILDLR = :BISTENCILDLR
													then 'MATCHED' ELSE 'MISMATCHED' end);
STNCLAMTSTS := (select case when :GPSTENCILSAMT = :BISTENCILSAMT
													then 'MATCHED' ELSE 'MISMATCHED' end);
SFPTDLRSTS := (select case when :GPSAFEPARTSDLR = :BISAFEPARTSDLR
													then 'MATCHED' ELSE 'MISMATCHED' end);
SFPTAMTSTS := (select case when :GPSAFEPARTSAMT = :BISAFEPARTSAMT
													then 'MATCHED' ELSE 'MISMATCHED' end);
TMGMFSTS := (select case when :GPTOTALMGMTFEE = :BITOTALMGMTFEE
													then 'MATCHED' ELSE 'MISMATCHED' end);
--DECLARE @BodyMain varchar(max)
BODYMAIN := '<html>

<style type="text/css">

.ap{background-color:Transparent;border:1pt none Black;}.a395xB{border:1pt none Black;background-color:Transparent;}.a5{word-wrap:break-word;white-space:pre-wrap;padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt none Black;background-color:Transparent;font-style:normal;font-family:Tahoma;font-size:16pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:#4c68a2;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}.a173{border:1pt none Black;background-color:Transparent;}

.a24c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:#4c68a2;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 403.23mm;WIDTH:403.23mm;}

.a24{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:14pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:White;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a40c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Silver;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 100.81mm;WIDTH:100.81mm;}

.a40{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:10pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a46c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Gray;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 100.81mm;WIDTH:100.81mm;}

.a46{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:10pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:White;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a52c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Silver;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 100.81mm;WIDTH:100.81mm;}

.a52{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:10pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a58c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Gray;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 100.81mm;WIDTH:100.81mm;}

.a58{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:10pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:White;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a65c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:LightSteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a65{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a69c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:LightSteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a69{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a73c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:LightSteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a73{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a77c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:SteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a77{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:White;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a81c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:SteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a81{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:White;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}';
BODYMAIN := :BODYMAIN || '.a85c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:SteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a85{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:White;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a89c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:LightSteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a89{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a93c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:LightSteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a93{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a97c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:LightSteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a97{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a101c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:SteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a101{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:White;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a105c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:SteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a105{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:White;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a109c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:SteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a109{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:White;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a114c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a114{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a118c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a118{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a122c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a122{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a126c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a126{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a130c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a130{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a134c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a134{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a138c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a138{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a142c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a142{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a146c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a146{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a150c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a150{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a154c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a154{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a158c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a158{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a393{border:1pt none Black;background-color:Transparent;}.a195c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:#4c68a2;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 504.03mm;WIDTH:504.03mm;}

.a195{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:14pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:White;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a215c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Silver;vertical-align:middle;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 100.81mm;WIDTH:100.81mm;}

.a215{word-wrap:break-word;white-space:pre-wrap;font-size:0pt;unicode-bidi:normal;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;}.a214{text-align:center;}.a212{font-family:Tahoma;font-size:9pt;font-weight:700;font-style:normal;text-decoration:none;color:Black;}

.a213{font-family:Tahoma;font-size:10pt;font-weight:700;font-style:normal;text-decoration:none;color:Black;}.a221c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Gray;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 100.81mm;WIDTH:100.81mm;}

.a221{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:White;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a228c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Silver;vertical-align:middle;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 100.81mm;WIDTH:100.81mm;}

.a228{word-wrap:break-word;white-space:pre-wrap;font-size:0pt;unicode-bidi:normal;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;}.a227{text-align:center;}

.a225{font-family:Tahoma;font-size:9pt;font-weight:700;font-style:normal;text-decoration:none;color:Black;}.a226{font-family:Tahoma;font-size:10pt;font-weight:700;font-style:normal;text-decoration:none;color:Black;}

.a236c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Gray;vertical-align:middle;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 100.81mm;WIDTH:100.81mm;}

.a236{word-wrap:break-word;white-space:pre-wrap;font-size:0pt;unicode-bidi:normal;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;}.a235{text-align:center;}.a232{font-family:Tahoma;font-size:9pt;font-weight:700;font-style:normal;text-decoration:none;color:White;}.a233{font-family:Tahoma;font-size:9pt;font-weight:700;font-style:normal;text-decoration:none;color:Black;}.a234{font-family:Tahoma;font-size:9pt;font-weight:700;font-style:normal;text-decoration:none;color:White;}

.a245c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Silver;vertical-align:middle;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 100.81mm;WIDTH:100.81mm;}

.a245{word-wrap:break-word;white-space:pre-wrap;font-size:0pt;unicode-bidi:normal;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;}.a244{text-align:center;}.a240{font-family:Tahoma;font-size:10pt;font-weight:700;font-style:normal;text-decoration:none;color:Black;}.a241{font-family:Tahoma;font-size:10pt;font-weight:700;font-style:normal;text-decoration:none;color:White;}

.a242{font-family:Tahoma;font-size:9pt;font-weight:700;font-style:normal;text-decoration:none;color:Black;}.a243{font-family:Tahoma;font-size:10pt;font-weight:700;font-style:normal;text-decoration:none;color:White;}

.a252c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:LightSteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a252{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a258c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:LightSteelBlue;vertical-align:middle;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a258{word-wrap:break-word;white-space:pre-wrap;font-size:0pt;unicode-bidi:normal;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;}.a255{text-align:center;}

.a254{font-family:Tahoma;font-size:9pt;font-weight:700;font-style:normal;text-decoration:none;color:Black;}.a257{text-align:center;}.a256{font-family:Tahoma;font-size:9pt;font-weight:700;font-style:normal;text-decoration:none;color:Black;}

.a262c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:LightSteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a262{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a266c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:SteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a266{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:White;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a270c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:SteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a270{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:White;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a274c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:SteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a274{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:White;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a278c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:LightSteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a278{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a282c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:LightSteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a282{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a286c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:LightSteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a286{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a290c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:SteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a290{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:White;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a294c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:SteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a294{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:White;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a298c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:SteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a298{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:White;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a304c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:LightSteelBlue;vertical-align:middle;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a304{word-wrap:break-word;white-space:pre-wrap;font-size:0pt;unicode-bidi:normal;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;}';
BODYMAIN := :BODYMAIN || '.a301{text-align:center;}.a300{font-family:Tahoma;font-size:9pt;font-weight:700;font-style:normal;text-decoration:none;color:Black;}.a303{text-align:center;}.a302{font-family:Tahoma;font-size:9pt;font-weight:700;font-style:normal;text-decoration:none;color:Black;}.a310c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:LightSteelBlue;vertical-align:middle;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a310{word-wrap:break-word;white-space:pre-wrap;font-size:0pt;unicode-bidi:normal;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;}.a307{text-align:center;}.a306{font-family:Tahoma;font-size:9pt;font-weight:700;font-style:normal;text-decoration:none;color:Black;}.a309{text-align:center;}.a308{font-family:Tahoma;font-size:9pt;font-weight:700;font-style:normal;text-decoration:none;color:Black;}.a314c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:LightSteelBlue;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a314{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:9pt;font-weight:700;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a319c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a319{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a323c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a323{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a327c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a327{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a331c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a331{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a335c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a335{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a339c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a339{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a343c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a343{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a347c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a347{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a351c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a351{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a355c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a355{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a359c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a359{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a363c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a363{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a367c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a367{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a371c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a371{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}

.a375c{padding-left:2pt;padding-top:2pt;padding-right:2pt;padding-bottom:2pt;border:1pt solid Black;background-color:Transparent;vertical-align:middle;text-align:center;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;min-width: 33.60mm;WIDTH:33.60mm;}

.a375{word-wrap:break-word;white-space:pre-wrap;font-style:normal;font-family:Tahoma;font-size:8pt;font-weight:400;text-decoration:none;unicode-bidi:normal;color:Black;direction:ltr;layout-flow:horizontal;writing-mode:lr-tb;vertical-align:middle;text-align:center;}.r1{HEIGHT:100%;WIDTH:100%}.r2{HEIGHT:100%;WIDTH:100%;overflow:hidden}.r3{HEIGHT:100%}.r4{border-style:none}.r5{border-left-style:none}.r6{border-right-style:none}.r7{border-top-style:none}.r8{border-bottom-style:none}.r10{border-collapse:collapse}.r9{border-collapse:collapse;table-layout:fixed}.r11{WIDTH:100%;overflow-x:hidden}.r12{position:absolute;display:none;background-color:white;border:1px solid black;}.r13{

text-decoration:none;color:black;cursor:pointer;}.r14{font-size:0pt}.r15{direction:RTL;unicode-bidi:embed}.r16{margin-top:0pt;margin-bottom:0pt}.r17{HEIGHT:100%;WIDTH:100%;display:inline-table}.r18{HEIGHT:100%;display:inline-table} * { box-sizing: border-box }</style>';
BODYMAIN := :BODYMAIN || '<body style="BORDER: 0px; MARGIN: 0px; PADDING: 0px"><DIV dir="LTR" ID="oReportDiv"><DIV style="WIDTH:100%;" class="ap"><TABLE CELLSPACING="0" CELLPADDING="0"><TR><TD ID="oReportCell"><TABLE CELLSPACING="0" CELLPADDING="0" BORDER="0" class="a395xB"><TR><TD style="vertical-align:top"><TABLE CELLSPACING="0" CELLPADDING="0" BORDER="0" COLS="3" LANG="en-US" class="r10" style="WIDTH:504.8263mm"><TR VALIGN="top"><TD style="HEIGHT:15.88mm;WIDTH:403.23mm;min-width: 403.23mm;"><TABLE CELLSPACING="0" CELLPADDING="0" LANG="en-US" style=""><TR><TD style="WIDTH:403.23mm;min-width: 403.23mm;HEIGHT:15.88mm;" class="a5">MONTH END AUDIT ON &#39;&#39;AN MANAGEMENT FEE REPORT WITH STENCIL LOGIC&#39;&#39; AS OF ' || :MONTHYEAR || '</TD></TR></TABLE></TD><TD style="WIDTH:100.81mm;min-width: 100.81mm;HEIGHT:15.88mm;"></TD><TD style="WIDTH:0.79mm;min-width: 0.79mm;HEIGHT:15.88mm;"></TD></TR><TR VALIGN="top"><TD style="WIDTH:403.23mm;min-width: 403.23mm;"><TABLE CELLSPACING="0" CELLPADDING="0" COLS="12" BORDER="0" style="border-collapse:collapse;WIDTH:403.23mm;min-width: 403.23mm;" class="a173"><TR HEIGHT="0"><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD></TR><TR VALIGN="top"><TD COLSPAN="12" style="HEIGHT:10.32mm;" class="a24c"><DIV class="a24">AUDIT ON CMS (SOURCE:POSTGRES) VS BI (SOURCE:DW_SAFEGUARD) DATA IN AN MANAGEMENT FEE REPORT</DIV></TD></TR><TR VALIGN="top"><TD COLSPAN="3" style="HEIGHT:9.79mm;" class="a40c"><DIV class="a40">DEALER AUDIT</DIV></TD><TD COLSPAN="3" class="a46c"><DIV class="a46">CONTRACT AUDIT</DIV></TD><TD COLSPAN="3" class="a52c"><DIV class="a52">PRODUCT AUDIT</DIV></TD><TD COLSPAN="3" class="a58c"><DIV class="a58">TOTAL AMOUNT</DIV></TD></TR><TR VALIGN="top"><TD style="HEIGHT:10.32mm;" class="a65c"><DIV class="a65">CMS DEALER COUNT</DIV></TD><TD class="a69c"><DIV class="a69">BI DEALER COUNT</DIV></TD><TD class="a73c"><DIV class="a73">DEALER AUDIT STATUS</DIV></TD><TD class="a77c"><DIV class="a77">CMS CONTRACT COUNT</DIV></TD><TD class="a81c"><DIV class="a81">BI CONTRACT COUNT</DIV></TD><TD class="a85c"><DIV class="a85">CONTRACT AUDIT STATUS</DIV></TD><TD class="a89c"><DIV class="a89">CMS PRODUCT COUNT</DIV></TD><TD class="a93c"><DIV class="a93">BI PRODUCT COUNT</DIV></TD><TD class="a97c"><DIV class="a97">PRODUCT AUDIT STATUS</DIV></TD><TD class="a101c"><DIV class="a101">CMS TOTAL AMOUNT</DIV></TD><TD class="a105c"><DIV class="a105">BI TOTAL AMOUNT</DIV></TD><TD class="a109c"><DIV class="a109">AUDIT STATUS ON TOTAL AMOUNT</DIV></TD></TR><TR VALIGN="top"><TD style="HEIGHT:6.35mm;" class="a114c"><DIV class="a114">' || :CMSDLRCOUNT || '</DIV></TD><TD class="a118c"><DIV class="a118">' || :BIDLRCOUNT || '</DIV></TD><TD class="a122c"><DIV class="a122">' || :DLRCNTSTS || '</DIV></TD><TD class="a126c"><DIV class="a126">' || :CMSCONCOUNT || '</DIV></TD><TD class="a130c"><DIV class="a130">' || :BICONCOUNT || '</DIV></TD><TD class="a134c"><DIV class="a134">' || :CONCNTSTS || '</DIV></TD><TD class="a138c"><DIV class="a138">' || :CMSPRODUCT || '</DIV></TD><TD class="a142c"><DIV class="a142">' || :BIPRODUCT || '</DIV></TD><TD class="a146c"><DIV class="a146">' || :PRODCTCNTSTS || '</DIV></TD><TD class="a150c"><DIV class="a150">' || :CMSAMOUNT || '</DIV></TD><TD class="a154c"><DIV class="a154">' || :BIAMOUNT || '</DIV></TD><TD class="a158c"><DIV class="a158">' || :AMTSTS || '</DIV></TD></TR></TABLE></TD><TD ROWSPAN="2" COLSPAN="2" style="WIDTH:101.60mm;min-width: 101.60mm;HEIGHT:36.78mm;"></TD></TR><TR><TD style="HEIGHT:8.07mm;WIDTH:403.23mm;min-width: 403.23mm;HEIGHT:8.07mm;"></TD></TR><TR VALIGN="top"><TD COLSPAN="2" style="WIDTH:504.03mm;min-width: 504.03mm;"><TABLE CELLSPACING="0" CELLPADDING="0" COLS="15" BORDER="0" style="border-collapse:collapse;WIDTH:504.03mm;min-width: 504.03mm;" class="a393"><TR HEIGHT="0"><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD><TD style="WIDTH:33.60mm;min-width: 33.60mm"></TD></TR><TR VALIGN="top"><TD COLSPAN="15" style="HEIGHT:10.32mm;" class="a195c"><DIV class="a195">AUDIT ON STENCIL DATA (SOURCE:GREAT PLANES) VS BI (SOURCE:DW_SAFEGUARD) IN AN MANAGEMENT FEE REPORT</DIV></TD></TR><TR VALIGN="top"><TD COLSPAN="3" style="HEIGHT:9.79mm;" class="a215c"><DIV class="a215"><DIV style="overflow-x:hidden;WIDTH:98.69mm;"><DIV class="a214"><span class="a212">STENCIL </span><span class="a213">DEALER AUDIT</span></DIV></DIV></DIV></TD><TD COLSPAN="3" class="a221c"><DIV class="a221">STENCIL AMOUNT AUDIT</DIV></TD><TD COLSPAN="3" class="a228c"><DIV class="a228"><DIV style="overflow-x:hidden;WIDTH:98.69mm;"><DIV class="a227"><span class="a225">SAFEPARTS </span><span class="a226">DEALER AUDIT</span></DIV></DIV></DIV></TD><TD COLSPAN="3" class="a236c"><DIV class="a236"><DIV style="overflow-x:hidden;WIDTH:98.69mm;"><DIV class="a235"><span class="a232">SAFEPARTS</span><span class="a233"> </span><span class="a234">AMOUNT AUDIT</span></DIV></DIV></DIV></TD><TD COLSPAN="3" class="a245c"><DIV class="a245"><DIV style="overflow-x:hidden;WIDTH:98.69mm;"><DIV class="a244"><span class="a240">TOTAL</span><span class="a241"> </span><span class="a242">MGMT FEE AUDIT </span><span class="a243"> </span></DIV></DIV></DIV></TD></TR><TR VALIGN="top"><TD style="HEIGHT:13.23mm;" class="a252c"><DIV class="a252">CMS STENCIL DEALER COUNT</DIV></TD><TD class="a258c"><DIV class="a258"><DIV style="overflow-x:hidden;WIDTH:31.49mm;"><DIV class="a255"><span class="a254">BI STENCIL </span></DIV><DIV class="a257"><span class="a256">DEALER COUNT</span></DIV></DIV></DIV></TD><TD class="a262c"><DIV class="a262">STENCIL DEALER AUDIT STATUS</DIV></TD><TD class="a266c"><DIV class="a266">CMS STENCIL TOTAL AMOUNT</DIV></TD><TD class="a270c"><DIV class="a270">BI STENCIL TOTAL AMOUNT</DIV></TD><TD class="a274c"><DIV class="a274">STENCIL AMOUNT AUDIT STATUS</DIV></TD><TD class="a278c"><DIV class="a278">CMS SAFEPARTS DEALER COUNT</DIV></TD><TD class="a282c">';
BODYMAIN := :BODYMAIN || '<DIV class="a282">BI SAFEPARTS DEALER COUNT</DIV></TD><TD class="a286c"><DIV class="a286">SAFEPARTS DEALER AUDIT STATUS</DIV></TD><TD class="a290c"><DIV class="a290">CMS SAFEPARTS TOTAL AMOUNT</DIV></TD><TD class="a294c"><DIV class="a294">BI SAFEPARTS TOTAL AMOUNT</DIV></TD><TD class="a298c"><DIV class="a298">SAFEPARTS AMOUNT AUDIT STATUS</DIV></TD><TD class="a304c"><DIV class="a304"><DIV style="overflow-x:hidden;WIDTH:31.49mm;"><DIV class="a301"><span class="a300">CMS TOTAL </span></DIV><DIV class="a303"><span class="a302">MGMT FEE</span></DIV></DIV></DIV></TD><TD class="a310c"><DIV class="a310"><DIV style="overflow-x:hidden;WIDTH:31.49mm;"><DIV class="a307"><span class="a306">BI TOTAL </span></DIV><DIV class="a309"><span class="a308">MGMT FEE</span></DIV></DIV></DIV></TD><TD class="a314c"><DIV class="a314">MGMT FEE AUDIT STATUS</DIV></TD></TR><TR VALIGN="top"><TD style="HEIGHT:6.35mm;" class="a319c"><DIV class="a319">' || :GPSTENCILDLR || '</DIV></TD><TD class="a323c"><DIV class="a323">' || :BISTENCILDLR || '</DIV></TD><TD class="a327c"><DIV class="a327">' || :STNCLDLRSTS || '</DIV></TD><TD class="a331c"><DIV class="a331">' || :GPSTENCILSAMT || '</DIV></TD><TD class="a335c"><DIV class="a335">' || :BISTENCILSAMT || '</DIV></TD><TD class="a339c"><DIV class="a339">' || :STNCLAMTSTS || '</DIV></TD><TD class="a343c"><DIV class="a343">' || :GPSAFEPARTSDLR || '</DIV></TD><TD class="a347c"><DIV class="a347">' || :BISAFEPARTSDLR || '</DIV></TD><TD class="a351c"><DIV class="a351">' || :SFPTDLRSTS || '</DIV></TD><TD class="a355c"><DIV class="a355">' || :GPSAFEPARTSAMT || '</DIV></TD><TD class="a359c"><DIV class="a359">' || :BISAFEPARTSAMT || '</DIV></TD><TD class="a363c"><DIV class="a363">' || :SFPTAMTSTS || '</DIV></TD><TD class="a367c"><DIV class="a367">' || :GPTOTALMGMTFEE || '</DIV></TD><TD class="a371c"><DIV class="a371">' || :BITOTALMGMTFEE || '</DIV></TD><TD class="a375c"><DIV class="a375">' || :TMGMFSTS || '</DIV></TD></TR></TABLE></TD><TD style="WIDTH:0.79mm;min-width: 0.79mm;HEIGHT:39.69mm;"></TD></TR><TR><TD COLSPAN="3" style="WIDTH:504.83mm;min-width: 504.83mm;HEIGHT:1.06mm;"></TD></TR></TABLE></TD></TR></TABLE></TD><TD WIDTH="100%" HEIGHT="0"></TD></TR><TR><TD WIDTH="0" HEIGHT="100%"></TD></TR></TABLE></DIV></DIV></body></html>';
 
CALL DW_STG.dbo.usp_Send_Email('an_mgmt_report_audit', NULL, :BODYMAIN, :SUBJ);
	END;
$$;