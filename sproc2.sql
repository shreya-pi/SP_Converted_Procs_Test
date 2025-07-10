USE DATABASE DW_DATAACCESS;

--/****** Object:  StoredProcedure [EP].[URS_Update_Earned_Premium]    Script Date: 6/27/2025 12:37:17 PM ******/
----** SSC-FDM-TS0027 - SET ANSI_NULLS ON STATEMENT MAY HAVE A DIFFERENT BEHAVIOR IN SNOWFLAKE **
--SET ANSI_NULLS ON

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

SET QUOTED_IDENTIFIER ON;

--** SSC-FDM-0007 - MISSING DEPENDENT OBJECTS "dw_hist.dbo.sg_cov_d3", "DW_HIST.dbo.sg_con_m1", "DW_HIST.dbo.rm_vehclass", "DW_HIST.dbo.rm_LeaseCovType", "dbo.hgapearn", "dbo.hvscearn", "dbo.Prorata", "dbo.hcpoearn", "dbo.hppmearn", "dbo.htppearn", "dbo.hy_hylsearn", "dbo.hwrapearn", "dbo.hy_ppsm_plus_earn_gpp", "dbo.fdotearn", "dbo.tfs_earn_v2", "dbo.vci_VWVS", "dbo.vci_AUVS", "dbo.vci_DUVS", "dbo.vci_vwcp", "dbo.vci_aucp", "dbo.vci_gapearn", "dbo.vci_leaseearn", "dbo.vci_VWTV_earn", "dbo.vci_AUTV_plus", "dbo.vci_AUTV_plat", "dbo.gapearntermpartner", "dbo.twearnclassstateterm", "dbo.wsearn", "dbo.dentearn", "dbo.etchearn", "dbo.leaseearncov", "dbo.vscearn", "DW_HIST.dbo.sg_dlr_m1" **
CREATE OR REPLACE PROCEDURE EP.URS_Update_Earned_Premium (RUN_TIME TIMESTAMP_NTZ(3))
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 2,  "patch": "6.0" }, "attributes": {  "component": "transact",  "convertedOn": "06-27-2025",  "domain": "test" }}'
EXECUTE AS CALLER
AS
$$
	DECLARE
		LOADDATE TIMESTAMP_NTZ(3) := :RUN_TIME;
	BEGIN
		 
		CREATE OR REPLACE TEMPORARY TABLE EP.T_temp_URS_Risk_Type2 (
			Contract_Number VARCHAR(15),
			sg_con_rstype VARCHAR(50),
			vehicle_model_class VARCHAR(50),
			Segment VARCHAR(100));
insert INTO T_temp_URS_Risk_Type2

		SELECT
			EP.Contract_Number,CASE WHEN m1.sg_con_cover IN (SELECT
						sg_cov3_cover
					FROM
						dw_hist.dbo.sg_cov_d3
					WHERE
						sg_cov3_rstype = 'T' AND current_flag = 'Y')            THEN 'T'

     WHEN m1.sg_con_cover IN (SELECT
						sg_cov3_cover
					FROM
						dw_hist.dbo.sg_cov_d3
					WHERE
						sg_cov3_rstype = 'D' AND current_flag = 'Y')           THEN 'D'

     WHEN m1.sg_con_cover IN (SELECT
						sg_cov3_cover
					FROM
						dw_hist.dbo.sg_cov_d3
					WHERE
						sg_cov3_rstype = 'W' AND current_flag = 'Y')            THEN 'W'

     WHEN m1.sg_con_cover IN (SELECT
						sg_cov3_cover
					FROM
						dw_hist.dbo.sg_cov_d3
					WHERE
						sg_cov3_rstype = 'K' AND current_flag = 'Y')            THEN 'K'

WHEN m1.sg_con_cover IN (SELECT
						sg_cov3_cover
					FROM
						dw_hist.dbo.sg_cov_d3
					WHERE
						sg_cov3_rstype = 'R' AND current_flag = 'Y') or m1.sg_con_cover ='RS'             THEN 'R'

     WHEN m1.sg_con_rstype in ('T','P') THEN 'P'

     ELSE m1.sg_con_rstype
			END AS sg_con_rstype

			,CASE

     WHEN UPPER(m1.sg_con_series) = 'CHCAM' THEN '2-L'

     WHEN UPPER(m1.sg_con_series) IN ( 'FDGT', 'VIPER', 'DGVIP','NIGTR' )   THEN '4-HP'

     WHEN UPPER(m1.sg_con_series) IN ( 'CORVET', 'CORVTT', 'CHCVT','CHZR1' ) THEN '3-UL'

     WHEN NVL(LTRIM(RTRIM(UPPER(m1.sg_con_series))), 0) = '' OR rmv.vehmake IS NULL THEN 'N/A'

     ELSE UPPER(rmv.vehclass)

	 END  AS VehModelClass

			,
			segment

		FROM
			EP.URS_Monthly_Contract_Earnings AS EP

			INNER JOIN
     DW_HIST.dbo.sg_con_m1 AS m1
     ON EP.Contract_Number = m1.sg_con_contract

			LEFT JOIN
     DW_HIST.dbo.rm_vehclass AS rmv
     ON m1.sg_con_ncic = rmv.vehmake
     and rmv.current_flag ='Y'
LEFT JOIN (    SELECT
						UPPER(LTRIM(RTRIM(coverage))) as coverage,
						segment
					FROM
						DW_HIST.dbo.rm_LeaseCovType

					WHERE
						current_flag = 'Y'

           ) AS leaseCovType
     ON leaseCovType.coverage = UPPER(LTRIM(RTRIM(m1.sg_con_cover)))

WHERE
			m1.current_flag ='Y' and EP.Load_Date = :LOADDATE;
UPDATE [EP]."[URS_Monthly_Contract_Earnings]" EP
			SET
     EP.Earned_Percentage =CASE

  WHEN m1.sg_con_plc in ('HYGP','KIGP','GEGP','HYFB','GEAP','PPGP') and m1.sg_con_carrier in ('HCI', 'HCIA')

  THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'hgapearn' NODE ***/!!!
  dbo.hgapearn(DATEDIFF(day, m1.sg_con_saledate, EP.Booking_Date), m1.sg_con_term)



  WHEN m1.sg_con_plc in ('HYVS','HYVE','GEVS','HYVB') and m1.sg_con_carrier in ('HCI', 'HCIA') THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'hvscearn' NODE ***/!!!
  dbo.hvscearn(DATEDIFF(day, m1.sg_con_saledate, EP.Booking_Date), m1.sg_con_term, m1.sg_con_expmiles, m1.sg_con_odom)



  WHEN m1.sg_con_plc ='PPTC' and m1.sg_con_carrier in ('HCI', 'HCIA') THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'Prorata' NODE ***/!!!
  dbo.Prorata(datediff(day, m1.sg_con_saledate, EP.Booking_Date)

  , Datediff(day, m1.sg_con_saledate, m1.sg_con_expdate))



  WHEN m1.sg_con_plc IN  ('HYCO' ,'GECO') and m1.sg_con_carrier in ('HCI', 'HCIA') THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'hcpoearn' NODE ***/!!!
  dbo.hcpoearn(EP.Booking_Date, m1.sg_con_saledate, m1.sg_con_expdate, EP.Inservice_Date)



  WHEN m1.sg_con_plc IN ('HYPM', 'GEPM', 'GECM','HYIS','KISM','HYCM','HYPS') and m1.sg_con_carrier in ('HCI', 'HCIA')
			 and m1.sg_con_status not in('P','E')  THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'hppmearn' NODE ***/!!!
  dbo.hppmearn(DATEDIFF(day, m1.sg_con_saledate, EP.Booking_Date), m1.sg_con_term)



  WHEN m1.sg_con_plc in ('HYTC', 'GETC') and m1.sg_con_carrier in ('HCI', 'HCIA')
			and m1.sg_con_status not in('P','E') THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'htppearn' NODE ***/!!!
  dbo.htppearn(DATEDIFF(day, m1.sg_con_saledate, EP.Booking_Date), m1.sg_con_term)



  WHEN m1.sg_con_plc in ('HYLS', 'GELS') and m1.sg_con_carrier in ('HCI', 'HCIA')  THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'hy_hylsearn' NODE ***/!!!
  dbo.hy_hylsearn(DATEDIFF(day, m1.sg_con_saledate, EP.Booking_Date), m1.sg_con_term)



  WHEN m1.sg_con_plc IN ('HYCP','HYVO','GECP') and m1.sg_con_carrier in ('HCI', 'HCIA') THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'hwrapearn' NODE ***/!!!
  dbo.hwrapearn(EP.Booking_Date, m1.sg_con_saledate, m1.sg_con_expdate, EP.Inservice_Date)



  WHEN m1.sg_con_plc = 'GEPP' and m1.sg_con_carrier in ('HCI', 'HCIA') THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'hy_ppsm_plus_earn_gpp' NODE ***/!!!
  dbo.hy_ppsm_plus_earn_gpp(DATEDIFF(day, m1.sg_con_saledate, EP.Booking_Date), m1.sg_con_term)

  ---Hyundai end---

  WHEN m1.sg_con_plc ='FDOT' THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'fdotearn' NODE ***/!!!
  dbo.fdotearn(datediff(day, m1.sg_con_saledate, EP.Booking_Date)

  , Datediff(day, m1.sg_con_saledate, m1.sg_con_expdate))



  ----Toyota--

  WHEN m1.sg_con_plc IN ('TFSP','TFST','TFSK') THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'tfs_earn_v2' NODE ***/!!!
  dbo.tfs_earn_v2(EP.Booking_Date,'claim',

				CASE WHEN LEFT(m1.sg_con_cover,1) IN ('T','P') THEN  'TW'

					 WHEN LEFT(m1.sg_con_cover,1) IN ('D') THEN 'PDR'

					 WHEN LEFT(m1.sg_con_cover,1) IN ('X') THEN 'WS'

					 WHEN LEFT(m1.sg_con_cover,1) IN ('K') THEN 'KEY' END, DATEDIFF(day, m1.sg_con_saledate, EP.Booking_Date)

  , DATEDIFF(day, m1.sg_con_saledate, m1.sg_con_expdate)

  ,CASE WHEN m1.sg_con_saledate < '2017/11/01' AND LEFT(Veh.vehicle_model_class, 1) NOT IN ('N', 'N/A')

							THEN LEFT(Veh.vehicle_model_class, 1)

						ELSE CASE WHEN RIGHT(m1.sg_con_cover, 1) NOT IN ('1', '2', '3', '4', '5', '6', '7') THEN '1' ELSE RIGHT(m1.sg_con_cover, 1) END

						END, m1.sg_con_term, m1.sg_con_saledate)





 ----VCI earning functions--

 WHEN m1.sg_con_plc in ('VWVS') THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'vci_VWVS' NODE ***/!!!
  dbo.vci_VWVS(DATEDIFF(day, m1.sg_con_saledate, EP.Booking_Date), m1.sg_con_term, m1.sg_con_expmiles, m1.sg_con_odom)

	WHEN m1.sg_con_plc in ('AUVS') THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'vci_AUVS' NODE ***/!!!
  dbo.vci_AUVS(DATEDIFF(day, m1.sg_con_saledate, EP.Booking_Date), m1.sg_con_term, m1.sg_con_expmiles, m1.sg_con_odom)

	WHEN m1.sg_con_plc in ('DUVS')

		THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'vci_DUVS' NODE ***/!!!
  dbo.vci_DUVS(EP.Booking_Date, m1.sg_con_saledate, m1.sg_con_term, EP.Inservice_Date)

	WHEN m1.sg_con_plc in ('VWCP')

		THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'vci_vwcp' NODE ***/!!!
  dbo.vci_vwcp(EP.Booking_Date, m1.sg_con_saledate, m1.sg_con_term, m1.sg_con_expmiles, m1.sg_con_odom, EP.Inservice_Date)

	WHEN m1.sg_con_plc in ('AUCP')

		THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'vci_aucp' NODE ***/!!!
  dbo.vci_aucp(EP.Booking_Date, m1.sg_con_saledate, m1.sg_con_term, m1.sg_con_expmiles, m1.sg_con_odom, EP.Inservice_Date)

	WHEN m1.sg_con_plc in ('VWGP', 'AUGP', 'DUGP', 'VWPL', 'AUPL', 'DUPL')

		THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'vci_gapearn' NODE ***/!!!
  dbo.vci_gapearn(DATEDIFF(day, m1.sg_con_saledate, EP.Booking_Date), m1.sg_con_term)

	WHEN m1.sg_con_plc in ('VWLS', 'AULS')

		THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'vci_leaseearn' NODE ***/!!!
  dbo.vci_leaseearn(DATEDIFF(day, m1.sg_con_saledate, EP.Booking_Date), m1.sg_con_term)

	WHEN m1.sg_con_plc in ('VWTV')

		THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'vci_VWTV_earn' NODE ***/!!!
  dbo.vci_VWTV_earn(DATEDIFF(day, m1.sg_con_saledate, EP.Booking_Date), m1.sg_con_term)

	WHEN m1.sg_con_plc in ('AUTV') and m1.sg_con_cover in ('AUTVPLUS')

		THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'vci_AUTV_plus' NODE ***/!!!
  dbo.vci_AUTV_plus(DATEDIFF(day, m1.sg_con_saledate, EP.Booking_Date), m1.sg_con_term)

	WHEN m1.sg_con_plc in ('AUTV')

		THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'vci_AUTV_plat' NODE ***/!!!
  dbo.vci_AUTV_plat(DATEDIFF(day, m1.sg_con_saledate, EP.Booking_Date), m1.sg_con_term)



  WHEN Veh.sg_con_rstype ='G' THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'gapearntermpartner' NODE ***/!!!
  dbo.gapearntermpartner(DATEDIFF(day, m1.sg_con_saledate, EP.Booking_Date)

  , DATEDIFF(day, m1.sg_con_saledate, m1.sg_con_expdate)

  , m1.sg_con_term, sg_dlr_agent

  )



  WHEN Veh.sg_con_rstype ='T' THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'twearnclassstateterm' NODE ***/!!!
  dbo.twearnclassstateterm(DATEDIFF(day, m1.sg_con_saledate, EP.Booking_Date)

  , DATEDIFF(day, m1.sg_con_saledate, m1.sg_con_expdate)

  , veh.vehicle_model_class

  , Dlr.sg_dlr_state, m1.sg_con_term)



  WHEN Veh.sg_con_rstype ='W' THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'wsearn' NODE ***/!!!
  dbo.wsearn(datediff(day, m1.sg_con_saledate, EP.Booking_Date)

  , Datediff(day, m1.sg_con_saledate, m1.sg_con_expdate))



  WHEN Veh.sg_con_rstype ='D' THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'dentearn' NODE ***/!!!
  dbo.dentearn(datediff(day, m1.sg_con_saledate, EP.Booking_Date)

  , Datediff(day, m1.sg_con_saledate, m1.sg_con_expdate))



  WHEN Veh.sg_con_rstype ='E' OR m1.sg_con_plc ='LOJK' THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'etchearn' NODE ***/!!!
  dbo.etchearn(datediff(day, m1.sg_con_saledate, EP.Booking_Date)

  , Datediff(day, m1.sg_con_saledate, m1.sg_con_expdate))



  WHEN Veh.sg_con_rstype ='L' THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'leaseearncov' NODE ***/!!!
  dbo.leaseearncov(datediff(day, m1.sg_con_saledate, EP.Booking_Date)

  , Datediff(day, m1.sg_con_saledate, m1.sg_con_expdate)

  , veh.Segment)



  WHEN Veh.sg_con_rstype ='V' THEN
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'vscearn' NODE ***/!!!
  dbo.vscearn(datediff(day, m1.sg_con_saledate, EP.Booking_Date), m1.sg_con_term, m1.sg_con_nu)



  ELSE
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'Prorata' NODE ***/!!!
  dbo.Prorata(datediff(day, m1.sg_con_saledate, EP.Booking_Date)

  , Datediff(day, m1.sg_con_saledate, m1.sg_con_expdate))



 END,
     EP.Default_Curve_Flag =CASE

  WHEN m1.sg_con_plc in ('HYGP','KIGP','GEGP','HYFB','GEAP','PPGP') and m1.sg_con_carrier in ('HCI', 'HCIA')

  THEN 'N'

  WHEN m1.sg_con_plc ='HYIS' and m1.sg_con_carrier in ('HCI', 'HCIA') THEN 'N'

  WHEN m1.sg_con_plc in ('HYVS','HYVE','GEVS','HYVB') and m1.sg_con_carrier in ('HCI', 'HCIA') THEN 'N'

  WHEN m1.sg_con_plc ='PPTC' and m1.sg_con_carrier in ('HCI', 'HCIA') THEN 'Y'

  WHEN m1.sg_con_plc IN( 'HYCO','GECO') and m1.sg_con_carrier in ('HCI', 'HCIA') THEN 'N'

  WHEN m1.sg_con_plc IN ('HYPM', 'GEPM', 'GECM','HYIS','KISM','HYCM','HYPS') and m1.sg_con_carrier in ('HCI', 'HCIA')
			 and m1.sg_con_status not in('P','E')  THEN 'N'

  WHEN m1.sg_con_plc in ('HYTC', 'GETC') and m1.sg_con_carrier in ('HCI', 'HCIA')
			and m1.sg_con_status not in('P','E') THEN 'N'

  WHEN m1.sg_con_plc in ('HYLS', 'GELS') and m1.sg_con_carrier in ('HCI', 'HCIA')  THEN 'N'

  WHEN m1.sg_con_plc IN ('HYCP','HYVO','GECP') and m1.sg_con_carrier in ('HCI', 'HCIA') THEN 'N'

  WHEN m1.sg_con_plc = 'GEPP' and m1.sg_con_carrier in ('HCI', 'HCIA') THEN 'N'

	WHEN m1.sg_con_plc ='FDOT' THEN 'N'

	WHEN m1.sg_con_plc IN('TFSP','TFST','TFSK') THEN 'N'

	WHEN m1.sg_con_plc in ('VWVS') THEN 'N'

	WHEN m1.sg_con_plc in ('AUVS') THEN 'N'

	WHEN m1.sg_con_plc in ('DUVS') THEN	'N'

	WHEN m1.sg_con_plc in ('VWCP') THEN 'N'

	WHEN m1.sg_con_plc in ('AUCP') THEN 'N'



	WHEN m1.sg_con_plc in ('VWGP', 'AUGP', 'DUGP', 'VWPL', 'AUPL', 'DUPL') THEN 'N'

	WHEN m1.sg_con_plc in ('VWLS', 'AULS') THEN 'N'

	WHEN m1.sg_con_plc in ('VWTV') THEN 'N'

	WHEN m1.sg_con_plc in ('AUTV') and m1.sg_con_cover in ('AUTVPLUS') THEN 'N'

	WHEN m1.sg_con_plc in ('AUTV') THEN 'N'

	WHEN Veh.sg_con_rstype ='G' THEN 'N'

	WHEN Veh.sg_con_rstype ='T' THEN 'N'

	WHEN Veh.sg_con_rstype ='W' THEN 'N'

	WHEN Veh.sg_con_rstype ='D' THEN 'N'

	WHEN Veh.sg_con_rstype ='E' OR m1.sg_con_plc ='LOJK' THEN 'N'

	WHEN Veh.sg_con_rstype ='L' THEN 'N'

	WHEN Veh.sg_con_rstype ='V' THEN 'N'

	ELSE 'Y'

	  END
			FROM
     DW_HIST.dbo.sg_con_m1 As m1,
     T_temp_URS_Risk_Type2 AS Veh,
     DW_HIST.dbo.sg_dlr_m1 Dlr
			WHERE
     m1.sg_con_contract = EP.Contract_Number
     AND Veh.Contract_Number = EP.Contract_Number
     AND Dlr.sg_dlr_dealer = m1.sg_con_dealer
     AND (m1.current_flag ='Y'	and EP.Load_Date = :LOADDATE);

		------DROP Temp tables
		DROP TABLE IF EXISTS T_temp_URS_Risk_Type2;
	END;
$$;