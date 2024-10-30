with 
cac_metrics as (
    select 
    seq4()+1 as index
    , case index
    when 1  then 'Services Revenue'
    when 2  then 'Hardware Revenue'
    when 3  then 'Hardware Revenue (Activated in other periods)'
    when 4  then 'Incremental CAC (Replacement Hardware)'
    when 5  then 'Shipping Revenue'
    when 6  then 'Shipping Revenue (Activated in other periods)'
    when 7  then 'Refunds Revenue'
    when 8  then 'Hardware Costs (Direct)'
    when 9  then 'Hardware Costs (Incremental Dealer)'
    when 10 then 'Hardware Costs (Activated in other periods)'
    when 11 then 'Services Costs'
    when 12 then 'Deployment Costs'
    when 13 then 'File Build Costs'
    when 14 then 'Implementation Costs'
    when 15 then 'Implementation Costs (Activated in other periods)'
    when 16 then 'Marketing Costs'
    when 17 then 'Customer Success Costs'
    when 18 then 'BDR Costs with Deal'
    when 19 then 'BDR Costs No Deal'
    when 20 then 'Agent Compensation (Salaries AE with Deals)'
    when 21 then 'Agent Compensation (Salaries & Fringe No Deals)'
    when 22 then 'Agent Compensation (Allocated Commissions)'
    when 23 then 'Agent Compensation (Direct Commissions)'
    when 24 then 'Agent Compensation (SPIFs)'
    when 25 then 'Agent Compensation (Incremental Revenue Bonus)'
    when 26 then 'Agent Compensation (Benefits)'
    when 27 then 'Agent Compensation (Mid Market)'
    when 28 then 'Sales Management Non Dealer Salary'
    when 29 then 'Sales Management Non Dealer Commission'
    when 30 then 'Sales Management Dealer Management'
    when 31 then 'Sales Management Non Dealer Non Deal Costs'
    when 32 then 'Incremental CAC (Timing)'
    else 'Other'
    end as cac_metrics
    , case index
    when 1 then 'count'
    when 2 then 'count'
    when 3 then 'count'
    when 4 then 'Incremental CAC (Replacement Hardware)'
    when 5 then 'Shipping Revenue'
    when 6 then 'Shipping Revenue (Activated in other periods)'
    when 7 then 'Refunds Revenue'
    when 8 then 'Hardware Costs (Direct)'
    when 9 then 'Hardware Costs (Incremental Dealer)'
    when 10 then 'Hardware Costs (Activated in other periods)'
    when 11 then 'Services Costs'
    when 12 then 'Deployment Costs'
    when 13 then 'File Build Costs'
    when 14 then 'Implementation Costs'
    when 15 then 'Implementation Costs (Activated in other periods)'
    when 16 then 'Marketing Costs'
    when 17 then 'Customer Success Costs'
    when 18 then 'BDR Costs with Deal'
    when 19 then 'BDR Costs No Deal'
    when 20 then 'Agent Compensation (Salaries AE with Deals)'
    when 21 then 'Agent Compensation (Salaries & Fringe No Deals)'
    when 22 then 'Agent Compensation (Allocated Commissions)'
    when 23 then 'Agent Compensation (Direct Commissions)'
    when 24 then 'Agent Compensation (SPIFs)'
    when 25 then 'Agent Compensation (Incremental Revenue Bonus)'
    when 26 then 'Agent Compensation (Benefits)'
    when 27 then 'Agent Compensation (Mid Market)'
    when 28 then 'Sales Management Non Dealer Salary'
    when 29 then 'Sales Management Non Dealer Commission'
    when 30 then 'Sales Management Dealer Management'
    when 31 then 'Sales Management Non Dealer Non Deal Costs'
    when 32 then 'Incremental CAC (Timing)'
    else 'Other'
    end as cac_metrics_unit
    from TABLE(GENERATOR(ROWCOUNT => 32)) v
    )
, original_merchant AS (
    SELECT 
    dom.original_merchant_id
    , dom.current_salesforce_account_id
    , dom.accepted_date
    , date_trunc('month', dom.accepted_date)::DATE AS accepted_month
    , dom.FIRST_TRANSACTION_DATE
    , dom.processing_active_month
    , dom.approved_arr
    , dom.activated_arr
    , dateadd('month', 1, date_trunc('month', dom.processing_active_month::DATE)) AS FIRST_FULL_PROCESSING_ACTIVATED_THRESHOLD_MONTH
    , gpv.gpv as first_full_month_gpv
    , gpv.ending_processing_revenue as first_full_month_net_processing_revenue
    , gpv.ending_software_revenue as first_full_month_software_revenue
    , dom.CURRENT_PAYMENTS_MID
    , ifnull(dom.approved_merchant_vertical, 'SMB') as approved_merchant_vertical
    , dom.activated_merchant_vertical
    , dom.original_agent_id as agent_id
    , ad.adp_id
    , ad.AGENT_TYPE_AT_TIME as agent_status_at_accepted_date
    , adp.sales_channel
    , adp.agent_end_month_type
    ,  case 
        when dom.original_agent_id = '5e6025b79adef3226cbb8b79' then 'Duane Owens'  
        when dom.original_agent_id = '5e63cfe1c21f82955227916f' then 'Duane Owens'  
        when ad.AGENT_TYPE_AT_TIME ilike '%Inactive%' then 'Inactive' else 'Other' end as Specific_Dealer
    , case 
        when Specific_Dealer = 'Duane Owens' then 'Duane Owens'
        when ad.IS_DEALER = true then 'Dealer'
        When ad.AGENT_TYPE_AT_TIME = '1099-Inactive Lifetime' then 'Inactive Lifetime'
        when ad.AGENT_TYPE_AT_TIME ilike '%Inactive%' then 'Inactive' 
        when ad.AGENT_TYPE_AT_TIME ilike '%1099%' then '1099s'
        when ad.AGENT_TYPE_AT_TIME = 'W2-FT Salary' then 'W2 Salary' 
        when ad.AGENT_TYPE_AT_TIME = 'W2-Monthly Guarantee' then 'W2 Salary' 
        else 'W2 Commission Only' end as Agent_Specific_Type
    , CASE  WHEN SPECIFIC_DEALER = 'Duane Owens' then 'Duane Owens'
            else sales_channel
            END AS merchant_5_type
    , dsa.residual_payments_percentage
    , case
        when dsa.residual_payments_percentage <= .09 then '0% to 9%'
        when dsa.residual_payments_percentage <= .19 then '10% to 19%'
        when dsa.residual_payments_percentage <= .49 then '20% to 49%'
        when dsa.residual_payments_percentage <= .79 then '50% to 79%'
        else '80% +' end as residual_payments_bucket
        , dom.is_dealer
    , dom.pipeline_source
        , case
        when pipeline_source = 'BDR' then 'BDR'
        when acquisition_channel = 'Merchant Referral' then 'Merchant Referral'
        else 'Non BDR'
        end as BDR_FLAG
    , bdro.bdr_user_id
    , case when bdr_flag = 'BDR' and bdro.bdr_user_id is null then 'NA' else bdro.bdr_user_id end as bdr_id
    FROM  ANALYTICS.so_finance.DIM_original_MERCHANT as dom
    LEFT JOIN ANALYTICS.so_warehouse.agg_agent_daily as ad on dom.original_AGENT_ID = ad.agent_Id and dom.accepted_date::date = ad.date_key::date
    LEFT JOIN ANALYTICS.so_warehouse.agg_agent_monthly as aam on dom.original_AGENT_ID = aam.agent_Id and dom.processing_active_month::date = aam.base_month::date
    left join ANALYTICS.so_warehouse.dim_schedule_a as dsa on dom.original_merchant_ID = dsa.merchant_ID
    left join ANALYTICS.DEV_FINANCE.AGENT_EMPLOYEE_SPINE adp on ad.agent_id = adp.hub_agent_id and adp.base_month = date_trunc('month', ad.date_key)::date
    left join analytics.so_finance.agg_roll_forward_revenue as gpv on dom.original_merchant_id = gpv.original_merchant_id and dateadd('month', 1, date_trunc('month', dom.processing_active_month::DATE)) = gpv.month and gpv.lookback_period = 'Month'
    left join analytics.dev_analytics_temp.bdr_original_merchant bdro on dom.original_merchant_id = bdro.original_merchant_id
    WHERE TRUE 
    AND dom.CURRENT_SALESFORCE_ACCOUNT_ID IS NOT NULL
    --and bdr_id is null and bdr_flag = 'BDR'
    --and dom.original_merchant_id = '6607398475f3c9003c235b37'
    )
, date_spine as (
    select distinct
    date_trunc('month', date_key) as base_month
    from so_warehouse.dim_date
    where date_key >= '2023-01-01'
    and date_key < dateadd('month', -1, date_trunc('month', current_date))
    )
, employee_census as (
    select
    date_trunc('month', file_date)::date as file_base_month_start
    --, last_day(file_date)::date as file_base_month_end
    , adp.adp_id
    , adp.file_date
    , adp.file_date_end
    , adp.worked_in_country
    , adp.payroll_company_code
    , adp.file_number
    , adp.position_id
    , adp.hired_date
    , adp.hire_rehired_date
    , adp.terminated_date
    , adp.position_status
    , adp.payroll_name
    , adp.employee_name
    , adp.legal_first_name
    , adp.legal_last_name
    , adp.job_title_description
    , adp.worker_category_description
    , adp.business_unit_code
    , adp.business_unit_description
    , adp.home_department_code
    , adp.home_department_description
    , adp.clock_full_code
    , adp.clock_full_description
    , adp.home_cost_number_description
    , adp.home_cost_number_code
    , adp.reports_to_associate_id
    , adp.reports_to_legal_first_name
    , adp.reports_to_legal_last_name
    , adp.reports_to_employee_name
    , adp.reports_to_job_title_description
    , adp.elt
    , adp.compensation_type
    , adp.contracting_partner
    , adp.is_leave_of_absence
    , adp.is_agent_headcount
    , adp.area_sales_manager
    , adp.sales_vice_president
    , da.agent_id
    , ad.unit_economics_sub_function
    --, dpd.department_reporting_internal_id
    --, '58' as account_reporting_internal_id
    --, concat(account_reporting_internal_id,'|',dpd.department_reporting_internal_id) as account_department_reporting_internal_id
    --, adp.modified_at
    --, adp._fivetran_synced
    from src_adp.workers_hist as adp
    left join so_finance.dim_pigment_department as dpd on cast(adp.home_department_code as varchar)=dpd.department_number
    left join so_warehouse.dim_agent as da on adp.adp_id=da.adp_id
    LEFT JOIN SO_FINANCE.DIM_PIGMENT_PNL_ACCOUNT_DEPARTMENT ad
    ON ad.ACCOUNT_DEPARTMENT_REPORTING_INTERNAL_ID = concat('58|',dpd.department_reporting_internal_id) 
    where true
    and adp.worker_category_description<>'Contractor - 1099'
    and adp.position_status in ('Active', 'Leave')
    --and adp.file_date >= '2024-09-01'
    qualify last_value(file_id) over (partition by date_trunc('month', file_date) order by file_date, _fivetran_synced) = file_id
    )
, agent_compensation AS (
    select
    'Residuals' as Source 
    , rp.AGENT_OID as Agent_ID
    , rp.AGENT_TRANSACTION_OID as AGENT_TRANSACTION_ID
    , fac.AGENT_PAYROLL_ID
    , fac.Completed_at
    , rp.month as Accrual_Month
    , rp.MERCHANT_OID as MERCHANT_ID
    , dm.original_merchant_id
    , case when rp.MERCHANT_OID is null then 'No Merchant' else 'Merchant' end as Related_to_merchant
    , rp.AGENT_EXCLUDE_FROM_RESIDUALS
    , rp.MERCHANT_EXCLUDE_FROM_RESIDUALS
    , 'Monthly Residuals' as DESCRIPTION
    , 'Residual' as TYPE_LABEL
    , 'residual' as TYPE
    , 'residual' as TYPE_DETAIL
    , 'Monthly Residuals' as Residual_Detail
    , 1 as Metabase_to_HUB_Sign_Conversion
    , 1 as Payroll_total_sign
    , rp.CALCULATED_AGENT_RESIDUAL as Original_Amount
    , rp.CALCULATED_AGENT_RESIDUAL as Agent_Transaction_Sign_Adjusted
    , 1 as Residual_Multiplier
    , Agent_Transaction_Sign_Adjusted*Residual_Multiplier as AMOUNT
    from ANALYTICS.SRC_HUB_POSTGRES.RESIDUAL_Payment as rp
    Left join ANALYTICS.so_warehouse.dim_merchant as dm on rp.MERCHANT_OID = dm.merchant_ID
    Left join ANALYTICS.so_warehouse.FACT_AGENT_COMPENSATION as fac on rp.AGENT_TRANSACTION_OID=fac.AGENT_TRANSACTION_ID
    where 
        rp.AGENT_EXCLUDE_FROM_RESIDUALS <> true 
        and rp.MERCHANT_EXCLUDE_FROM_RESIDUALS <> true
        and rp.CALCULATED_AGENT_RESIDUAL <> 0 
        and rp.month::date >='2021-01-01'
        and rp._FIVETRAN_DELETED='false'
        AND dm.original_merchant_id IS NOT null
    union all 
    select 
    'Agent Transactions' as Source
    , fac.agent_ID
    , fac.AGENT_TRANSACTION_ID
    , fac.AGENT_PAYROLL_ID
    , fac.Completed_at
    , case when fac.TYPE = 'residual' 
        then dateadd(Month, -1, fac.Completed_at)
        else fac.Completed_at end as Accrual_Month
    , dm.merchant_ID
    , dm.original_merchant_ID
    , case when fac.MERCHANT_ID is null then 'No Merchant' else 'Merchant' end as Related_to_merchant
    , 'false' as AGENT_EXCLUDE_FROM_RESIDUALS
    , 'false' as MERCHANT_EXCLUDE_FROM_RESIDUALS
    , fac.DESCRIPTION
    , fac.TYPE_LABEL
    , fac.TYPE
    , case when fac.DESCRIPTION ilike 'Activation - Invoice Funding%' and fac.DESCRIPTION not ilike '%rue%' then 'spoton_invest' else fac.type end as TYPE_DETAIL
    , case when fac.type = 'residual' then
        case 
            when fac.description ilike '%referral residual%' then 'Referral Residual' 
            when fac.description ilike '%Qualified Account Executive%' then 'Qualified Account Executive' 
            when fac.description ilike '%Area Manager Residual%' then 'Area Manager Residuals' 
            when fac.description ilike '%silver%' then 'Silver Tier True Up'     
            when fac.description ilike '%Gold%' then 'Gold Tier True Up'     
            when fac.description ilike '%Office Residuals%' then 'Office Residuals'
            when fac.description ilike '%SOPF%' then 'SOPF Revenue Share'
            when fac.description ilike '%Senior Sales Partner Residual%' then 'Senior Sales Partner Residual' 
            when fac.description ilike '%Guaranteed%' then 'Residual Guaranteed True-Up'
            when fac.description ilike '%40% / 20% Residual%' then '40% / 20% Residual True Up'
            when fac.description ilike '%True Up%' then 'Residual True Up' 
            when fac.description ilike 'Residual for%' and length(fac.description)=37 then 'Monthly Residuals' 
            when fac.description ilike 'Residual for%' and fac.description ilike '%part 2%' then 'Monthly Residuals'
            else 'Other Residuals' end 
        when fac.type = 'miscellaneous' and fac.description ilike '%Office Residuals%' then 'Office Residuals'
        else 'Non Residual'
        end as Residual_Detail
    , CASE
        WHEN TYPE_LABEL = 'Debit' THEN
            CASE WHEN fac.AMOUNT > 0 THEN -1 ELSE 1 END
        WHEN TYPE_LABEL = 'Chargeback' THEN
                CASE WHEN fac.AMOUNT > 0 THEN -1 ELSE 1 END
                ELSE 1
            END AS Metabase_to_HUB_Sign_Conversion
    , fac.AMOUNT as Original_Amount
    , CASE WHEN TYPE_LABEL = 'Residual Buyout Debit' THEN -1
                WHEN TYPE_LABEL = 'Advance Against Bonuses' THEN 0
                WHEN TYPE_LABEL = 'Advance Against Residuals' THEN 0
                WHEN TYPE_LABEL = 'Merchant Fee Debit W2 (Post-tax)' THEN 1
                WHEN DESCRIPTION = 'Loan Settlement' THEN -1
                WHEN DESCRIPTION = 'Loan Payoff Settlement' then 0 ELSE 1 END
                * Metabase_to_HUB_Sign_Conversion as Payroll_total_sign
    , fac.AMOUNT * Payroll_total_sign as Agent_Transaction_Sign_Adjusted
    , case WHEN Residual_Detail='Monthly Residuals' and source='Agent Transactions' and fac.type='residual' then 0 else 1 END as Residual_Multiplier
    , Agent_Transaction_Sign_Adjusted*Residual_Multiplier as AMOUNT
    from ANALYTICS.so_warehouse.FACT_AGENT_COMPENSATION as fac 
    Left join ANALYTICS.so_warehouse.AGG_AGENT_DAILY as aad on fac.agent_ID = aad.agent_ID and fac.completed_at::date = aad.date_key::date 
    Left join ANALYTICS.so_warehouse.FACT_AGENT_PAYROLL as fap on fac.AGENT_PAYROLL_ID = fap.AGENT_PAYROLL_ID
    Left join ANALYTICS.so_warehouse.dim_agent as da on fac.agent_ID=da.agent_ID
    Left join ANALYTICS.so_warehouse.dim_merchant as dm on fac.merchant_ID = dm.merchant_ID
    where fac.completed_at::date >='2021-01-01'
    AND dm.original_merchant_id IS NOT null
    )

, agent_direct_commissions_residuals_cost AS (
   SELECT distinct
    o.original_merchant_id
    , o.processing_active_month
    , o.sales_channel
    --, c.accrual_month
    , sum(case when c.TYPE = 'residual' and dateadd('month', 1, o.processing_active_month) = c.accrual_month then c.AMOUNT else 0 end) over (partition by o.original_merchant_id) AS residuals_agent_direct
    , sum(case when c.TYPE <> 'residual' then c.AMOUNT else 0 end) over (partition by o.original_merchant_id) AS commissions_agent_direct
    FROM original_merchant o
    LEFT JOIN agent_compensation c USING (original_merchant_id)
    WHERE true
    --and c.TYPE = 'residual'
    and o.processing_active_month >= '2023-01-01'
    --qualify residuals_agent_direct <> 0 or commissions_agent_direct <> 0
    --order by 2 desc
    --GROUP BY ALL 
    )
, fulfillment_data as (
    SELECT
    dm.original_merchant_id
    , dm.merchant_id
    , DM.PLAN_CONSOLIDATED_CATEGORY
    , dm.agent_id
    , tran.ENTITY_ID
    , tran.createddate::date as date
    , date_trunc('month', tran.createddate)::date as start_of_month
    , tran.document_number
    --, tran.record_type
    --, tranline.billeddate
    , tranline.item_type
    , tranline.quantity
    --, tranline.rateamount
    , tranline.quantity * tranline.rateamount as cogs_amount
    , item.item_name
    , item.item_display_name
    , tran_2.actual_ship_date::date as salesorder_actual_ship_date
    , tran_2.ship_date::date as salesorder_ship_date
    , tran_2.document_number as sales_order
    , ot.name as order_type

    FROM ANALYTICS.SRC_NETSUITE.NETSUITE_TRANSACTION as tran
    --- adding a link to salesorder ( created from )
    left join ANALYTICS.SRC_NETSUITE.NETSUITE_PREVIOUS_TRANSACTION_LINK as prev_link on tran.transaction_id = prev_link.target_transaction_id
    left join ANALYTICS.SRC_NETSUITE.NETSUITE_TRANSACTION as tran_2 on prev_link.origin_transaction_id = tran_2.transaction_id
    left JOIN ANALYTICS.SRC_NETSUITE.NETSUITE_TRANSACTION_LINE tranline ON tranline.transaction_id = tran.transaction_id
    left JOIN ANALYTICS.SRC_NETSUITE.NETSUITE_ITEM item ON item.item_id = tranline.item_id
    left JOIN ANALYTICS.SRC_NETSUITE.NETSUITE_LOCATION location ON location.location_id = tranline.location
    left join ANALYTICS.SRC_NETSUITE.NETSUITE_CUSTOMER cust on cust.customer_id = tran.entity_id
    left join analytics.so_warehouse.dim_merchant dm on dm.merchant_id = cust.spoton_merchant_id
    left join ANALYTICS.SRC_NETSUITE.NETSUITE_ORDER_TYPE ot on ot.ORDER_TYPE_ID = tran_2.order_type_id
    WHERE true
    and tran.record_type = 'itemfulfillment'
    --and (tranline.item_type in ('InvtPart', 'Assembly') or item.item_name = 'SO-Hdwr_Other' )
    --and tran.ENTITY_ID = 93524
    and quantity <= -1
    --and rateamount is not null
    --and cust.sales_agent_HUB_id = '610851f49adef3002b38786d'
    --and cust.spoton_merchant_id = '660f34b6eff7593924dc09f4'
    --and tran.createddate::date between '2023-11-01' and '2023-11-30'
    --and item.item_name = 'SO-MKACPT'
    and tranline.iscogs = 'T'
    )
, gldata as (
    select
    gl.accounting_period_start_date
    --, gl.document_number
    --, gl.account_number
    --, gl.account_name
    --, gl.account_number_name
    --, transaction_memo
    --, transaction_line_memo
    , gl.department_number
        --, gl.department_number_name
        --, gl.account_department_reporting_internal_id
        --, gl.vendor_reporting_internal_id
        --, gl.vendor_reporting_ID_name
        --, gl.class_reporting_name
        --, gl.subclass_reporting_name
        --, gl.transaction_memo
        --, gl.transaction_line_memo
    --, regexp_replace(regexp_substr(TRANSACTION_LINE_MEMO, 'ADPID#_([A-Z0-9]{9})|ADPID#([A-Z0-9]{9})|ADPAID#([A-Z0-9]{9})', 1, 1), 'ADPID#_|ADPID#|ADPAID#', '') as adp_associate_id
    , coalesce(spoton_merchant_id, regexp_substr(TRANSACTION_LINE_MEMO, '([a-z0-9]{24})', 1, 1)) as merchant_id
    , ad.pnl_reporting_l1
    , ad.PNL_REPORTING_L2
    --, ad.PNL_REPORTING_L2_sort_order
    , case 
    when ad.PNL_REPORTING_L3 in ('Cost of Revenue Allocation', 'Software Cost Of Revenue', 'Transaction Cost Of Revenue', 'Services Cost Of Revenu', 'Hardware Cost Of Revenue') 
    and ad.unit_economics_sub_function in ('Call Center', 'Deployment', 'Implementation', 'Shared Services') 
    then 'exclude from CAC' 
    when ad.PNL_REPORTING_L3 in ('Cost of Revenue Allocation') then 'the rest of allocations'
    else 'keep as CAC'
    end cac_exclude
    , case when gl.account_number in ('502020', '570019') then 'Shipping Cost of Revenue'
    when ad.PNL_REPORTING_L3 in 
            ('Allocations',
            'Building Lease Payments',
            'Cloud Services',
            'Contractors and Consultants',
            'Equipment And Supplies',
            'Facilities And Other',
            'Marketing Expenses',
            'Misc Expenses',
            'Other Outside Services',
            'Taxes') then 'Facilities & 3rd Party Services'
            when ad.PNL_REPORTING_L3 in 
            ('401(K) Match',
            'Employee Welfare',
            'Other Employee Expenses',
            'Payroll Benefits',
            'Payroll Taxes'
            ) then 'Fringe Benefits'
            when ad.PNL_REPORTING_L3 in ('Bonuses', 'Commissions') then 'Bonuses Commissions'
            when ad.PNL_REPORTING_L3 in ('Commissions Capitalization', 'Commissions Amortization') then 'GAAP Commissions'
            when ad.PNL_REPORTING_L3 = 'Spoton Capital' 
            and transaction_memo ilike 'Referred%' and transaction_line_memo = 'Client Referral Bonus' then 'Merchant Referral'
            when ad.PNL_REPORTING_L3 = 'Spoton Capital' then 'Other Referral'
        else ad.PNL_REPORTING_L3
        end as UNIT_ECONOMICS_L3
    , ad.PNL_REPORTING_L3
    --, ad.PNL_REPORTING_L3_sort_order
    , ad.unit_economics_sub_function
    , ad.unit_economics_cac
    , sum(gl.net_amount_reporting_currency_usd) as amount
    from so_finance.netsuite_general_ledger as gl
    LEFT JOIN ANALYTICS.SO_FINANCE.DIM_PIGMENT_PNL_ACCOUNT_DEPARTMENT ad
    ON ad.ACCOUNT_DEPARTMENT_REPORTING_INTERNAL_ID = gl.ACCOUNT_DEPARTMENT_REPORTING_INTERNAL_ID
    where true
    and ad.ebitda_presentation = 'EBITDA'
    and ad.net_revenue_presentation = 'Net Revenue'
    and ifnull(gl.class_reporting_name,'na') not in ('Enterprise', 'Disney')
        --and ad.unit_economics_sub_function = 'Implementation'
        --and gl.department_number ilike '51%'
        --and gl.department_number in ('511000', '513000', '513100', '513200', '514000')
        --and merchant_id = '649c6d47e9b32719379855c7'
    AND gl.net_amount_reporting_currency_usd <> 0
    and gl.account_number not like '6950%'
    and ad.pnl_reporting_l2 <> 'G&A'
    and ad.pnl_reporting_l3 <> 'Severance'
        --and ad.unit_economics_cac <> 'Non CAC'
        --and department_number = '334000'
        --and ad.pnl_reporting_l3 = 'Commissions Amortization'
        --and merchant_id in ('6622e5d98e9514003fdaab21', '6607398475f3c9003c235b37')
        --and merchant_id ilike 'dlr%'
    group by all
)
/*, sub_function_l3_costs as (
    select accounting_period_start_date
    , unit_economics_l3
    , unit_economics_sub_function
    , unit_economics_cac
    , sum(amount) as amount
    from gldata
    where true
    and unit_economics_cac <> 'Non CAC'
    and accounting_period_start_date = '2024-08-01'
    group by all
    ;
    )
*/
--scratchpad
    --select distinct unit_economics_sub_function from gldata where unit_economics_cac <> 'Non CAC';
    --select macro_expense_category, department_number_name, account_name, round(sum(amount),0) as total_amount from gldata where accounting_period_start_date = '2024-04-01' group by all order by 1, 2, total_amount desc;
/*
, merchant_specific_implementation_costs as (
    select
    merchant_id
    , accounting_period_start_date
    , sum(case when department_number = '513000' then amount end) as merchant_specific_installers_amount
    , sum(case when department_number <> '513000' then amount end) as merchant_specific_all_other_expenses_amount
    , sum(amount) as merchant_specific_amount
    from gldata
    where true 
    and accounting_period_start_date >= '2023-01-01'
    and merchant_id is not null
    and unit_economics_sub_function = 'Implementation'
    and unit_economics_cac = 'CAC'
    group by all
    )

, merchant_specific_revenue as (
    select
    original_merchant_id
    --, accounting_period_start_date
    , SUM(CASE WHEN ifnull(mf.FIRST_FULL_PROCESSING_ACTIVATED_THRESHOLD_MONTH, '1900-01-01') = g.accounting_period_start_date AND PNL_REPORTING_L3 = 'Software Revenue' THEN g.amount ELSE 0 END) AS first_full_month_software_revenue
    , mf.FIRST_FULL_PROCESSING_ACTIVATED_THRESHOLD_MONTH
    , avg(CASE 
        WHEN PNL_REPORTING_L3 = 'Software Revenue' 
        and g.amount < 0 
        and ifnull(mf.FIRST_FULL_PROCESSING_ACTIVATED_THRESHOLD_MONTH, '1900-01-01') between dateadd('month', 1, accounting_period_start_date) and dateadd('month', 3, accounting_period_start_date) THEN g.amount end) AS avg_full_month_software_revenue
    , iff(ifnull(first_full_month_software_revenue,0) = 0 or avg_full_month_software_revenue < first_full_month_software_revenue, avg_full_month_software_revenue, first_full_month_software_revenue) as full_month_software_revenue
    , full_month_software_revenue * -0.25 AS full_month_cost_of_software_revenue
     ---*** PS, HW, shipping before/in the activation month
    , SUM(CASE WHEN ifnull(mf.FIRST_FULL_PROCESSING_ACTIVATED_THRESHOLD_MONTH, '1900-01-01') >= g.accounting_period_start_date AND PNL_REPORTING_L3 = 'Hardware Revenue' THEN g.amount ELSE 0 END) AS activated_hardware_revenue
    , SUM(CASE WHEN ifnull(mf.FIRST_FULL_PROCESSING_ACTIVATED_THRESHOLD_MONTH, '1900-01-01') >= g.accounting_period_start_date AND PNL_REPORTING_L3 = 'Hardware Cost Of Revenue' THEN g.amount ELSE 0 END) AS activated_hardware_cost
    , SUM(CASE WHEN ifnull(mf.FIRST_FULL_PROCESSING_ACTIVATED_THRESHOLD_MONTH, '1900-01-01') >= g.accounting_period_start_date AND PNL_REPORTING_L3 = 'Shipping Revenue' THEN g.amount ELSE 0 END) AS activated_shipping_revenue
    , SUM(CASE WHEN PNL_REPORTING_L3 = 'Shipping Cost Of Revenue' THEN g.amount ELSE 0 END) AS activated_shipping_cost
    , SUM(CASE WHEN PNL_REPORTING_L3 = 'Other Non-Recurring Revenue' THEN g.amount ELSE 0 END) AS non_recurring_refunds
    , SUM(CASE WHEN PNL_REPORTING_L3 = 'Services Revenue'  THEN g.amount ELSE 0 END) AS services_revenue
    ---*** replacements, swaps, other HW + shipping after of activation month
    , SUM(CASE WHEN ifnull(mf.FIRST_FULL_PROCESSING_ACTIVATED_THRESHOLD_MONTH, '1900-01-01') < g.accounting_period_start_date AND PNL_REPORTING_L3 = 'Hardware Revenue' THEN g.amount ELSE 0 END) AS replacement_hardware_revenue
    , SUM(CASE WHEN ifnull(mf.FIRST_FULL_PROCESSING_ACTIVATED_THRESHOLD_MONTH, '1900-01-01') < g.accounting_period_start_date AND PNL_REPORTING_L3 = 'Hardware Cost Of Revenue' THEN g.amount ELSE 0 END) AS replacement_shipping_revenue
    , SUM(CASE WHEN ifnull(mf.FIRST_FULL_PROCESSING_ACTIVATED_THRESHOLD_MONTH, '1900-01-01') < g.accounting_period_start_date AND PNL_REPORTING_L3 = 'Shipping Revenue' THEN g.amount ELSE 0 END) AS replacement_hardware_cost
    , sum(g.amount) as total_merchant_specific_revenue_amount
    from gldata g
    left join ANALYTICS.SO_warehouse.DIM_MERCHANT AS dm using (merchant_id)
    left join original_merchant mf using (original_merchant_id)
    where true 
    and accounting_period_start_date >= '2023-01-01'
    and merchant_id is not null
    and mf.processing_active_month >= '2023-04-01'

    and g.PNL_REPORTING_L1 in ('Revenue', 'Cost Of Revenue')
    group by all
    )

*/

, no_mid_total_implementation_costs as (
    select
    accounting_period_start_date
    , sum(case when department_number = '513000' then amount end) as installers_amount
    , sum(case when department_number <> '513000' then amount end) as all_other_expenses_amount
    , sum(amount) as total_amount
    from gldata
    where true 
    and accounting_period_start_date >= '2023-01-01'
    and merchant_id is null
    and unit_economics_sub_function = 'Implementation'
    and unit_economics_cac = 'CAC'
    group by all
    )

, total_implementation_costs as (
    select
    accounting_period_start_date
    , sum(case when department_number = '513000' then amount end) as installers_amount
    , sum(case when department_number <> '513000' then amount end) as all_other_expenses_amount
    , sum(amount) as total_amount
    from gldata
    where true 
    and accounting_period_start_date >= '2023-01-01'
    and unit_economics_sub_function = 'Implementation'
    and unit_economics_cac <> 'Non CAC'
    group by all
    )
, salesforce_cases as (

    select
    c.*
    , (coalesce(c.number_of_extra_days, 0) + 1) * (iff(c.extra_person, 1, 0) + 1) as total_install_days
    from src_salesforce.sfdc_case as c
    left join so_warehouse.dim_merchant AS dm ON dm.salesforce_account_id = c.salesforce_account_id
    left join so_warehouse.agg_agent_daily as ad on dm.agent_id = ad.agent_id and dm.accepted_date::date = ad.date_key::date
    where c.record_type_id = '0124x0000002SCFAA2' -- implementation case type
    --and c.case_id = '500WR000004Kfw0YAC'
    and dm.merchant_id is not null
    and ad.is_dealer = false -- removing dealers because SO implementation don't work on them
    -- Need to align with Giancarlo to make sure this is de-duped better
    QUALIFY  ROW_NUMBER() OVER (PARTITION BY c.salesforce_account_id 
                                ORDER BY 
                                case 
                                when c.status = 'Implementation Complete' then 1 
                                else 2
                                end ASC
                                , c.created_date ASC) = 1 -- one case per merchant

    )

-- monthly units for allocation
, welcome_calls as (
    select
    d.base_month
    , count_if(date_trunc('month', c.welcome_call_date::date) = d.base_month) as welcome_call_count
    from date_spine as d
    left join salesforce_cases as c on date_trunc('month', c.welcome_call_date::date) = d.base_month
    group by all
    )

, onboarding as (
    select
    d.*
    , count_if(date_trunc('month', c.onboarding_date::date) = d.base_month) as onboarding_count
    from welcome_calls as d
    left join salesforce_cases as c on date_trunc('month', c.onboarding_date::date) = d.base_month
    group by all
    )

, menu_build as (
    select
    d.*
    , count_if(date_trunc('month', c.menu_build_date::date) = d.base_month) as menu_build_count
    from onboarding as d
    left join salesforce_cases as c on date_trunc('month', c.menu_build_date::date) = d.base_month
    group by all
    )

, menu_consult as (
    select
    d.*
    , count_if(date_trunc('month', c.menu_consult_date::date) = d.base_month) as menu_consult_count
    from menu_build as d
    left join salesforce_cases as c on date_trunc('month', c.menu_consult_date::date) = d.base_month
    group by all
    )

, follow_up_menu_consult as (
    select
    d.*
    , count_if(date_trunc('month', c.follow_up_menu_consult_date::date) = d.base_month) as follow_up_menu_consult_count
    from menu_consult as d
    left join salesforce_cases as c on date_trunc('month', c.follow_up_menu_consult_date::date) = d.base_month
    group by all
    )

, final_menu_consult as (
    select
    d.*
    , count_if(date_trunc('month', c.final_menu_consult_date::date) = d.base_month) as final_menu_consult_count
    from follow_up_menu_consult as d
    left join salesforce_cases as c on date_trunc('month', c.final_menu_consult_date::date) = d.base_month
    group by all
    )

, installs as (
    select
    d.*
    , sum(total_install_days) as total_install_units
    from final_menu_consult as d
    left join salesforce_cases as c on date_trunc('month', c.install_date::date) = d.base_month
    group by all
    )

, activations as (
    select 
    d.*
    , sum(iff(approved_merchant_vertical = 'Restaurant' and dom.sales_channel <> 'Dealer', 1, 0)) as non_dealer_activated_restaurant_in_month
    , sum(iff(approved_merchant_vertical = 'Express' and dom.sales_channel <> 'Dealer', 1, 0)) as non_dealer_activated_express_in_month
    , sum(iff(approved_merchant_vertical = 'SMB' and dom.sales_channel <> 'Dealer', 1, 0)) as non_dealer_activated_smb_in_month    
    , sum(iff(approved_merchant_vertical = 'Restaurant' and dom.sales_channel = 'Dealer', 1, 0)) as dealer_activated_restaurant_in_month
    , sum(iff(approved_merchant_vertical = 'Express' and dom.sales_channel = 'Dealer', 1, 0)) as dealer_activated_express_in_month
    , sum(iff(approved_merchant_vertical = 'SMB' and dom.sales_channel = 'Dealer', 1, 0)) as dealer_activated_smb_in_month
    , sum(iff(dom.agent_end_month_type = 'W2-FT Commission', 1, 0)) as commission_activated_in_month
    , sum(iff(dom.agent_end_month_type = 'W2-FT Salary', 1, 0)) as salary_activated_in_month
    , sum(iff(dom.agent_end_month_type = '1099', 1, 0)) as "1099_activated_in_month"
    , sum(iff(dom.agent_end_month_type = 'Dealer', 1, 0)) as dealer_activated_in_month
    , sum(iff(dom.agent_end_month_type = 'Inside Sales', 1, 0)) as inside_sales_activated_in_month
    , sum(iff(dom.agent_end_month_type = 'Mid Market AE', 1, 0)) as mid_market_activated_in_month
    , count(distinct dom.original_merchant_id) as activated_total_in_month
    , count(distinct (case when dom.sales_channel = 'Salary' then dom.agent_id end)) as w2_salary_productive_headcount
    , count(distinct (case when dom.sales_channel = 'Commission' then dom.agent_id end)) as w2_commission_productive_headcount
    , count(distinct (case when dom.bdr_flag = 'BDR' then dom.bdr_id end)) as bdr_productive_headcount
    from installs as d 
    left join original_merchant as dom on dom.processing_active_month = d.base_month and dom.activated_arr > 0
    group by all
    )
, ae_headcount as (
    select 
    d.*
    , count(distinct case when e.unit_economics_sub_function = 'BDR' then e.adp_id end) as bdr_headcount
    , count(distinct case when e.unit_economics_sub_function = 'Commission Only' then e.adp_id end) as w2_commission_headcount
    , count(distinct case when e.unit_economics_sub_function = 'Salary' then e.adp_id end) as w2_salary_headcount
    , count(distinct case when e.unit_economics_sub_function = 'Dealer Management' then e.adp_id end) as dealer_mgmt_headcount
    , count(distinct case when e.unit_economics_sub_function = 'Sales Management' then e.adp_id end) as sales_mgmt_headcount
    from activations d
    left join employee_census e on e.file_base_month_start = d.base_month
    group by all
    )
--- begin cost grouping at month, sub_function, and unit_econ_l3
, bdr_cs_mktg_dealer_mgmt_sales_mgmt_sales_ops_sales_support_team_leads as (
    select accounting_period_start_date
        , unit_economics_sub_function
        , unit_economics_l3
        , sum(amount) as amount
        , case when unit_economics_l3 = 'Commission Only' then commission_activated_in_month
               when unit_economics_l3 = 'Salary' then salary_activated_in_month
               when unit_economics_l3 = '1099s' then "1099_activated_in_month"
               when unit_economics_l3 = 'Dealer' then dealer_activated_in_month
               when unit_economics_l3 = 'Inside Sales' then inside_sales_activated_in_month
               when unit_economics_l3 = 'Mid Market' then mid_market_activated_in_month
          else 0 end as activated_channel_specific_mids
        -- activated_channel_specific_mids
        -- activated_channel_specific_cost_per_mid
        -- activated_total_mids
        -- activated_cost_per_mid
        -- implementation_mid_level
        -- implementation_leftover
        -- implementation_units
        -- implementation_cpu
        -- activated_bdr_mids
        -- bdr_cpu
        -- activated_arr
        -- arr_cpu
        -- commission_mid_level
        -- commission_leftover
        -- commission_allocated_per_mid

    --- complete this with Bo    
    , sum(case when unit_economics_sub_function = 'BDR' and unit_economics_l3 = 'Bonuses Commissions' then amount end) as bdr_cost_total_month_bonus_commission
    , sum(case when unit_economics_sub_function = 'BDR' and unit_economics_l3 <> 'Bonuses Commissions' then amount end) as bdr_cost_total_month_non_commission
    , sum(case when unit_economics_l3 = 'Merchant Referral' then amount end) as total_month_merchant_referral_cost
    , sum(case when unit_economics_sub_function = 'Commission Only' and unit_economics_l3 = 'Bonuses Commissions' then amount end) as w2_commission_cost_total_month_bonus_commission
    , sum(case when unit_economics_sub_function = 'Commission Only' and unit_economics_l3 <> 'Bonuses Commissions' then amount end) as w2_commission_cost_total_month_non_commission

    , sum(case when unit_economics_sub_function = 'Salary' and unit_economics_l3 = 'Bonuses Commissions' then amount end) as w2_salary_cost_total_month_bonus_commission
    , sum(case when unit_economics_sub_function = 'Salary' and unit_economics_l3 <> 'Bonuses Commissions' then amount end) as w2_salary_cost_total_month_non_commission

    , sum(case when unit_economics_sub_function = 'Dealer Management' and unit_economics_l3 = 'Bonuses Commissions' then amount end) as Dealer_management_cost_total_month_bonus_commission
    , sum(case when unit_economics_sub_function = 'Dealer Management' and unit_economics_l3 <> 'Bonuses Commissions' then amount end) as Dealer_management_cost_total_month_non_commission

    , sum(case when unit_economics_sub_function = 'Sales Management' and unit_economics_l3 = 'Bonuses Commissions' then amount end) as sales_management_cost_total_month_bonus_commission
    , sum(case when unit_economics_sub_function = 'Sales Management' and unit_economics_l3 <> 'Bonuses Commissions' then amount end) as sales_management_cost_total_month_non_commission

    , sum(case when unit_economics_sub_function = 'Dealer' and unit_economics_l3 = 'Bonuses Commissions' then amount end) as dealer_cost_total_month_bonus_commission
    , sum(case when unit_economics_sub_function = 'Dealer' and unit_economics_l3 <> 'Bonuses Commissions' then amount end) as dealer_cost_total_month_non_commission

    , sum(case when unit_economics_sub_function = '1099s' and unit_economics_l3 = 'Bonuses Commissions' then amount end) as cost_total_month_bonus_commission_1099
    , sum(case when unit_economics_sub_function = '1099s' and unit_economics_l3 <> 'Bonuses Commissions' then amount end) as cost_total_month_non_commission_1099
    from gldata g
    left join activations a on g.accounting_period_start_date = a.base_month
    where true
    and unit_economics_cac <> 'Non CAC'
    group by all
)
-- select * from activations;
select * from bdr_cs_mktg_dealer_mgmt_sales_mgmt_sales_ops_sales_support_team_leads;
, bdr_sales_channel_vertical_allocation_stats_step1 as (
    select 
    processing_active_month
    , approved_merchant_vertical
    , sales_channel
    , BDR_FLAG
    , count(*) as merchant_count
    , sum(approved_arr) as approved_arr
    , sum(activated_arr) as activated_arr
    , sum(first_full_month_gpv) as first_full_month_gpv
    , sum(first_full_month_net_processing_revenue) as first_full_month_processing_revenue
    , sum(first_full_month_software_revenue) as first_full_month_software_revenue
    from original_merchant
    where true
    and activated_arr > 0
    and processing_active_month >= '2023-01-01'
    group by all
)
, bdr_sales_channel_vertical_allocation_stats as (
select 
allocation.processing_active_month
, allocation.approved_merchant_vertical
, allocation.sales_channel
, allocation.BDR_FLAG
, sum(allocation.merchant_count) over (partition by allocation.processing_active_month) as total_month_merchant_count
, sum(allocation.approved_arr) over (partition by allocation.processing_active_month)  as total_month_approved_arr
, sum(allocation.activated_arr) over (partition by allocation.processing_active_month)  as total_month_activated_arr
, sum(allocation.first_full_month_gpv) over (partition by allocation.processing_active_month)  as total_month_first_full_month_gpv
, sum(allocation.first_full_month_processing_revenue) over (partition by allocation.processing_active_month)  as total_month_first_full_month_processing_revenue
, sum(allocation.first_full_month_software_revenue) over (partition by allocation.processing_active_month)  as total_month_first_full_month_software_revenue
--bdr metrics
, iff(bdr_flag = 'BDR', sum(allocation.merchant_count) over (partition by allocation.processing_active_month, allocation.bdr_flag), 0) as total_month_bdr_merchant_count
, iff(bdr_flag = 'BDR', sum(allocation.activated_arr) over (partition by allocation.processing_active_month, allocation.bdr_flag), 0)  as total_month_total_month_bdr_activated_arr
--Merchant Referrals
, iff(bdr_flag = 'Merchant Referral', sum(allocation.merchant_count) over (partition by allocation.processing_active_month, allocation.bdr_flag), 0) as total_month_merchant_referral_count
from bdr_sales_channel_vertical_allocation_stats_step1 as allocation
)
, costs_allocated_to_activated_merchant as (
    select --o.original_merchant_id,
    o.processing_active_month
    , o.approved_merchant_vertical
    , o.sales_channel
    , o.bdr_flag
    , 1 as merchant_count
    , o.activated_arr
    --bdr cost allocation
    , case when o.bdr_flag = 'BDR' then div0(ae.bdr_productive_headcount, ae.bdr_headcount) end as bdr_productive_pct
    , case when o.bdr_flag = 'BDR' then div0(ae.bdr_headcount - ae.bdr_productive_headcount, ae.bdr_headcount) end as bdr_non_productive_pct
    , b.total_month_bdr_merchant_count
    , c.bdr_cost_total_month_non_commission
    , div0((case when o.bdr_flag = 'BDR' then 1 else 0 end), b.total_month_bdr_merchant_count) * c.bdr_cost_total_month_non_commission * bdr_productive_pct as bdr_productive_non_commission_cost
    , div0((case when o.bdr_flag = 'BDR' then 1 else 0 end), b.total_month_bdr_merchant_count) * c.bdr_cost_total_month_non_commission * bdr_non_productive_pct as bdr_non_productive_non_commission_cost
        --, sum(case when o.bdr_flag = 'BDR' then 1 else 0 end) over (partition by o.processing_active_month) - b.total_month_bdr_merchant_count as bdr_count_check
    --BONUS & COMMISSION - at Count ATM - should it be at ARR?
    , b.total_month_total_month_bdr_activated_arr
    , c.bdr_cost_total_month_bonus_commission
    , div0((case when o.bdr_flag = 'BDR' then 1 else 0 end), b.total_month_bdr_merchant_count) * c.bdr_cost_total_month_bonus_commission as bdr_bonus_commission_cost
        --, div0((case when o.bdr_flag = 'BDR' then o.activated_arr else 0 end), b.total_month_total_month_bdr_activated_arr) * c.bdr_cost_total_month_bonus_commission as bdr_bonus_commission_cost
        --, sum(case when o.bdr_flag = 'BDR' then o.activated_arr else 0 end) over (partition by o.processing_active_month) - b.total_month_total_month_bdr_activated_arr as bdr_ARR_check
    , bdr_bonus_commission_cost + bdr_productive_non_commission_cost + bdr_non_productive_non_commission_cost as bdr_deal_cost
    --Merchant Referrals
    , c.total_month_merchant_referral_cost
    , div0((case when o.bdr_flag = 'Merchant Referral' then 1 else 0 end), b.total_month_merchant_referral_count) * c.total_month_merchant_referral_cost as merchant_referral_cost
    --agent_commissions
    , c.w2_commission_cost_total_month_bonus_commission
    from original_merchant o
    left join bdr_sales_channel_vertical_allocation_stats b using (processing_active_month, sales_channel, bdr_flag, approved_merchant_vertical)
    left join bdr_cs_mktg_dealer_mgmt_sales_mgmt_sales_ops_sales_support_team_leads c on c.accounting_period_start_date = o.processing_active_month
    left join ae_headcount ae on ae.base_month = o.processing_active_month
    left join agent_direct_commissions_residuals_cost acr
    where activated_arr > 0 and processing_active_month >= '2023-01-01'
    --qualify bdr_ARR_check <> 0 AND bdr_deal_cost <> 0
)
select original_merchant_id
, processing_active_month
case c.index
    when 1 then services_revenue --'Services Revenue'"
    when 2 then hardware_revenue --'Hardware Revenue'
    when 3 then 0 --'Hardware Revenue (Activated in other periods)'
    when 4 then replacement_hardware_revenue --'Incremental CAC (Replacement Hardware)'
    when 5 then shipping_revenue --'Shipping Revenue'
    when 6 then 0 --'Shipping Revenue (Activated in other periods)'
    when 7 then refunds_revenue 'Refunds Revenue'
    when 8 then 'Hardware Costs (Direct)'
    when 9 then 'Hardware Costs (Incremental Dealer)'
    when 10 then 'Hardware Costs (Activated in other periods)'
    when 11 then 'Services Costs'
    when 12 then 'Deployment Costs'
    when 13 then 'File Build Costs'
    when 14 then 'Implementation Costs'
    when 15 then 'Implementation Costs (Activated in other periods)'
    when 16 then 'Marketing Costs'
    when 17 then 'Customer Success Costs'
    when 18 then bdr_bonus_commission_cost + bdr_productive_non_commission_cost --'BDR Costs with Deal'
    when 19 then bdr_non_productive_non_commission_cost --'BDR Costs No Deal'
    when 20 then 'Agent Compensation (Salaries AE with Deals)'
    when 21 then 'Agent Compensation (Salaries & Fringe No Deals)'
    when 22 then 'Agent Compensation (Allocated Commissions)'
    when 23 then 'Agent Compensation (Direct Commissions)'
    when 24 then 'Agent Compensation (SPIFs)'
    when 25 then 'Agent Compensation (Incremental Revenue Bonus)'
    when 26 then 'Agent Compensation (Benefits)'
    when 27 then 'Agent Compensation (Mid Market)'
    when 28 then 'Sales Management Non Dealer Salary'
    when 29 then 'Sales Management Non Dealer Commission'
    when 30 then 'Sales Management Dealer Management'
    when 31 then 'Sales Management Non Dealer Non Deal Costs'
    when 32 then 'Incremental CAC (Timing)'
    else 'Other'
    end
, SUM(merchant_referral_cost)
, max(bdr_deal_cost)
, median(bdr_deal_cost)
, min(bdr_deal_cost)
, sum(bdr_deal_cost)
, sum(bdr_bonus_commission_cost)
, sum(bdr_non_productive_non_commission_cost) 
from costs_allocated_to_activated_merchants c
cross join cac_metrics m
group by 1 order by 1 desc
;














, no_mid_costs_per_install as (
    select
    base_month
    , onboarding_count + welcome_call_count + menu_build_count + menu_consult_count + follow_up_menu_consult_count + final_menu_consult_count as total_other_department_units
    , total_install_units as installer_units
    , t.installers_amount
    , t.all_other_expenses_amount
    , t.total_amount
    , iff(base_month != date_trunc('month', current_date)
    , (t.all_other_expenses_amount / total_other_department_units), 750) as cost_per_other_departments_unit -- cost doesn't work until month is closed so add a placeholder
    , iff(base_month != date_trunc('month', current_date)
    , (t.installers_amount / installer_units), 2000) as cost_per_install_unit -- cost doesn't work until month is closed so add a placeholder
    from activations as i
    left join no_mid_total_implementation_costs as t on i.base_month = t.accounting_period_start_date
    )

--, costs_per_install as (
    select
    base_month
    , onboarding_count + welcome_call_count + menu_build_count + menu_consult_count + follow_up_menu_consult_count + final_menu_consult_count as total_other_department_units
    , total_install_units as installer_units
    , t.installers_amount
    , t.all_other_expenses_amount
    , t.total_amount
    , iff(base_month != date_trunc('month', current_date)
    , (t.all_other_expenses_amount / total_other_department_units), 750) as cost_per_other_departments_unit -- cost doesn't work until month is closed so add a placeholder
    , iff(base_month != date_trunc('month', current_date)
    , (t.installers_amount / installer_units), 2000) as cost_per_install_unit -- cost doesn't work until month is closed so add a placeholder
    from activations as i
    left join total_implementation_costs as t on i.base_month = t.accounting_period_start_date
    ;
    )

--main query
--, mid_level_gl_and_allocations as (
select
dm.merchant_id
, dom.ORIGINAL_MERCHANT_ID
, c.status
, PROCESSING_ACTIVE_MONTH
--, c.install_date
--, c.install_resource
, iff(c.status = 'Implementation Complete' or date_trunc('month', c.install_date::date) <= co0.base_month, 1, 0) as install_completed_flag
, coalesce(c.number_of_extra_days, 0) as extra_days_count
, (iff(c.extra_person, 1, 0) + install_completed_flag) as installers_count
, (extra_days_count + install_completed_flag) * installers_count as installer_person_days_units
, coalesce(co0.cost_per_install_unit, 0) as cost_per_install_unit
--, c.menu_build_resource
--, c.menu_build_date
, coalesce(co0.cost_per_other_departments_unit, 0) as menu_build_cost
--, c.menu_consult_resource
--, c.menu_consult_date
, coalesce(co0.cost_per_other_departments_unit, 0) as menu_consult_cost
, coalesce(co0.cost_per_other_departments_unit, 0) as follow_up_menu_consult_cost
, coalesce(co0.cost_per_other_departments_unit, 0) as final_menu_consult_cost
--, c.welcome_call_resource
--, c.welcome_call_date
, coalesce(co0.cost_per_other_departments_unit, 0) as welcome_call_cost
--, c.onboarding_resource
--, c.onboarding_date
, coalesce(co0.cost_per_other_departments_unit, 0) as onboarding_cost
--, wcw.adp_id
--, c.extra_person

--cost in the month of occurance
, cost_per_install_unit * installer_person_days_units as install_cost -- installer salary + travel
, onboarding_cost + welcome_call_cost + menu_build_cost + menu_consult_cost + follow_up_menu_consult_cost + final_menu_consult_cost + install_cost as total_implementation_cost
--units of activated merchants
, (iff(c.extra_person, 1, 0) + iff(c.install_resource is not null, 1, 0)) * (coalesce(c.number_of_extra_days, 0) + 1) as total_install_units
, sum(total_install_units) over (partition by dom.processing_active_month) as total_month_install_units
, div0(co0.installers_amount, total_month_install_units) as cost_per_install_unit_activated_mids
, cost_per_install_unit_activated_mids * total_install_units as cost_of_installers_per_mid
, iff(c.onboarding_date is not null,1,0) as onboarding_count
, iff(c.welcome_call_date is not null,1,0) as welcome_call_count
, iff(c.menu_build_date is not null,1,0) as menu_build_count
, iff(c.menu_consult_date is not null,1,0) as menu_consult_count
, iff(c.follow_up_menu_consult_date is not null,1,0) as follow_up_menu_consult_count
, iff(c.final_menu_consult_date is not null,1,0) as final_menu_consult_count
, onboarding_count + welcome_call_count + menu_build_count + menu_consult_count + follow_up_menu_consult_count + final_menu_consult_count as total_other_departments_units
, sum(total_other_departments_units) over (partition by dom.processing_active_month) as total_month_other_departments_units
, div0(co0.all_other_expenses_amount, total_month_other_departments_units) as cost_per_other_departments_unit_activated_mids
, cost_per_other_departments_unit_activated_mids * total_other_departments_units as cost_of_other_resources_per_mid
, iff(cost_of_other_resources_per_mid = 0, DIV0(co0.total_amount, (total_install_units + total_other_departments_units)), cost_of_installers_per_mid + cost_of_other_resources_per_mid) as total_implementation_per_mid
from salesforce_cases as c
left join ANALYTICS.SO_warehouse.DIM_MERCHANT AS dm ON dm.SALESFORCE_ACCOUNT_ID = c.salesforce_account_id
left join original_merchant as dom on dm.original_merchant_id = dom.original_merchant_id
--left join merchant_specific_implementation_costs as e on e.merchant_id = dm.merchant_id and e.merchant_id is not null
--left join merchant_specific_revenue as r on r.original_merchant_id = dom.original_merchant_id
left join costs_per_install as co0 on co0.base_month = dom.processing_active_month
where true
and dom.ORIGINAL_MERCHANT_ID IS NOT NULL
--and dm.merchant_id = '642b7d3eb4c09f003d01475d'
--and dom.processing_active_date >= '2023-01-01'
and c.created_date >= '2023-01-01'
--and dm.merchant_name ilike '%best pizza & subs%'
--group by all
--order by 1
;
)