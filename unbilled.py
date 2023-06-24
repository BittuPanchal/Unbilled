import streamlit as st
import pandas as pd
import pandas_gbq
from datetime import datetime
from datetime import date
from io import BytesIO
import base64

project_id = 'pathwell-datawarehouse'

# Query to fetch distinct updated_at dates
query_dynamic_date = f"""
SELECT DISTINCT(cast(DATE(updated_at) as string)) AS updated_at_dynamic
FROM  `pathwell-datawarehouse`.`raw`.`schedule_reports` order by updated_at_dynamic desc
"""

# Execute the query and retrieve the results
results_dynamic_date = pandas_gbq.read_gbq(query_dynamic_date, project_id=project_id)

dates = results_dynamic_date['updated_at_dynamic'].tolist()

st.title("Historical Data for Unbilled")
selected_date_1 = st.selectbox('date according to updated at', dates, key=str)


    
    
query = f"""
with sr as
(
select * from
(
with schedule_report_first_pass as 
(
    select
    cast(target_date as date) as visit_date,
    patient_last,
    patient_first,
    cast(medical_record_number as string) as mrn,
    phone,
    replace(trim(task_name),"  ", " ") as task_name,
    trim(task_type) as task_type,
    split(provider_last, ' ') as provider_last,
    split(provider_first, ' ') as provider_first,
    trim(status) as status,
    time_in,
    time_out,
    documentation_time_min,
    travel_time_min,
    address,
    city,
    state,
    zip_code,
    trim(insurance) as insurance,
    productivity_units,
    mileage,
    employee_id,
    agency,
    updated_at,
    unique_visit_id,
    EXTRACT(HOUR FROM (time_out - time_in)) as hours,
    --EXTRACT(MINUTE FROM (time_out - time_in))as minutes,
    case
        when EXTRACT(MINUTE FROM (time_out - time_in)) < 7.5 then 0
        when EXTRACT(MINUTE FROM (time_out - time_in)) <= 22.5 then 0.25
        when EXTRACT(MINUTE FROM (time_out - time_in)) <= 37.5 then 0.50
        when EXTRACT(MINUTE FROM (time_out - time_in)) <= 52.5 then 0.75
        else 1
    end as rounded_hours
    from (select *, 
          row_number() over (partition by unique_visit_id order by updated_at desc) rn
          from (select * from `pathwell-datawarehouse`.`raw`.`schedule_reports` where date(updated_at) < ('{selected_date_1}'))
          )s
    where rn = 1
),

schedule_report_enhancement as 
(
    select 
    visit_date,patient_last,patient_first,mrn,task_name,task_type,status,
    provider_last[offset(0)] as provider_last,
    provider_first[offset(0)] as provider_first,
    time_in,time_out,documentation_time_min,travel_time_min,
    address,city,state,zip_code,insurance,
    productivity_units,mileage,employee_id,agency,
    updated_at,unique_visit_id,hours,rounded_hours
    from schedule_report_first_pass
),

filtered_schedule_report as 
(
    select *
    from schedule_report_enhancement
    where task_type in ('OT','PT','SN','ST','MSW','HHA') and
          status not in ('Missed Visit (Approved) (MV)','Missed Visit','Not Started','Not Started (MV)','Submitted with Signature (MV)',
                         'Returned for Review (MV)', 'Not Yet Due','Not Yet Due (MV)', 'Returned for Review (MV)', 'Saved (MV)') 
),

task_mapped_report as 
(
    select s.*,
    n.task_group,
    n.discipline_type
    from filtered_schedule_report as s
    left join pathwell-datawarehouse.transformed.dim_task_name_mapping as n
    on s.task_name = n.task_name
),

final_schedule_report as
(
    select 
    t.visit_date,
    t.patient_last,
    t.patient_first,
    t.mrn,
    t.task_name,
    t.task_type,
    case 
        when t.task_type = 'HHA' then 'HHA'
        else 'Clinical'
    end as visit_type,
    t.provider_last,
    t.provider_first,
    t.status,
    t.time_in,
    t.time_out,
    t.documentation_time_min,
    t.travel_time_min,
    t.address,
    t.city,
    t.state,
    t.zip_code,
    t.insurance,
    t.productivity_units,
    t.mileage,
    t.employee_id,
    t.agency,
    t.updated_at,
    t.unique_visit_id,
    (t.hours + t.rounded_hours) as total_hours,
    t.task_group,
    t.discipline_type,
    i.parent_insurance,
    i.insurance_type,
    i.payment_model
    from task_mapped_report as t
    left join pathwell-datawarehouse.transformed.dim_insurance_mapping as i 
    on t.insurance = i.insurance and t.agency = i.office
)


, cte1 as
(
select *,
       row_number() over(partition by unique_visit_id order by updated_at desc) rn
    from final_schedule_report
    where patient_last not in ('Again', 'Test', 'Test2', 'Test3', 'ZZZZ')
)
, cte2 as (
select visit_date,
       patient_last,
       patient_first,
       mrn,
       task_name,
       task_type,
       visit_type,
       provider_last,
       provider_first,
       status,
       time_in,
       time_out,
       documentation_time_min,
       travel_time_min,
       address,
       city,
       state,
       zip_code,
       insurance,
       productivity_units,
       mileage,
       employee_id,
       agency,
       updated_at,
       unique_visit_id,
       total_hours,
       task_group,
       discipline_type,
       parent_insurance,
       insurance_type,
       payment_model
    from cte1
    where rn = 1
)
select * from cte2
)
),
scheduled_report_with_episodes as
(
    select s.*, 
    u.episode_start_date, 
    u.episode_end_date, 
    u.episode_identifier
    from sr as s
    left join (with unique_pdgm_episodes as 
(
    SELECT DISTINCT agency, mrn, episode_start_date, episode_end_date, episode_identifier
    FROM (SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY episode_identifier ORDER BY episode_end_date ASC) rn
        FROM (with episodes as 
(
  SELECT
  
         
         CASE
           WHEN INSTR(unique_id, '.') > 0 THEN LEFT(unique_id, INSTR(unique_id, '.') - 1)
           ELSE unique_id
         END AS unique_id,
  
  --unique_id,    
  patient_last_name,
  patient_first_name,   
  cast(mrn as string) as mrn,
  medicare_,    
  cast(dob as date) as dob,   
  branch,   
  clinic,   
  physician_last_name,    
  physician_first_name,   
  suffix,   
  --concat(agency, '_', cast(mrn as string), '_', cast(episode_start_date as string)) as episode_identifier, 
  cast(soc_date as date) as soc_date, 
  cast(episode_start_date as date) as episode_start_date,   
  cast(episode_end_date as date) as episode_end_date,   
  cast(billing_period_start_date as date) as billing_period_start_date,   
  cast(billing_period_end_date as date) as billing_period_end_date, 
  --concat(agency, '_', cast(mrn as string), '_', episode_start_date , '_', episode_end_date) as episode_identifier,
  episode_identifier,
  concat(agency, '_', cast(mrn as string), '_', billing_period_start_date , '_', billing_period_end_date)  as billing_period_identifier,  
  cast(billing_period as int) as billing_period,    
  cast(first_billable_visit as date) as first_billable_visit, 
  cast(total_scheduled_visits as int) as  total_scheduled_visits,   
  cast(total_completed_visits as int) as  total_completed_visits,   
  cast(total_missed_visits as int) as total_missed_visits,    
  cast(sn as int) as sn,      
  cast(pt as int) as pt,    
  cast(ot as int) as ot,    
  cast(st as int) as st,    
  cast(hha as int) as hha,    
  cast(msw as int) as msw,    
  cast(total_therapy_visits as int) as total_therapy_visits,    
  primary_diagnosis,    
  diagnosis_description,    
  initial_hipps,    
  admission_source,   
  admission_timing,   
  clinical_grouping,    
  functional_level,   
  comorbidity_level,    
  case_mix_weight,    
  wage_index,   
  anticipated_30_day_payment,   
  rural_add_on,   
  cast(lupa_level as int) as lupa_level,    
  lupa_add_on,
  lupa_payment,
  outlier,
  sequester,
  charge,
  cast(rap_date as date) as rap_date,   
  rap_anticipated_payment,    
  rap_payment,    
  cast(rap_payment_date as date) as rap_payment_date,   
  cast(take_back_date as date) as take_back_date,   
  cast(eoe_date as date) as eoe_date, 
  eoe_anticipated_payment,    
  eoe_payment,    
  cast(eoe_payment_date as date) as eoe_payment_date,   
  pcr_tracking_number,    
  cast(number_of_outstanding_orders as int) as number_of_outstanding_orders,    
  referral_sources,   
  cast(referral_date as date) as referral_date,   
  trim(insurance) as insurance,    
  patient_address,    
  patient_city,   
  zip_code,   
  oasis_type,   
  cbsa,   
  fips_code,    
  agency,   
  updated_at
  FROM (SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY updated_at DESC) rn
        FROM  (select * from `pathwell-datawarehouse`.`raw`.`pdgm_episode_reports` where date(updated_at) < ('{selected_date_1}'))
        )e
  WHERE rn = 1 and 
        total_scheduled_visits is not null and total_scheduled_visits!=0 and
        (total_completed_visits!=0 or total_missed_visits!=0)
),

episodes_and_insurance as
(
  select e.*,
  i.parent_insurance,
  i.insurance_type,
  i.payment_model, 
  i.payment_multiplier
  from episodes as e
  left join pathwell-datawarehouse.transformed.dim_insurance_mapping  as i 
  on e.insurance = i.insurance and e.agency = i.office
)

select * from (
select *, row_number() over(partition by unique_id order by updated_at desc) rn1 from episodes_and_insurance
) where rn1 = 1)
        )e
    WHERE rn = 1
), 

unique_mc_episodes as
(
    SELECT DISTINCT agency, mrn, episode_start_date, episode_end_date, unique_id as episode_identifier
    FROM (SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY episode_end_date ASC) rn
        FROM (with episodes as 
(
  SELECT 
  unique_id,    
  patient_last_name,
  patient_first_name,   
  cast(mrn as string) as mrn,
  policy_,    
  cast(dob as date) as dob,   
  branch,   
  clinic,  
  trim(insurance) as insurance, 
  zip_code,
  physician_last_name,    
  physician_first_name,    
  cast(soc_date as date) as soc_date, 
  cast(episode_start_date as date) as episode_start_date,   
  cast(episode_end_date as date) as episode_end_date,  
  cast(total_scheduled_visits as int) as  total_scheduled_visits,   
  cast(total_completed_visits as int) as  total_completed_visits,   
  cast(total_missed_visits as int) as total_missed_visits, 
  cast(total_orphan_visits as int) as total_orphan_visits,   
  cast(sn as int) as sn,      
  cast(pt as int) as pt,    
  cast(ot as int) as ot,    
  cast(st as int) as st,    
  cast(hha as int) as hha,    
  cast(msw as int) as msw, 
  agency,        
  total_allowed, 
  total_charges,
  total_payments, 
  last_payment_date,   
  updated_at
  FROM (SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY updated_at DESC) rn
        FROM  (select * from `pathwell-datawarehouse`.`raw`.`mc_episode_reports` where date(updated_at) < ('{selected_date_1}'))
        )e
  WHERE rn = 1 and 
        total_scheduled_visits is not null and total_scheduled_visits!=0 and
        (total_completed_visits!=0 or total_missed_visits!=0)
),

episodes_and_insurance as
(
  select e.*,
  i.parent_insurance,
  i.insurance_type,
  i.payment_model, 
  i.payment_multiplier
  from episodes as e
  left join pathwell-datawarehouse.transformed.dim_insurance_mapping as i 
  on e.insurance = i.insurance and e.agency = i.office
)

select * from episodes_and_insurance)
        )e
    WHERE rn = 1
),

unique_episodes as
(
    SELECT * FROM unique_pdgm_episodes
    UNION ALL
    SELECT * FROM unique_mc_episodes
) 

SELECT DISTINCT agency, mrn, episode_start_date, episode_end_date, episode_identifier FROM unique_episodes) as u
    on s.mrn = u.mrn AND s.agency=u.agency AND (s.visit_date >= u.episode_start_date and s.visit_date <= u.episode_end_date)
), 



unique_visits as 
(
    select *
    from (select *, 
          ROW_NUMBER() OVER (PARTITION BY unique_visit_id ORDER BY episode_start_date ASC) rn
          from scheduled_report_with_episodes
         )e
    where rn = 1
),

sre as
(
select s.*,
current_datetime() as created_at 
from unique_visits as s
),
billable_visits as
(
    select s.*, 
    from sre as s
    where task_name not in (select * from pathwell-datawarehouse.transformed.non_billable_visits 
        )
),

pdgm_episode_report as
(
select * from
(
with episodes as 
(
  SELECT
  
         
         CASE
           WHEN INSTR(unique_id, '.') > 0 THEN LEFT(unique_id, INSTR(unique_id, '.') - 1)
           ELSE unique_id
         END AS unique_id,
  
  --unique_id,    
  patient_last_name,
  patient_first_name,   
  cast(mrn as string) as mrn,
  medicare_,    
  cast(dob as date) as dob,   
  branch,   
  clinic,   
  physician_last_name,    
  physician_first_name,   
  suffix,   
  --concat(agency, '_', cast(mrn as string), '_', cast(episode_start_date as string)) as episode_identifier, 
  cast(soc_date as date) as soc_date, 
  cast(episode_start_date as date) as episode_start_date,   
  cast(episode_end_date as date) as episode_end_date,   
  cast(billing_period_start_date as date) as billing_period_start_date,   
  cast(billing_period_end_date as date) as billing_period_end_date, 
  --concat(agency, '_', cast(mrn as string), '_', episode_start_date , '_', episode_end_date) as episode_identifier,
  episode_identifier,
  concat(agency, '_', cast(mrn as string), '_', billing_period_start_date , '_', billing_period_end_date)  as billing_period_identifier,  
  cast(billing_period as int) as billing_period,    
  cast(first_billable_visit as date) as first_billable_visit, 
  cast(total_scheduled_visits as int) as  total_scheduled_visits,   
  cast(total_completed_visits as int) as  total_completed_visits,   
  cast(total_missed_visits as int) as total_missed_visits,    
  cast(sn as int) as sn,      
  cast(pt as int) as pt,    
  cast(ot as int) as ot,    
  cast(st as int) as st,    
  cast(hha as int) as hha,    
  cast(msw as int) as msw,    
  cast(total_therapy_visits as int) as total_therapy_visits,    
  primary_diagnosis,    
  diagnosis_description,    
  initial_hipps,    
  admission_source,   
  admission_timing,   
  clinical_grouping,    
  functional_level,   
  comorbidity_level,    
  case_mix_weight,    
  wage_index,   
  anticipated_30_day_payment,   
  rural_add_on,   
  cast(lupa_level as int) as lupa_level,    
  lupa_add_on,
  lupa_payment,
  outlier,
  sequester,
  charge,
  cast(rap_date as date) as rap_date,   
  rap_anticipated_payment,    
  rap_payment,    
  cast(rap_payment_date as date) as rap_payment_date,   
  cast(take_back_date as date) as take_back_date,   
  cast(eoe_date as date) as eoe_date, 
  eoe_anticipated_payment,    
  eoe_payment,    
  cast(eoe_payment_date as date) as eoe_payment_date,   
  pcr_tracking_number,    
  cast(number_of_outstanding_orders as int) as number_of_outstanding_orders,    
  referral_sources,   
  cast(referral_date as date) as referral_date,   
  trim(insurance) as insurance,    
  patient_address,    
  patient_city,   
  zip_code,   
  oasis_type,   
  cbsa,   
  fips_code,    
  agency,   
  updated_at
  FROM (SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY unique_id ORDER BY updated_at DESC) rn
        FROM (select * from `pathwell-datawarehouse`.`raw`.`pdgm_episode_reports` where date(updated_at) < ('{selected_date_1}'))
        )e
  WHERE rn = 1 and 
        total_scheduled_visits is not null and total_scheduled_visits!=0 and
        (total_completed_visits!=0 or total_missed_visits!=0)
),

episodes_and_insurance as
(
  select e.*,
  i.parent_insurance,
  i.insurance_type,
  i.payment_model, 
  i.payment_multiplier
  from episodes as e
  left join pathwell-datawarehouse.transformed.dim_insurance_mapping  as i 
  on e.insurance = i.insurance and e.agency = i.office
)

select * from (
select *, row_number() over(partition by unique_id order by updated_at desc) rn1 from episodes_and_insurance
) where rn1 = 1
)
),
fct_episode_rp as
(
  select e.*,

  
  anticipated_30_day_payment*payment_multiplier as ongoing_revenue,
  eoe_anticipated_payment*payment_multiplier as closed_revenue
  from pdgm_episode_report as e
  
),


billable_visits_report as
(
    select s.*, 
    r.type as rate_type,
    r.from_date as rate_card_from_date,
    r.to_date as rate_card_to_date,
    r.rate,
    case 
        when r.type = 'Hourly' then round(r.rate * total_hours,2)
        when r.type = 'Unit' then r.rate
    end as revenue_per_visit
    from billable_visits as s
    left join pathwell-datawarehouse.transformed.dim_rate_cards as r 
    on s.insurance = r.insurer and s.task_name = r.task_name and 
        lower(trim(s.agency)) = lower(trim(r.location)) and 
        s.visit_date <= r.to_date and s.visit_date >= r.from_date
    where  s.task_name NOT IN ('OASIS-D1 Death', 'OT Discharge (Non-Billable)',
                        'OASIS-D1 Transfer','OASIS-D1 Discharge (Non-Billable)',
                        'Discharge Summary', 'MSW Discharge (Non-Billable)', 
                        'Aide Supervisory Visit', 'OASIS-D1 Transfer (PT)', 
                        'PT Discharge (Non-Billable)','OASIS-D1 Discharge (Non-Billable) - PT', 
                        'LPN 1/Month Supervisory Visit', 'OASIS-D1 Transfer (OT)',
                        'OASIS-D1 Transfer (ST)','OASIS-D1 Discharge (Non-Billable) - OT', 
                        'Skilled Nurse Visit (Non-Billable)', 'Discharge Summary (ST)', 
                        'Discharge Summary (OT)','Discharge Summary (PT)', 'Transfer Summary', 
                        'OASIS-D1 Death (PT)','Aide Care Plan')
),

fct_rate_card_visits as
(
select * from billable_visits_report
where payment_model = 'Per Visit Payment' or payment_model is NULL
),

scheduled_report_with_billing_periods as
(
    select s.*, 
    u.unique_id as billing_period_id,
    u.billing_period_start_date,
    u.billing_period_end_date,
    u.billing_period,
    u.closed_revenue as episode_closed_revenue,
    u.ongoing_revenue as episode_ongoing_revenue,
    u.total_scheduled_visits,
    u.total_completed_visits,
    u.lupa_level,
    u.eoe_date,
    LAST_DAY(s.visit_date) as last_day_of_month,
    DATE_ADD(DATE_ADD(LAST_DAY(s.visit_date),INTERVAL 1 DAY),INTERVAL - 1 MONTH) AS first_day_of_month 
    from billable_visits as s
    left join fct_episode_rp as u
    on s.episode_identifier = u.episode_identifier AND (s.visit_date >= u.billing_period_start_date and s.visit_date <= u.billing_period_end_date)
    where s.payment_model = 'Episodic Payment' and  s.task_name NOT IN ('OASIS-D1 Death', 'OT Discharge (Non-Billable)',
                        'OASIS-D1 Transfer','OASIS-D1 Discharge (Non-Billable)',
                        'Discharge Summary', 'MSW Discharge (Non-Billable)', 
                        'Aide Supervisory Visit', 'OASIS-D1 Transfer (PT)', 
                        'PT Discharge (Non-Billable)','OASIS-D1 Discharge (Non-Billable) - PT', 
                        'LPN 1/Month Supervisory Visit', 'OASIS-D1 Transfer (OT)',
                        'OASIS-D1 Transfer (ST)','OASIS-D1 Discharge (Non-Billable) - OT', 
                        'Skilled Nurse Visit (Non-Billable)', 'Discharge Summary (ST)', 
                        'Discharge Summary (OT)','Discharge Summary (PT)', 'Transfer Summary', 
                        'OASIS-D1 Death (PT)','Aide Care Plan')
), 

billing_period_overlap as 
(
    select *, 
    --FIRST_DAY(visit_date)as first_day_of_month, 
    case
        when billing_period_end_date < last_day_of_month then billing_period_end_date
        else last_day_of_month
    end as minEnd, 
    case
        when billing_period_start_date < first_day_of_month then first_day_of_month
        else billing_period_start_date
    end as maxStart
    from scheduled_report_with_billing_periods
), 

visits_in_overlap as 
(
    select 
    billing_period_id, 
    first_day_of_month,
    last_day_of_month, 
    count(*) as visits_in_month
    from billing_period_overlap
    group by billing_period_id, first_day_of_month, last_day_of_month
),

billing_period_days as 
(
    select *, 
    case 
        when date_diff(minEnd, maxStart, day) + 1 < 0 then 0
        else date_diff(minEnd, maxStart, day) + 1
    end as billing_period_days_in_month,
    extract(day from last_day_of_month) as days_in_month
    from billing_period_overlap
), 

prorate_calculation as 
(
    select b.*, 
    v.visits_in_month, 
    b.billing_period_days_in_month/b.days_in_month as prorate_percent 
    from billing_period_days as b
    left join visits_in_overlap as v
    on b.billing_period_id=v.billing_period_id and b.first_day_of_month=v.first_day_of_month and b.last_day_of_month=v.last_day_of_month

), 

revenue_in_month as 
(
    select *, 
    episode_ongoing_revenue * prorate_percent as ongoing_revenue_in_month 
    from prorate_calculation
), 

revenue_per_visit as 
(
    select *,
    ongoing_revenue_in_month/NULLIF(visits_in_month,0) as ongoing_revenue, 
    episode_closed_revenue/NULLIF(total_completed_visits,0) as closed_revenue
    from revenue_in_month
),

fct_episodic_visits as
(
select * from revenue_per_visit
where payment_model = 'Episodic Payment'
),

rate_card_visits_report as (
        select
            cast(mrn as string) as mrn,
            unique_visit_id as unique_id,
            episode_start_date,
            episode_end_date,
            episode_identifier,
            cast(null as date) as billing_period_start_date,
            cast(null as date) as billing_period_end_date,
            cast(visit_date as date) as revenue_date,
            cast(null as string) as billing_period_id,
            cast(revenue_per_visit as float64) as revenue,
            -- revenue_per_visit,
            cast(null as float64) as closed_revenue,
            cast(null as float64) as ongoing_revenue,
            payment_model,
            parent_insurance,
            insurance_type,
            insurance,
            agency,
            updated_at,
            cast(null as int) as billing_period_days_in_month,
            cast(null as float64) as prorate_percent,
            cast(null as int) as total_scheduled_visits,
            cast(null as int) as total_completed_visits,
            cast(null as int) as lupa_level,
            cast(null as date) as eoe_date,
            task_name,
            status,
            cast(visit_date as date) as visit_date,
            task_type,
            cast(total_hours as float64) as hours_worked,
            visit_type,
            task_group,
            discipline_type,
            rate_type,
            rate
        from fct_rate_card_visits
    ),

    episodic_visits_report as (
        select
            cast(mrn as string) as mrn,
            unique_visit_id as unique_id,
            episode_start_date,
            episode_end_date,
            episode_identifier,
            billing_period_start_date,
            billing_period_end_date,
            cast(visit_date as date) as revenue_date,
            billing_period_id,
            case
                when closed_revenue is not null then closed_revenue else ongoing_revenue
            end as revenue,
            -- cast(closed_revenue as FLOAT64) as revenue,
            -- cast(null as FLOAT64) as revenue_per_visit,
            closed_revenue,
            ongoing_revenue,
            payment_model,
            parent_insurance,
            insurance_type,
            insurance,
            agency,
            updated_at,
            billing_period_days_in_month,
            prorate_percent,
            total_scheduled_visits,
            total_completed_visits,
            lupa_level,
            eoe_date,
            task_name,
            status,
            cast(visit_date as date) as visit_date,
            task_type,
            cast(total_hours as float64) as hours_worked,
            visit_type,
            task_group,
            discipline_type,
            cast(null as string) as rate_type,
            cast(null as float64) as rate
        from fct_episodic_visits
    ),

    final_revenue_report as (
        select *
        from rate_card_visits_report
        union all
        select *
        from episodic_visits_report
    ),

    add_pdgm_non_pdgm as (
        select c.*, ifnull(p.pdgm_non_pdgm, 'Non PDGM') pdgm_non_pdgm
        from final_revenue_report c
        left join
            (
                select *
                from pathwell-datawarehouse.transformed.dim_pdgm_non_pdgm_insurance
                where pdgm_non_pdgm = 'PDGM'
            ) p
            on c.agency = p.office
            and c.insurance = p.insurance
    ),

    add_exceptions as (
        select r.*, e.new_revenue
        from add_pdgm_non_pdgm r
        left join
            pathwell-datawarehouse.transformed.dim_revenue_report_exceptions e on r.unique_id = e.unique_id
    ),

fct_revenue_report as
(
select
    mrn,
    unique_id,
    episode_start_date,
    episode_end_date,
    episode_identifier,
    billing_period_start_date,
    billing_period_end_date,
    revenue_date,
    billing_period_id,
    if(revenue is null and new_revenue is not null, new_revenue, revenue) as revenue,
    closed_revenue,
    ongoing_revenue,
    payment_model,
    parent_insurance,
    insurance_type,
    insurance,
    agency,
    updated_at,
    billing_period_days_in_month,
    prorate_percent,
    total_scheduled_visits,
    total_completed_visits,
    lupa_level,
    eoe_date,
    task_name,
    status,
    visit_date,
    task_type,
    hours_worked,
    visit_type,
    task_group,
    discipline_type,
    rate_type,
    rate,
    pdgm_non_pdgm
from add_exceptions
),

billed_claim_report_first_pass_pre as
(
   select * from   (select * from pathwell-datawarehouse.raw.billed_claim_reports where date(updated_at) < ('{selected_date_1}'))
   where claim_status not in ('Void', 'Claim Cancelled', 'Auto-Cancelled')
),


billed_claim_report_first_pass as
(
   select * from  billed_claim_report_first_pass_pre
   where concat(unique_claim_id, anticipated_payment) not in (
                                    select concat(unique_claim_id, anticipated_payment) from billed_claim_report_first_pass_pre
                                    where tob = '322' and billing_period_start_date = billing_period_end_date and anticipated_payment = 0
                                 )  
),


cte as (select c.*, 
               r.revenue as expected_revenue,
               r.visit_date,
               case 
                   when row_number() over(partition by r.visit_date,
                                                       c.mrn,
                                                       c.agency
                                          order by c.billed_date,
                                                   c.updated_at desc) = 1
                   then 1
                   when visit_date is null then 1 
                   else 0
               end as check2 
               
       from billed_claim_report_first_pass c left join   fct_revenue_report r 
       on c.mrn = r.mrn and
          c.agency = r.agency and
          r.visit_date >= c.billing_period_start_date and
          r.visit_date <= c.billing_period_end_date 
),

cte1 as 
(
select billed_date,
       patient_name,
       mrn,
       claim_number,
       claim_status,
       cast(billing_period_start_date as date) billing_period_start_date,
       cast(billing_period_end_date as date) billing_period_end_date,
       tob,
       insurance,
       payer_type,
       branch,
       medicare_subscriber_number,
       anticipated_payment,
       agency,
       updated_at,
       unique_claim_id,
       sum(expected_revenue) expected_revenue
from cte where check2 = 1
group by billed_date,
       patient_name,
       mrn,
       claim_number,
       claim_status,
       billing_period_start_date,
       billing_period_end_date,
       tob,
       insurance,
       payer_type,
       branch,
       medicare_subscriber_number,
       anticipated_payment,
       agency,
       updated_at,
       unique_claim_id
),

find_duplicates as
(
   select * from (select *,
                         row_number() over (partition by unique_claim_id order by updated_at desc) rn
                  FROM cte1)
   where rn = 1
),

stg_billed_claim_report as(select c.*,
                                im.parent_insurance,
                                im.insurance_type
                         from find_duplicates c
                         left join pathwell-datawarehouse.transformed.dim_insurance_mapping im
                         on c.insurance = im.insurance and
                            c.agency = im.office
),

cteun as (
        select
            s.*,
            bc.billing_period_start_date bs,
            bc.billing_period_end_date be,
            bc.expected_revenue
        from fct_revenue_report s
        left join
            stg_billed_claim_report bc
            on s.mrn = bc.mrn
            and s.agency = bc.agency
        where bc.anticipated_payment is not null
    ),

    cte1un as (
        select
            *,
            case
                when visit_date >= bs and visit_date <= be then 'billed' else 'unbilled'
            end as check
        from cteun
    ),

    cte2un as (
        select *, row_number() over (partition by unique_id) rn
        from cte1un
        where check = 'billed'
    ),

    cte3un as (
        select * from cte2un where rn = 1  -- billed 
    ),

    cte4un as (
        select *
        from fct_revenue_report
        where unique_id not in (select unique_id from cte3un)
    ),

    cte5un as (
        select
            mrn,
            unique_id,
            episode_start_date,
            episode_end_date,
            episode_identifier,
            billing_period_start_date,
            billing_period_end_date,
            revenue_date,
            billing_period_id,
            revenue,
            closed_revenue,
            ongoing_revenue,
            payment_model,
            parent_insurance,
            insurance_type,
            insurance,
            agency,
            updated_at,
            billing_period_days_in_month,
            prorate_percent,
            total_scheduled_visits,
            total_completed_visits,
            lupa_level,
            eoe_date,
            task_name,
            status,
            visit_date,
            task_type,
            hours_worked,
            visit_type,
            task_group,
            discipline_type,
            rate_type,
            rate,
            pdgm_non_pdgm,
            check
        from cte3un
        union all
        select *, null
        from cte4un
    ),
    cte6un as (
        select
            mrn,
            unique_id,
            episode_start_date,
            episode_end_date,
            episode_identifier,
            billing_period_start_date,
            billing_period_end_date,
            revenue_date,
            billing_period_id,
            revenue,
            closed_revenue,
            ongoing_revenue,
            payment_model,
            parent_insurance,
            insurance_type,
            insurance,
            agency,
            updated_at,
            billing_period_days_in_month,
            prorate_percent,
            total_scheduled_visits,
            total_completed_visits,
            lupa_level,
            eoe_date,
            task_name,
            status,
            visit_date,
            task_type,
            hours_worked,
            visit_type,
            task_group,
            discipline_type,
            rate_type,
            rate,
            revenue as unbilled_ar,
            pdgm_non_pdgm
        from cte5un
        where
            check is null
            and insurance not in ('Private Pay1', 'Private Pay', '10 PRIVATE PAY')
    )

select *
from cte6un
"""

data = pandas_gbq.read_gbq(query, project_id=project_id)
data['unbilled_ar'] = data['unbilled_ar'].round()

start_date = st.date_input('Start date', data['visit_date'].min())
end_date = st.date_input('End date', data['visit_date'].max())
data = data[(data['visit_date'] >= start_date) & (data['visit_date'] <= end_date)]

# st.dataframe(data)

total_unbilled_ar = data['unbilled_ar'].sum()
st.markdown(
    f"<div style='border: 1px solid black; padding: 10px; background-color: #f0f0f0;'>"
    f"<p style='font-size: 18px; font-weight: bold;'>Total Unbilled AR:</p>"
    f"<p style='font-size: 24px; font-weight: bold;'>{total_unbilled_ar}</p>"
    f"</div>",
    unsafe_allow_html=True
    )




# Calculate the days difference
filtered_data = data
filtered_data['visit_date'] = pd.to_datetime(data['visit_date'])
filtered_data['days_difference'] = (pd.Timestamp(date.today()) - filtered_data['visit_date']).dt.days

# Perform the calculations based on the filtered data
filtered_data['0 to 30'] = filtered_data.apply(lambda row: row['unbilled_ar'] if 0 <= row['days_difference'] <= 30 else 0, axis=1)
filtered_data['31 to 60'] = filtered_data.apply(lambda row: row['unbilled_ar'] if 31 <= row['days_difference'] <= 60 else 0, axis=1)
filtered_data['61 to 90'] = filtered_data.apply(lambda row: row['unbilled_ar'] if 61 <= row['days_difference'] <= 90 else 0, axis=1)
filtered_data['91 to 120'] = filtered_data.apply(lambda row: row['unbilled_ar'] if 91 <= row['days_difference'] <= 120 else 0, axis=1)
filtered_data['121 to 180'] = filtered_data.apply(lambda row: row['unbilled_ar'] if 121 <= row['days_difference'] <= 180 else 0, axis=1)
filtered_data['181 to 240'] = filtered_data.apply(lambda row: row['unbilled_ar'] if 181 <= row['days_difference'] <= 240 else 0, axis=1)
filtered_data['241 to 300'] = filtered_data.apply(lambda row: row['unbilled_ar'] if 241 <= row['days_difference'] <= 300 else 0, axis=1)
filtered_data['301 to 365'] = filtered_data.apply(lambda row: row['unbilled_ar'] if 301 <= row['days_difference'] <= 365 else 0, axis=1)
filtered_data['> 365'] = filtered_data.apply(lambda row: row['unbilled_ar'] if row['days_difference'] > 365 else 0, axis=1)

# Group by 'agency' and sum the calculated columns
agging_agency = filtered_data.groupby(['agency']).sum()[['0 to 30', '31 to 60', '61 to 90',
                                                  '91 to 120', '121 to 180', '181 to 240',
                                                  '241 to 300', '301 to 365', '> 365']]

agging_agency.loc['Column_Total']= agging_agency.sum(numeric_only=True, axis=0)
agging_agency.loc[:,'Row_Total'] = agging_agency.sum(numeric_only=True, axis=1)

# Display the resulting aggregated data
st.dataframe(agging_agency)

# Group by 'agency' and sum the calculated columns
agging_insurance = filtered_data.groupby(['insurance']).sum()[['0 to 30', '31 to 60', '61 to 90',
                                                  '91 to 120', '121 to 180', '181 to 240',
                                                  '241 to 300', '301 to 365', '> 365']]

agging_insurance.loc['Column_Total']= agging_insurance.sum(numeric_only=True, axis=0)
agging_insurance.loc[:,'Row_Total'] = agging_insurance.sum(numeric_only=True, axis=1)

# Display the resulting aggregated data
st.dataframe(agging_insurance)




# Download button
excel_file = pd.ExcelWriter("data.xlsx")  # Create an ExcelWriter object with the file path
filtered_data.to_excel(excel_file, index=False)  # Save the DataFrame to the ExcelWriter object
excel_file.save()  # Save the Excel file

# Read the saved file as bytes
with open("data.xlsx", "rb") as file:
    excel_data = file.read()

# Create a download link for the Excel file
b64_excel = base64.b64encode(excel_data).decode()
href_excel = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64_excel}" download="data.xlsx">Download Data (Excel)</a>'
st.markdown(href_excel, unsafe_allow_html=True)









































