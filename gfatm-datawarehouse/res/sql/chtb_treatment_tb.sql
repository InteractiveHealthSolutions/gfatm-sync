CREATE DEFINER=`gfatmdwuser`@`localhost` PROCEDURE `treatment_tb_script`()
BEGIN
DROP TABLE IF EXISTS fact_chtb_treatment_tb;
create table gfatm_dw.fact_chtb_treatment_tb (implementation_id int(11) NOT NULL,datetime_id bigint(21) NOT NULL, 
 location_id int(11) NOT NULL ,  tb_diagnosed decimal(10,2), tb_treatment_initiated decimal(10,2), percentage_tb_treatment_initiated decimal(10,2),
missed_tb_follow_up decimal(10,2), percentage_missed_tb_follow_up decimal(10,2), on_tb_treatment decimal(10,2), percentage_on_tb_treatment decimal(10,2),
cured decimal(10,2), percentage_cured decimal(10,2), treatment_completed decimal(10,2), percentage_treatment_completed decimal(10,2), died decimal(10,2),
percentage_died decimal(10,2), transfer_out decimal(10,2), percentage_transfer_out decimal(10,2), treatment_failure decimal(10,2), percentage_treatment_failure decimal(10,2),
lost_to_followUp decimal(10,2), percentage_lost_to_followup decimal(10,2)) engine=MYISAM default charset=latin1;

insert into gfatm_dw.fact_chtb_treatment_tb (datetime_id,implementation_id,location_id)(
select  datetime_id, implementation_id,  location_id from gfatm_dw.fact_encounter
where  encounter_type IN(51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,81,82,83,84,85,86,87,160) and location_id is not null  group by  datetime_id, location_id  order by location_id ASC);

update gfatm_dw.fact_chtb_treatment_tb  ttb set tb_diagnosed=(select count(*) from gfatm_dw.enc_childhood_tb_treatment_initiation t 
left join dim_datetime  as d on d.full_date = date(t.date_entered) 
where t.tb_patient='yes' and  t.date_entered=(Select max(date_entered) from gfatm_dw.enc_childhood_tb_treatment_initiation 
where patient_id=t.patient_id) and d.datetime_id= ttb.datetime_id and t.location_id= ttb.location_id and t.implementation_id= ttb.implementation_id and d.full_date>='2017-01-01' );


update gfatm_dw.fact_chtb_treatment_tb ttb set tb_treatment_initiated=(select count(*) from gfatm_dw.enc_childhood_tb_treatment_initiation t left join dim_datetime as d on d.full_date = date(t.date_entered) where t.tb_patient='yes' and t.treatment_initiated='yes' and   t.date_entered=(Select max(date_entered) from gfatm_dw.enc_childhood_tb_treatment_initiation 
where patient_id=t.patient_id) and d.datetime_id=ttb.datetime_id and t.location_id= ttb.location_id and t.implementation_id=ttb.implementation_id and d.full_date>='2017-01-01');

update gfatm_dw.fact_chtb_treatment_tb ttb set percentage_tb_treatment_initiated=(ttb.tb_treatment_initiated/ttb.tb_diagnosed)*100;

update gfatm_dw.fact_chtb_treatment_tb ttb set  missed_tb_follow_up =(
select count(*) from  gfatm_dw.enc_childhood_tb_treatment_initiation t
inner join gfatm_dw.enc_childhood_tb_tb_treatment_followup f 
on t.patient_id= f.patient_id and f.date_entered=(Select max(date_entered)from enc_childhood_tb_tb_treatment_followup where patient_id=f.patient_id)
left join dim_datetime as d on d.full_date = date (t.date_entered)   
where ((date(f.date_entered)<= curdate()- interval 2 month ) or (f.patient_id is null)) and t.date_entered=(Select max(date_entered) from gfatm_dw.enc_childhood_tb_treatment_initiation 
where patient_id=t.patient_id)
 and t.tb_patient='yes'  and d.datetime_id=ttb.datetime_id
 and t.location_id= ttb.location_id and t.implementation_id=ttb.implementation_id and d.full_date>='2017-01-01');	



update gfatm_dw.fact_chtb_treatment_tb ttb set percentage_missed_tb_follow_up=(ttb.missed_tb_follow_up/ttb.tb_treatment_initiated) * 100;


update gfatm_dw.fact_chtb_treatment_tb ttb set on_tb_treatment=
(select count(*) from gfatm_dw.enc_childhood_tb_treatment_initiation t
 inner join gfatm_dw.enc_childhood_tb_tb_treatment_followup f on t.patient_id=f.patient_id
 and f.date_entered=(Select max(date_entered)from enc_childhood_tb_tb_treatment_followup 
where patient_id=f.patient_id) 
left join gfatm_dw.enc_tb_end_of_followup e 
on t.patient_id=e.patient_id and e.date_entered=(Select max(date_entered) from gfatm_dw.enc_tb_end_of_followup 
where patient_id=e.patient_id) left join dim_datetime as d on d.full_date=date(t.date_entered) 
  where ((date(f.date_entered)>= curdate()- interval 2 month ) and (e.patient_id is null)) and t.tb_patient='yes'
and t.date_entered=(Select max(date_entered) from gfatm_dw.enc_childhood_tb_treatment_initiation 
where patient_id=t.patient_id)
 and d.datetime_id=ttb.datetime_id and t.location_id= ttb.location_id and t.implementation_id=ttb.implementation_id and d.full_date>='2017-01-01');


update gfatm_dw.fact_chtb_treatment_tb ttb set percentage_on_tb_treatment=(on_tb_treatment/tb_treatment_initiated)*100;

update gfatm_dw.fact_chtb_treatment_tb ttb set cured= (select count(*) from gfatm_dw.enc_childhood_tb_treatment_initiation t  inner join gfatm_dw.enc_end_of_followup e on t.patient_id=e.patient_id  and e.date_entered=(Select max(date_entered) from gfatm_dw.enc_end_of_followup 
where patient_id=e.patient_id)left join dim_datetime as d on d.full_date = date(t.date_entered) where e.treatment_outcome='CURE' and  t.tb_patient='yes' and t.date_entered=(Select max(date_entered) from gfatm_dw.enc_childhood_tb_treatment_initiation 
where patient_id=t.patient_id) and d.datetime_id=ttb.datetime_id and t.location_id= ttb.location_id and t.implementation_id=ttb.implementation_id and d.full_date>='2017-01-01');
update gfatm_dw.fact_chtb_treatment_tb ttb set percentage_cured=(cured/tb_treatment_initiated)*100;

update gfatm_dw.fact_chtb_treatment_tb ttb set Treatment_Completed= (select count(*) from gfatm_dw.enc_childhood_tb_treatment_initiation t  inner join gfatm_dw.enc_end_of_followup e on t.patient_id=e.patient_id and e.date_entered=(Select max(date_entered) from gfatm_dw.enc_end_of_followup 
where patient_id=e.patient_id) left join dim_datetime as d on d.full_date = date(t.date_entered) where e.treatment_outcome='treatment completed' and  t.tb_patient='yes' and t.date_entered=(Select max(date_entered) from gfatm_dw.enc_childhood_tb_treatment_initiation 
where patient_id=t.patient_id) and d.datetime_id=ttb.datetime_id and t.location_id= ttb.location_id and t.implementation_id=ttb.implementation_id and d.full_date>='2017-01-01');
update gfatm_dw.fact_chtb_treatment_tb ttb set percentage_treatment_completed=(Treatment_Completed/tb_treatment_initiated) *100;

update gfatm_dw.fact_chtb_treatment_tb ttb set died=(select count(*) from gfatm_dw.enc_childhood_tb_treatment_initiation t  inner join gfatm_dw.enc_end_of_followup e on t.patient_id=e.patient_id left join dim_datetime as d on d.full_date = date(t.date_entered) where e.treatment_outcome='died' and  t.tb_patient='yes'  and e.date_entered=(Select max(date_entered) from gfatm_dw.enc_end_of_followup 
where patient_id=e.patient_id) and t.date_entered=(Select max(date_entered) from gfatm_dw.enc_childhood_tb_treatment_initiation 
where patient_id=t.patient_id) and d.datetime_id=ttb.datetime_id and t.location_id= ttb.location_id and t.implementation_id=ttb.implementation_id and d.full_date>='2017-01-01');
update gfatm_dw.fact_chtb_treatment_tb ttb set percentage_died=(died/tb_treatment_initiated)*100;

update gfatm_dw.fact_chtb_treatment_tb ttb set transfer_out=(select count(*) from gfatm_dw.enc_childhood_tb_treatment_initiation t inner join gfatm_dw.enc_end_of_followup e  on t.patient_id=e.patient_id and  e.date_entered=(Select max(date_entered) from gfatm_dw.enc_end_of_followup 
where patient_id=e.patient_id) left join dim_datetime as d on d.full_date = date(t.date_entered) where e.treatment_outcome='transferred_out' and  t.tb_patient='yes' and t.date_entered=(Select max(date_entered) from gfatm_dw.enc_childhood_tb_treatment_initiation 
where patient_id=t.patient_id) and d.datetime_id=ttb.datetime_id and t.location_id= ttb.location_id and t.implementation_id=ttb.implementation_id and d.full_date>='2017-01-01');

update gfatm_dw.fact_chtb_treatment_tb ttb set percentage_transfer_out=(transfer_out/tb_treatment_initiated)*100;

update gfatm_dw.fact_chtb_treatment_tb ttb set treatment_failure=(select count(*) from gfatm_dw.enc_childhood_tb_treatment_initiation t  inner join gfatm_dw.enc_end_of_followup e on t.patient_id=e.patient_id and e.date_entered=(Select max(date_entered) from gfatm_dw.enc_end_of_followup 
where patient_id=e.patient_id) left join dim_datetime as d on d.full_date = date(t.date_entered) where e.treatment_outcome='tb_treatment_failure' and  t.tb_patient='yes' and t.date_entered=(Select max(date_entered) from gfatm_dw.enc_childhood_tb_treatment_initiation 
where patient_id=t.patient_id) and d.datetime_id=ttb.datetime_id and t.location_id= ttb.location_id and t.implementation_id=ttb.implementation_id and d.full_date>='2017-01-01');

update gfatm_dw.fact_chtb_treatment_tb ttb set percentage_treatment_failure =(treatment_failure/tb_treatment_initiated)*100;

update gfatm_dw.fact_chtb_treatment_tb ttb set lost_to_followUp=(select count(*) from gfatm_dw.enc_childhood_tb_treatment_initiation t inner join gfatm_dw.enc_end_of_followup e on e.patient_id=t.patient_id and e.date_entered=(Select max(date_entered) from gfatm_dw.enc_end_of_followup 
where patient_id=e.patient_id) left join dim_datetime as d on d.full_date = date(t.date_entered) where e.treatment_outcome='DEFAULT' and  t.tb_patient='yes' and t.date_entered=(Select max(date_entered) from gfatm_dw.enc_childhood_tb_treatment_initiation 
where patient_id=t.patient_id) and d.datetime_id=ttb.datetime_id and t.location_id= ttb.location_id and t.implementation_id=ttb.implementation_id and d.full_date>='2017-01-01');
update gfatm_dw.fact_chtb_treatment_tb ttb set  percentage_lost_to_followup=(lost_to_followUp/tb_treatment_initiated)*100;
END