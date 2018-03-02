DELIMITER $$
CREATE PROCEDURE `encounter_dimension`(in impl_id int, in date_from datetime, in date_to datetime)
BEGIN

insert ignore into dim_encounter 
select e.surrogate_id, e.implementation_id, e.encounter_id, e.encounter_type, et.name as encounter_name, et.description, e.patient_id, e.location_id, p.identifier as provider, e.encounter_datetime as date_entered, e.creator, e.date_created as date_start, e.changed_by, e.date_changed, e.date_created as date_end, e.uuid from encounter as e 
inner join encounter_type as et on et.implementation_id = e.implementation_id and et.encounter_type_id = e.encounter_type and et.retired = 0 
left outer join encounter_provider as ep on ep.implementation_id = e.implementation_id and ep.encounter_id = e.encounter_id and ep.voided = 0 
left outer join provider as p on p.implementation_id = e.implementation_id and p.provider_id = ep.provider_id and p.retired = 0 
where e.voided = 0 and not exists (select * from dim_encounter where implementation_id = e.implementation_id and encounter_id = e.encounter_id) 
and ((e.date_created between date_from and date_to) or (e.date_changed between date_from and date_to));

insert ignore into dim_obs 
select o.surrogate_id, o.implementation_id, e.encounter_id, e.encounter_type, e.patient_id, p.patient_identifier, e.provider, o.obs_id, o.obs_group_id, o.concept_id, c.short_name as question, obs_datetime, o.location_id, concat(ifnull(ifnull(ifnull(c2.short_name, c2.default_name), c2.full_name), ''), ifnull(o.value_datetime, ''), ifnull(o.value_numeric, ''), ifnull(o.value_text, '')) as answer, o.value_coded, o.value_datetime, o.value_numeric, o.value_text, o.creator, o.date_created, o.voided, o.uuid from obs as o 
inner join dim_concept as c on c.implementation_id = o.implementation_id and c.concept_id = o.concept_id 
inner join dim_encounter as e on e.implementation_id = o.implementation_id and e.encounter_id = o.encounter_id 
inner join dim_patient as p on p.implementation_id = e.implementation_id and p.patient_id = e.patient_id 
left outer join dim_concept as c2 on c2.implementation_id = o.implementation_id and c2.concept_id = o.value_coded 
where o.voided = 0 and not exists (select * from dim_obs where implementation_id = o.implementation_id and obs_id = o.obs_id) 
and (o.date_created between date_from and date_to);

drop table if exists tmp_group_obs;

-- Delete all past encounters from dimension, which are now voided
create table temp_voided select encounter_id from encounter where voided=1 and encounter_id in(select encounter_id from dim_encounter);

delete from dim_encounter where  encounter_id in ( select encounter_id from temp_voided);

drop table temp_voided;

-- Delete all past observations from dimension, which are now null
delete from dim_obs 
where  obs_id in 
	(select previous_version from obs where previous_version is not null);

create table tmp_group_obs 
select implementation_id, encounter_type, obs_group_id, question, 
group_concat(case answer when '' then null else answer end) as answer from dim_obs 
where implementation_id = impl_id and obs_group_id is not null 
group by implementation_id, obs_group_id, encounter_type 
having answer is not null;

update dim_obs as o, tmp_group_obs as t 
set o.answer = t.answer 
where o.implementation_id = t.implementation_id and o.obs_id = t.obs_group_id;

END$$
DELIMITER ;
