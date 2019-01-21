CREATE PROCEDURE `dim_modeling` (in impl_id int, in date_from datetime, in date_to datetime)
BEGIN

-- -- -- -- --
-- Concept
-- -- -- -- --
drop table if exists concept_latest_name;
create table concept_latest_name 
select c.implementation_id, c.concept_id, 
(select max(concept_name_id) from concept_name where implementation_id = c.implementation_id and concept_id = c.concept_id and locale = 'en' and voided = 0 and concept_name_type is null) as default_name, 
(select max(concept_name_id) from concept_name where implementation_id = c.implementation_id and concept_id = c.concept_id and locale = 'en' and voided = 0 and concept_name_type = 'SHORT') as short_name, 
(select max(concept_name_id) from concept_name where implementation_id = c.implementation_id and concept_id = c.concept_id and locale = 'en' and voided = 0 and concept_name_type = 'FULLY_SPECIFIED') as full_name from concept as c 
having concat(ifnull(default_name, ''), ifnull(short_name, ''), ifnull(full_name, '')) <> '';

alter table concept_latest_name add primary key composite_id (implementation_id, concept_id);

insert ignore into dim_concept (surrogate_id, implementation_id, concept_id, full_name, short_name, default_name, description, retired, data_type, class, hi_absolute, hi_critical, hi_normal, low_absolute, low_critical, low_normal, creator, date_created, version, changed_by, date_changed, uuid) 
select c.surrogate_id, c.implementation_id, c.concept_id, cnf.name as full_name, cns.name as short_name, cnd.name as default_name, d.description, c.retired, dt.name as data_type, cl.name as class, cn.hi_absolute, cn.hi_critical, cn.hi_normal, cn.low_absolute, cn.low_critical, cn.low_normal, c.creator, c.date_created, c.version, c.changed_by, c.date_changed, c.uuid from concept as c 
left outer join concept_datatype as dt on dt.implementation_id = c.implementation_id and dt.concept_datatype_id = c.datatype_id 
left outer join concept_class as cl on cl.implementation_id = c.implementation_id and cl.concept_class_id = c.class_id 
inner join concept_latest_name as nm on nm.implementation_id = c.implementation_id and nm.concept_id = c.concept_id 
left outer join concept_name as cnf on cnf.implementation_id = c.implementation_id and cnf.concept_name_id = nm.full_name 
left outer join concept_name as cns on cns.implementation_id = c.implementation_id and cns.concept_name_id = nm.short_name 
left outer join concept_name as cnd on cnd.implementation_id = c.implementation_id and cnd.concept_name_id = nm.default_name 
left outer join concept_description as d on d.implementation_id = c.implementation_id and d.concept_id = c.concept_id and d.locale = 'en' 
left outer join concept_numeric as cn on cn.implementation_id = c.implementation_id and cn.concept_id = c.concept_id 
where c.implementation_id = impl_id and not exists (select * from dim_concept where implementation_id = c.implementation_id and concept_id = c.concept_id);

update dim_concept set full_name = 'Yes', short_name = 'Yes', default_name = 'Yes' where concept_id = 1;
update dim_concept set full_name = 'No', short_name = 'No', default_name = 'No' where concept_id = 2;

-- -- -- -- --
-- Location
-- -- -- -- --
delete from dim_location where implementation_id = impl_id;

update location_attribute set value_reference = 'Yes' where value_reference = 'true';
update location_attribute set value_reference = 'No' where value_reference = 'false';

drop table if exists location_attribute_merged;

create table location_attribute_merged 
select a.implementation_id, a.location_id, 
group_concat(if(a.attribute_type_id = 1, a.value_reference, null)) as location_identifier, 
group_concat(if(a.attribute_type_id = 2, a.value_reference, null)) as primary_contact, 
group_concat(if(a.attribute_type_id = 3, a.value_reference, null)) as fast_location, 
group_concat(if(a.attribute_type_id = 4, a.value_reference, null)) as pmdt_location, 
group_concat(if(a.attribute_type_id = 5, a.value_reference, null)) as aic_location, 
group_concat(if(a.attribute_type_id = 6, a.value_reference, null)) as pet_location, 
group_concat(if(a.attribute_type_id = 7, a.value_reference, null)) as comorbidities_location, 
group_concat(if(a.attribute_type_id = 8, a.value_reference, null)) as childhoodtb_location, 
group_concat(if(a.attribute_type_id = 9, a.value_reference, null)) as location_type, 
group_concat(if(a.attribute_type_id = 10, a.value_reference, null)) as secondary_contact, 
group_concat(if(a.attribute_type_id = 11, a.value_reference, null)) as staff_time, 
group_concat(if(a.attribute_type_id = 12, a.value_reference, null)) as opd_timing, 
group_concat(if(a.attribute_type_id = 13, a.value_reference, null)) as status, 
group_concat(if(a.attribute_type_id = 14, a.value_reference, null)) as primary_contact_name, 
group_concat(if(a.attribute_type_id = 15, a.value_reference, null)) as secondary_contact_name,
group_concat(if(a.attribute_type_id = 16, a.value_reference, null)) as site_supervisor_system_id,
group_concat(if(a.attribute_type_id = 17, a.value_reference, null)) as ztts_location,
group_concat(if(a.attribute_type_id = 18, a.value_reference, null)) as doctor_visit_timing,
 '' as BLANK from location_attribute as a 
where a.voided = 0 and a.implementation_id = impl_id 
group by a.location_id;

insert into dim_location (surrogate_id, implementation_id, location_id, location_name, description, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, creator, date_created, retired, parent_location, uuid, location_identifier,primary_contact,fast_location,pmdt_location,aic_location,pet_location,comorbidities_location,childhoodtb_location,location_type,secondary_contact,staff_time,opd_timing,status,primary_contact_name,secondary_contact_name,site_supervisor_system_id,ztts_location,doctor_visit_timing) 
select l.surrogate_id, l.implementation_id, l.location_id, l.name as location_name, l.description, l.address1, l.address2, l.city_village, l.state_province, l.postal_code, l.country, l.latitude, l.longitude, l.creator, l.date_created, l.retired, l.parent_location, l.uuid, lam.location_identifier,lam.primary_contact,lam.fast_location,lam.pmdt_location,lam.aic_location,lam.pet_location,lam.comorbidities_location,lam.childhoodtb_location,lam.location_type,lam.secondary_contact,lam.staff_time,lam.opd_timing,lam.status,lam.primary_contact_name,lam.secondary_contact_name,lam.site_supervisor_system_id,lam.ztts_location,lam.doctor_visit_timing from location as l 
left outer join location_attribute_merged as lam using (implementation_id, location_id) 
where l.implementation_id = impl_id and l.surrogate_id not in 
	(select surrogate_id from dim_location where implementation_id = l.implementation_id);

-- -- -- -- --
-- User
-- -- -- -- --
drop table if exists user_role_merged;

create table user_role_merged 
select a.implementation_id, a.user_id, 
group_concat(if(a.role = 'Anonymous', 'Yes', null)) as anonymous, 
group_concat(if(a.role = 'Authenticated', 'Yes', null)) as authenticated, 
group_concat(if(a.role = 'Call Center Agent', 'Yes', null)) as call_center_agent, 
group_concat(if(a.role = 'ChildhoodTB Lab Technician', 'Yes', null)) as childhoodtb_lab_technician, 
group_concat(if(a.role = 'ChildhoodTB Medical Officer', 'Yes', null)) as childhoodtb_medical_officer, 
group_concat(if(a.role = 'ChildhoodTB Monitor', 'Yes', null)) as childhoodtb_monitor, 
group_concat(if(a.role = 'ChildhoodTB Nurse', 'Yes', null)) as childhoodtb_nurse, 
group_concat(if(a.role = 'ChildhoodTB Program Assistant', 'Yes', null)) as childhoodtb_program_assistant, 
group_concat(if(a.role = 'ChildhoodTB Program Manager', 'Yes', null)) as childhoodtb_program_manager, 
group_concat(if(a.role = 'Clinical Coordinator', 'Yes', null)) as clinical_coordinator, 
group_concat(if(a.role = 'Clinician', 'Yes', null)) as clinician, 
group_concat(if(a.role = 'Community Health Services', 'Yes', null)) as community_health_services, 
group_concat(if(a.role = 'Comorbidities Associate Diabetologist', 'Yes', null)) as comorbidities_associate_diabetologist, 
group_concat(if(a.role = 'Comorbidities Counselor', 'Yes', null)) as comorbidities_counselor, 
group_concat(if(a.role = 'Comorbidities Diabetes Educator', 'Yes', null)) as comorbidities_diabetes_educator, 
group_concat(if(a.role = 'Comorbidities Eye Screener', 'Yes', null)) as comorbidities_eye_screener, 
group_concat(if(a.role = 'Comorbidities Foot Screener', 'Yes', null)) as comorbidities_foot_screener, 
group_concat(if(a.role = 'Comorbidities Health Worker', 'Yes', null)) as comorbidities_health_worker, 
group_concat(if(a.role = 'Comorbidities Program Manager', 'Yes', null)) as comorbidities_program_manager, 
group_concat(if(a.role = 'Comorbidities Psychologist', 'Yes', null)) as comorbidities_psychologist, 
group_concat(if(a.role = 'Counselor', 'Yes', null)) as counselor, 
group_concat(if(a.role = 'Data Entry Operator', 'Yes', null)) as data_entry_operator, 
group_concat(if(a.role = 'Diabetes Educator', 'Yes', null)) as diabetes_educator, 
group_concat(if(a.role = 'Facility DOT Provider', 'Yes', null)) as facility_dot_provider, 
group_concat(if(a.role = 'FAST Facilitator', 'Yes', null)) as fast_facilitator, 
group_concat(if(a.role = 'FAST Field Supervisor', 'Yes', null)) as fast_field_supervisor, 
group_concat(if(a.role = 'FAST Lab Technician', 'Yes', null)) as fast_lab_technician, 
group_concat(if(a.role = 'FAST Manager', 'Yes', null)) as fast_manager, 
group_concat(if(a.role = 'FAST Program Manager', 'Yes', null)) as fast_program_manager, 
group_concat(if(a.role = 'FAST Screener', 'Yes', null)) as fast_screener, 
group_concat(if(a.role = 'FAST Site Manager', 'Yes', null)) as fast_site_manager, 
group_concat(if(a.role = 'Field Supervisor', 'Yes', null)) as field_supervisor, 
group_concat(if(a.role = 'Health Worker', 'Yes', null)) as health_worker, 
group_concat(if(a.role = 'Implementer', 'Yes', null)) as implementer, 
group_concat(if(a.role = 'Lab Technician', 'Yes', null)) as lab_technician, 
group_concat(if(a.role = 'Medical Officer', 'Yes', null)) as medical_officer, 
group_concat(if(a.role = 'Monitor', 'Yes', null)) as monitor, 
group_concat(if(a.role = 'PET Clinician', 'Yes', null)) as pet_clinician, 
group_concat(if(a.role = 'PET Field Supervisor', 'Yes', null)) as pet_field_supervisor, 
group_concat(if(a.role = 'PET Health Worker', 'Yes', null)) as pet_health_worker, 
group_concat(if(a.role = 'PET Program Manager', 'Yes', null)) as pet_program_manager, 
group_concat(if(a.role = 'PET Psychologist', 'Yes', null)) as pet_psychologist, 
group_concat(if(a.role = 'PMDT Diabetes Educator', 'Yes', null)) as pmdt_diabetes_educator, 
group_concat(if(a.role = 'PMDT Lab Technician', 'Yes', null)) as pmdt_lab_technician, 
group_concat(if(a.role = 'PMDT Program Manager', 'Yes', null)) as pmdt_program_manager, 
group_concat(if(a.role = 'PMDT Treatment Coordinator', 'Yes', null)) as pmdt_treatment_coordinator, 
group_concat(if(a.role = 'PMDT Treatment Supporter', 'Yes', null)) as pmdt_treatment_supporter, 
group_concat(if(a.role = 'Program Manager', 'Yes', null)) as program_manager, 
group_concat(if(a.role = 'Provider', 'Yes', null)) as provider, 
group_concat(if(a.role = 'Psychologist', 'Yes', null)) as psychologist, 
group_concat(if(a.role = 'Referral Site Coordinator', 'Yes', null)) as referral_site_coordinator, 
group_concat(if(a.role = 'Screener', 'Yes', null)) as screener, 
group_concat(if(a.role = 'System Developer', 'Yes', null)) as system_developer, 
group_concat(if(a.role = 'Treatment Coordinator', 'Yes', null)) as treatment_coordinator, 
group_concat(if(a.role = 'Treatment Supporter', 'Yes', null)) as treatment_supporter, '' as BLANK from user_role as a 
where a.implementation_id = impl_id 
group by a.user_id;

alter table user_role_merged add primary key (implementation_id, user_id);

truncate dim_user;

insert into dim_user (surrogate_id, implementation_id, user_id, username, person_id, identifier, secret_question, secret_answer, creator, date_created, changed_by, date_changed, retired, retire_reason, uuid, anonymous,authenticated,call_center_agent,childhoodtb_lab_technician,childhoodtb_medical_officer,childhoodtb_monitor,childhoodtb_nurse,childhoodtb_program_assistant,childhoodtb_program_manager,clinical_coordinator,clinician,community_health_services,comorbidities_associate_diabetologist,comorbidities_counselor,comorbidities_diabetes_educator,comorbidities_eye_screener,comorbidities_foot_screener,comorbidities_health_worker,comorbidities_program_manager,comorbidities_psychologist,counselor,data_entry_operator,diabetes_educator,facility_dot_provider,fast_facilitator,fast_field_supervisor,fast_lab_technician,fast_manager,fast_program_manager,fast_screener,fast_site_manager,field_supervisor,health_worker,implementer,lab_technician,medical_officer,monitor,pet_clinician,pet_field_supervisor,pet_health_worker,pet_program_manager,pet_psychologist,pmdt_diabetes_educator,pmdt_lab_technician,pmdt_program_manager,pmdt_treatment_coordinator,pmdt_treatment_supporter,program_manager,provider,psychologist,referral_site_coordinator,screener,system_developer,treatment_coordinator,treatment_supporter) 
select u.surrogate_id, u.implementation_id, u.user_id, ifnull(u.username, '') as username, u.person_id, p.identifier, u.secret_question, pa1.value_reference as intervention, u.creator, u.date_created, u.changed_by, u.date_changed, u.retired, u.retire_reason, u.uuid, urm.anonymous,urm.authenticated,urm.call_center_agent,urm.childhoodtb_lab_technician,urm.childhoodtb_medical_officer,urm.childhoodtb_monitor,urm.childhoodtb_nurse,urm.childhoodtb_program_assistant,urm.childhoodtb_program_manager,urm.clinical_coordinator,urm.clinician,urm.community_health_services,urm.comorbidities_associate_diabetologist,urm.comorbidities_counselor,urm.comorbidities_diabetes_educator,urm.comorbidities_eye_screener,urm.comorbidities_foot_screener,urm.comorbidities_health_worker,urm.comorbidities_program_manager,urm.comorbidities_psychologist,urm.counselor,urm.data_entry_operator,urm.diabetes_educator,urm.facility_dot_provider,urm.fast_facilitator,urm.fast_field_supervisor,urm.fast_lab_technician,urm.fast_manager,urm.fast_program_manager,urm.fast_screener,urm.fast_site_manager,urm.field_supervisor,urm.health_worker,urm.implementer,urm.lab_technician,urm.medical_officer,urm.monitor,urm.pet_clinician,urm.pet_field_supervisor,urm.pet_health_worker,urm.pet_program_manager,urm.pet_psychologist,urm.pmdt_diabetes_educator,urm.pmdt_lab_technician,urm.pmdt_program_manager,urm.pmdt_treatment_coordinator,urm.pmdt_treatment_supporter,urm.program_manager,urm.provider,urm.psychologist,urm.referral_site_coordinator,urm.screener,urm.system_developer,urm.treatment_coordinator,urm.treatment_supporter from users as u 
left outer join provider as p on p.implementation_id = u.implementation_id and p.provider_id = (select max(provider_id) from provider where person_id = u.person_id) 
left outer join provider_attribute as pa1 on pa1.implementation_id = u.implementation_id and pa1.provider_id = p.provider_id and pa1.attribute_type_id = 1 and pa1.voided = 0 
left outer join user_role_merged as urm on urm.implementation_id = u.implementation_id and urm.user_id = u.user_id 
where u.implementation_id = impl_id;

-- Remove duplicate providers
create table if not exists clean_provider 
select * from provider 
where provider_id in (select min(provider_id) as provider_id from provider where retired = 0 group by identifier);

truncate provider;
insert into provider 
select * from clean_provider;

drop table clean_provider;

-- -- -- -- --
-- Patient
-- -- -- -- --
drop table if exists person_latest_name;

create table person_latest_name 
select * from person_name as a 
where a.person_name_id = 
	(select max(person_name_id) from person_name 
		where implementation_id = a.implementation_id and person_id = a.person_id and preferred = 1);

alter table person_latest_name add primary key surrogate_id (surrogate_id), add index person_index (person_id, person_name_id);

drop table if exists person_latest_address;

create table person_latest_address
select * from person_address as a 
where a.person_address_id = 
	(select max(person_address_id) from person_address
		where implementation_id = a.implementation_id and person_id = a.person_id and preferred = 1);

alter table person_latest_address add primary key surrogate_id (surrogate_id), add index person_index (person_id, person_address_id);


drop table if exists patient_latest_identifier;

create table patient_latest_identifier 
select implementation_id, patient_id, identifier_type, identifier, max(patient_identifier_id) as max_patient_identifier_id from patient_identifier 
where implementation_id = impl_id and voided = 0 
group by implementation_id, patient_id, identifier_type;

alter table patient_latest_identifier add index identifier_id_index (patient_id, identifier_type, max_patient_identifier_id);

drop table if exists person_attribute_merged;

create table person_attribute_merged 
select a.implementation_id, a.person_id, 
group_concat(if(a.person_attribute_type_id = 1, a.value, null)) as race, 
group_concat(if(a.person_attribute_type_id = 2, a.value, null)) as birthplace, 
group_concat(if(a.person_attribute_type_id = 3, a.value, null)) as citizenship, 
group_concat(if(a.person_attribute_type_id = 4, a.value, null)) as mother_name, 
group_concat(if(a.person_attribute_type_id = 5, a.value, null)) as marital_status, 
group_concat(if(a.person_attribute_type_id = 6, a.value, null)) as health_district, 
group_concat(if(a.person_attribute_type_id = 7, a.value, null)) as health_center, 
group_concat(if(a.person_attribute_type_id = 8, a.value, null)) as primary_contact, 
group_concat(if(a.person_attribute_type_id = 9, a.value, null)) as unknown_patient, 
group_concat(if(a.person_attribute_type_id = 10, a.value, null)) as test_patient, 
group_concat(if(a.person_attribute_type_id = 11, a.value, null)) as primary_contact_owner, 
group_concat(if(a.person_attribute_type_id = 12, a.value, null)) as secondary_contact, 
group_concat(if(a.person_attribute_type_id = 13, a.value, null)) as secondary_contact_owner, 
group_concat(if(a.person_attribute_type_id = 14, a.value, null)) as ethnicity, 
group_concat(if(a.person_attribute_type_id = 15, a.value, null)) as education_level, 
group_concat(if(a.person_attribute_type_id = 16, a.value, null)) as employment_status, 
group_concat(if(a.person_attribute_type_id = 17, a.value, null)) as occupation, 
group_concat(if(a.person_attribute_type_id = 18, a.value, null)) as mother_tongue, 
group_concat(if(a.person_attribute_type_id = 19, a.value, null)) as income_class, 
group_concat(if(a.person_attribute_type_id = 20, a.value, null)) as national_id, 
group_concat(if(a.person_attribute_type_id = 21, a.value, null)) as national_id_owner, 
group_concat(if(a.person_attribute_type_id = 22, a.value, null)) as guardian_name, 
group_concat(if(a.person_attribute_type_id = 23, a.value, null)) as tertiary_contact, 
group_concat(if(a.person_attribute_type_id = 24, a.value, null)) as quaternary_contact, 
group_concat(if(a.person_attribute_type_id = 25, a.value, null)) as treatment_supporter, 
group_concat(if(a.person_attribute_type_id = 26, a.value, null)) as other_identification_number, 
group_concat(if(a.person_attribute_type_id = 27, a.value, null)) as transgender, 
group_concat(if(a.person_attribute_type_id = 28, a.value, null)) as patient_type, 
group_concat(if(a.person_attribute_type_id = 29, a.value, null)) as email_address, 
'' as BLANK from person_attribute as a 
where a.voided = 0 
group by a.implementation_id, a.person_id;

alter table person_attribute_merged add primary key (implementation_id, person_id);

delete FROM gfatm_dw.dim_patient where patient_id in(select patient_id from patient where date_changed between date_from and date_to) ;

insert ignore into dim_patient (surrogate_id, implementation_id, patient_id, patient_identifier, enrs, external_id, gender, birthdate, birthdate_estimated, dead, first_name, middle_name, last_name, address1, address2, city_village, state_province, postal_code, country, creator, date_created, changed_by, date_changed, voided, uuid, race,birthplace,citizenship,mother_name,marital_status,health_district,health_center,primary_contact,unknown_patient,test_patient,primary_contact_owner,secondary_contact,secondary_contact_owner,ethnicity,education_level,employment_status,occupation,mother_tongue,income_class,national_id,national_id_owner,guardian_name,tertiary_contact,quaternary_contact,treatment_supporter,other_identification_number,transgender,patient_type) 
select p.surrogate_id, p.implementation_id, p.patient_id, pid.identifier as patient_identifier, enrs.identifier as enrs, eid.identifier as external_id, pr.gender, pr.birthdate, pr.birthdate_estimated, pr.dead, n.given_name as first_name, n.middle_name, n.family_name as last_name,  ad.address1, ad.address2, ad.city_village, ad.state_province, ad.postal_code, ad.country, p.creator, p.date_created, p.changed_by, p.date_changed, p.voided, pr.uuid, pam.race,pam.birthplace,pam.citizenship,pam.mother_name,pam.marital_status,pam.health_district,pam.health_center,pam.primary_contact,pam.unknown_patient,pam.test_patient,pam.primary_contact_owner,pam.secondary_contact,pam.secondary_contact_owner,pam.ethnicity,pam.education_level,pam.employment_status,pam.occupation,pam.mother_tongue,pam.income_class,pam.national_id,pam.national_id_owner,pam.guardian_name,pam.tertiary_contact,pam.quaternary_contact,pam.treatment_supporter,pam.other_identification_number,pam.transgender,pam.patient_type from patient as p 
inner join person as pr on pr.implementation_id = p.implementation_id and pr.person_id = p.patient_id 
inner join patient_latest_identifier as pid on pid.implementation_id = p.implementation_id and pid.patient_id = p.patient_id and pid.identifier_type = 3 
left outer join patient_latest_identifier as enrs on enrs.implementation_id = p.implementation_id and enrs.patient_id = p.patient_id and enrs.identifier_type = 4 
left outer join patient_latest_identifier as eid on eid.implementation_id = p.implementation_id and eid.patient_id = p.patient_id and eid.identifier_type = 5 
inner join person_latest_name as n on n.implementation_id = p.implementation_id and n.person_id = pr.person_id and n.preferred = 1 
left outer join person_latest_address as ad on ad.implementation_id = p.implementation_id and ad.person_id = pr.person_id and ad.preferred = 1 
left outer join person_attribute_merged as pam on pam.implementation_id = p.implementation_id and pam.person_id = p.patient_id 
where p.implementation_id = impl_id and p.voided = 0 
	and not exists (select * from dim_patient where implementation_id = p.implementation_id and patient_id = p.patient_id)
    and ((p.date_created between date_from and date_to) or (p.date_changed between date_from and date_to));

drop table if exists tmp_dim_patient;

create table  tmp_dim_patient 
select p.surrogate_id, p.implementation_id, p.patient_id, pid.identifier as patient_identifier, enrs.identifier as enrs, eid.identifier as external_id, pr.gender, pr.birthdate, pr.birthdate_estimated, pr.dead, n.given_name as first_name, n.middle_name, n.family_name as last_name,  ad.address1, ad.address2, ad.city_village, ad.state_province, ad.postal_code, ad.country, p.creator, p.date_created, p.changed_by, p.date_changed, p.voided, pr.uuid, pam.race,pam.birthplace,pam.citizenship,pam.mother_name,pam.marital_status,pam.health_district,pam.health_center,pam.primary_contact,pam.unknown_patient,pam.test_patient,pam.primary_contact_owner,pam.secondary_contact,pam.secondary_contact_owner,pam.ethnicity,pam.education_level,pam.employment_status,pam.occupation,pam.mother_tongue,pam.income_class,pam.national_id,pam.national_id_owner,pam.guardian_name,pam.tertiary_contact,pam.quaternary_contact,pam.treatment_supporter,pam.other_identification_number,pam.transgender,pam.patient_type from patient as p 
inner join person as pr on pr.implementation_id = p.implementation_id and pr.person_id = p.patient_id 
inner join patient_latest_identifier as pid on pid.implementation_id = p.implementation_id and pid.patient_id = p.patient_id and pid.identifier_type = 3 
left outer join patient_latest_identifier as enrs on enrs.implementation_id = p.implementation_id and enrs.patient_id = p.patient_id and enrs.identifier_type = 4 
left outer join patient_latest_identifier as eid on eid.implementation_id = p.implementation_id and eid.patient_id = p.patient_id and eid.identifier_type = 5 
inner join person_latest_name as n on n.implementation_id = p.implementation_id and n.person_id = pr.person_id and n.preferred = 1 
left outer join person_latest_address as ad on ad.implementation_id = p.implementation_id and ad.person_id = pr.person_id and ad.preferred = 1 
left outer join person_attribute_merged as pam on pam.implementation_id = p.implementation_id and pam.person_id = p.patient_id 
where p.implementation_id = impl_id and p.voided = 0 
	and  ((p.date_changed between date_from and date_to) or (n.date_changed between date_from and date_to) or (ad.date_changed between date_from and date_to));

update dim_patient as dp, tmp_dim_patient as tp 
set 
    dp.patient_identifier= tp.patient_identifier,
    dp.enrs = tp.enrs,
    dp.external_id = tp.external_id,
    dp.gender = tp.gender,
    dp.birthdate = tp.birthdate,
    dp.birthdate_estimated = tp.birthdate_estimated,
    dp.dead = tp.dead,
    dp.first_name = tp.first_name,
    dp.middle_name= tp.middle_name,
    dp.last_name= tp.last_name,
    dp.address1= tp.address1,
    dp.address2= tp.address2,
    dp.city_village= tp.city_village,
    dp.state_province= tp.state_province,
    dp.postal_code= tp.postal_code,
    dp.country= tp.country,
    dp.creator= tp.creator,
    dp.date_created= tp.date_created,
    dp.changed_by= tp.changed_by,
    dp.date_changed= tp.date_changed,
    dp.voided= tp.voided,
    dp.uuid= tp.uuid,
    dp.race= tp.race,
    dp.birthplace= tp.birthplace,
    dp.citizenship= tp.citizenship,
    dp.mother_name= tp.mother_name,
    dp.marital_status= tp.marital_status,
    dp.health_district= tp.health_district,
    dp.health_center= tp.health_center,
    dp.primary_contact= tp.primary_contact,
    dp.unknown_patient= tp.unknown_patient,
    dp.test_patient= tp.test_patient,
    dp.primary_contact_owner= tp.primary_contact_owner,
    dp.secondary_contact= tp.secondary_contact,
    dp.secondary_contact_owner= tp.secondary_contact_owner,
    dp.ethnicity= tp.ethnicity,
    dp.education_level= tp.education_level,
    dp.employment_status= tp.employment_status,
    dp.occupation= tp.occupation,
    dp.mother_tongue= tp.mother_tongue,
    dp.income_class= tp.income_class,
    dp.national_id= tp.national_id,
    dp.national_id_owner= tp.national_id_owner,
    dp.guardian_name= tp.guardian_name,
    dp.tertiary_contact= tp.tertiary_contact,
    dp.quaternary_contact= tp.quaternary_contact,
    dp.treatment_supporter= tp.treatment_supporter,
    dp.other_identification_number= tp.other_identification_number,
    dp.transgender= tp.transgender,
    dp.patient_type= tp.patient_type
where dp.implementation_id = tp.implementation_id and dp.patient_id=tp.patient_id ;

-- -- -- -- --
-- Encounter
-- -- -- -- --
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
delete from dim_obs 
where  obs_id in 
	(select obs_id from obs where voided =1);

create table tmp_group_obs 
select implementation_id, encounter_type, obs_group_id, question, 
group_concat(case answer when '' then null else answer end) as answer from dim_obs 
where implementation_id = impl_id and obs_group_id is not null 
group by implementation_id, obs_group_id, encounter_type 
having answer is not null;

update dim_obs as o, tmp_group_obs as t 
set o.answer = t.answer 
where o.implementation_id = t.implementation_id and o.obs_id = t.obs_group_id;

drop table if exists tmp_dim_encounter;
create table  tmp_dim_encounter 
select e.surrogate_id, e.implementation_id, e.encounter_id, e.encounter_type, et.name as encounter_name, et.description, e.patient_id, e.location_id, p.identifier as provider, e.encounter_datetime as date_entered, e.creator, e.date_created as date_start, e.changed_by, e.date_changed, e.date_created as date_end, e.uuid from encounter as e 
inner join encounter_type as et on et.implementation_id = e.implementation_id and et.encounter_type_id = e.encounter_type and et.retired = 0 
left outer join encounter_provider as ep on ep.implementation_id = e.implementation_id and ep.encounter_id = e.encounter_id and ep.voided = 0 
left outer join provider as p on p.implementation_id = e.implementation_id and p.provider_id = ep.provider_id and p.retired = 0 
where e.voided = 0 and  (e.date_changed between date_from and date_to);

update dim_encounter as de, tmp_dim_encounter as t 
set de.patient_id = t.patient_id , de.location_id=t.location_id,de.provider=t.provider,de.date_entered=t.date_entered
where de.implementation_id = t.implementation_id and de.encounter_id=t.encounter_id and de.encounter_type=t.encounter_type;

-- -- -- -- --
-- User Form
-- -- -- -- --
insert into dim_user_form 
select ut.surrogate_id, ut.implementation_id, ut.user_form_id, uft.user_form_type_id, uft.user_form_type, uft.description, ut.user_id, ut.created_at as location_id, ut.date_entered, ut.date_created, ut.changed_by, ut.date_changed, ut.uuid from gfatm_user_form as ut 
inner join gfatm_user_form_type as uft on uft.user_form_type_id = ut.user_form_type_id 
where ut.implementation_id = impl_id 
and not exists (select * from dim_user_form where implementation_id = ut.implementation_id and user_form_id = ut.user_form_id) 
and ((ut.date_created between date_from and date_to) or (ut.date_changed between date_from and date_to));

insert into dim_user_form_result 
select ufr.surrogate_id, ufr.implementation_id, uf.user_form_type_id, ufr.user_form_result_id, ufr.user_form_id, ufr.element_id, e.element_name as question, ufr.result as answer, ufr.created_by as user_id, ufr.created_at as location_id, uf.date_entered, ufr.date_created, ufr.changed_by, ufr.date_changed, ufr.uuid from gfatm_user_form_result as ufr 
inner join gfatm_user_form as uf on uf.implementation_id = ufr.implementation_id and uf.user_form_id = ufr.user_form_id 
inner join gfatm_element as e on e.implementation_id = ufr.implementation_id and e.element_id = ufr.element_id 
where ufr.implementation_id = impl_id 
and not exists (select * from dim_user_form_result where implementation_id = ufr.implementation_id and user_form_result_id = ufr.user_form_result_id) 
and ((ufr.date_created between date_from and date_to) or (ufr.date_changed between date_from and date_to));

-- -- -- -- --
-- Lab Test
-- -- -- -- --
insert ignore into dim_lab_test 
select t.surrogate_id, t.implementation_id, t.test_order_id, t.test_type_id, tt.name as test_name, tt.short_name, tt.test_group, e.patient_id, o.orderer, o.encounter_id, o.order_reason, o.order_number, o.date_activated as order_date, t.lab_reference_number, o.instructions, t.report_file_path, t.result_comments, t.creator, t.date_created, t.changed_by, t.date_changed, t.uuid from commonlabtest_test as t
inner join commonlabtest_type as tt on tt.implementation_id = t.implementation_id and tt.test_type_id = t.test_type_id and tt.retired = 0 
inner join orders as o on o.implementation_id = t.implementation_id and o.order_id = t.test_order_id and o.voided = 0 
inner join encounter as e on e.implementation_id = t.implementation_id and e.encounter_id = o.encounter_id and e.voided = 0 
where t.voided = 0 and not exists (select * from dim_lab_test where implementation_id = t.implementation_id and test_order_id = t.test_order_id) 
and ((t.date_created between date_from and date_to) or (t.date_changed between date_from and date_to));

insert ignore into dim_lab_test_result 
select a.surrogate_id, a.implementation_id, a.test_order_id, t.patient_id, t.test_type_id, a.test_attribute_id, a.attribute_type_id, at.name as attribute_type_name, a.value_reference, replace(at.datatype, 'org.openmrs.customdatatype.datatype.', '') datatype, t.lab_reference_number, a.creator, a.date_created, a.changed_by, a.date_changed, a.uuid from commonlabtest_attribute as a 
inner join commonlabtest_attribute_type as at on at.implementation_id = a.implementation_id and at.test_attribute_type_id = a.attribute_type_id and at.retired = 0 
inner join dim_lab_test as t on t.implementation_id = a.implementation_id and t.test_order_id = a.test_order_id 
where a.voided = 0 and not exists (select * from dim_lab_test_result where implementation_id = a.implementation_id and test_attribute_id = a.test_attribute_id) 
and (a.date_created between date_from and date_to);

END