DELIMITER $$
CREATE PROCEDURE `patient_dimension`(in impl_id int, in date_from datetime, in date_to datetime)
BEGIN

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

END$$
DELIMITER ;
