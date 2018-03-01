DELIMITER $$
CREATE PROCEDURE `location_dimension`(in impl_id int)
BEGIN

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
group_concat(if(a.attribute_type_id = 16, a.value_reference, null)) as Site_Supervisor_System_ID,
 '' as BLANK from location_attribute as a 
where a.voided = 0 and a.implementation_id = 1 
group by a.location_id;

insert into dim_location (surrogate_id, implementation_id, location_id, location_name, description, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, creator, date_created, retired, parent_location, uuid, location_identifier,primary_contact,fast_location,pmdt_location,aic_location,pet_location,comorbidities_location,childhoodtb_location,location_type,secondary_contact,staff_time,opd_timing,status,primary_contact_name,secondary_contact_name,Site_Supervisor_System_ID) 
select l.surrogate_id, l.implementation_id, l.location_id, l.name as location_name, l.description, l.address1, l.address2, l.city_village, l.state_province, l.postal_code, l.country, l.latitude, l.longitude, l.creator, l.date_created, l.retired, l.parent_location, l.uuid, lam.location_identifier,lam.primary_contact,lam.fast_location,lam.pmdt_location,lam.aic_location,lam.pet_location,lam.comorbidities_location,lam.childhoodtb_location,lam.location_type,lam.secondary_contact,lam.staff_time,lam.opd_timing,lam.status,lam.primary_contact_name,lam.secondary_contact_name,lam.Site_Supervisor_System_ID from location as l 
left outer join location_attribute_merged as lam using (implementation_id, location_id) 
where l.implementation_id = impl_id and l.surrogate_id not in 
	(select surrogate_id from dim_location where implementation_id = l.implementation_id);

END$$
DELIMITER ;
