DELIMITER $$
CREATE PROCEDURE `concept_dimension`(in impl_id int)
BEGIN

drop table if exists concept_latest_name;
create table concept_latest_name 
select c.implementation_id, c.concept_id, 
(select max(concept_name_id) from concept_name where implementation_id = c.implementation_id and concept_id = c.concept_id and locale = 'en' and voided = 0 and concept_name_type is null) as default_name, 
(select max(concept_name_id) from concept_name where implementation_id = c.implementation_id and concept_id = c.concept_id and locale = 'en' and voided = 0 and concept_name_type = 'SHORT') as short_name, 
(select max(concept_name_id) from concept_name where implementation_id = c.implementation_id and concept_id = c.concept_id and locale = 'en' and voided = 0 and concept_name_type = 'FULLY_SPECIFIED') as full_name from concept as c 
having concat(ifnull(default_name, ''), ifnull(short_name, ''), ifnull(full_name, '')) <> '';

alter table concept_latest_name add primary key composite_id (implementation_id, concept_id);

insert into dim_concept (surrogate_id, implementation_id, concept_id, full_name, short_name, default_name, description, retired, data_type, class, hi_absolute, hi_critical, hi_normal, low_absolute, low_critical, low_normal, creator, date_created, version, changed_by, date_changed, uuid) 
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

END$$
DELIMITER ;
