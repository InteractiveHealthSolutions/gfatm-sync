DELIMITER $$
CREATE PROCEDURE `lab_test_dimension`(in impl_id int, in date_from datetime, in date_to datetime)
BEGIN

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