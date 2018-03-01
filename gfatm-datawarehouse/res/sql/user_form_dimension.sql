DELIMITER $$
CREATE PROCEDURE `user_form_dimension`(in impl_id int, in date_from datetime, in date_to datetime)
BEGIN

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


END$$
DELIMITER ;
