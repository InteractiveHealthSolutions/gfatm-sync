DELIMITER $$
CREATE PROCEDURE `user_dimension`(in impl_id int)
BEGIN

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

END$$
DELIMITER ;
