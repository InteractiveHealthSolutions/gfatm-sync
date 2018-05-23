CREATE PROCEDURE `delete_data`(date_from DATETIME, date_to DATETIME)
BEGIN

	DELETE FROM concept WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM concept_answer WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM concept_description WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM concept_name WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM dim_concept WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM dim_encounter WHERE date_end BETWEEN date_from AND date_to;
	DELETE FROM dim_location WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM dim_obs WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM dim_patient WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM dim_user WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM dim_user_form WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM dim_user_form_result WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM encounter WHERE date_end BETWEEN date_from AND date_to;
	DELETE FROM encounter_provider WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM encounter_role WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM gfatm_element WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM gfatm_location WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM gfatm_location_attribute WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM gfatm_user_attribute WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM gfatm_user_form WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM gfatm_user_form_result WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM gfatm_user_location WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM gfatm_user_role WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM gfatm_users WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM location WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM location_attribute WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM location_tag WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM location_tag_map WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM obs WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM patient WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM patient_identifier WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM patient_program WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM patient_state WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM person WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM person_address WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM person_attribute WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM person_name WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM provider WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM provider_attribute WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM role WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM role_privilege WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM role_role WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM user_attribute WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM user_form WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM user_form_result WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM user_gfatm_location WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM user_role WHERE date_created BETWEEN date_from AND date_to;
	DELETE FROM users WHERE date_created BETWEEN date_from AND date_to;

END