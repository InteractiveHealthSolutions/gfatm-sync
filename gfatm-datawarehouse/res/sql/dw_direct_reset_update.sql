DELIMITER $$
CREATE PROCEDURE `dw_direct_reset_update`()
BEGIN

TRUNCATE TABLE tmp_gfatm_element; 
TRUNCATE TABLE tmp_gfatm_location;
TRUNCATE TABLE tmp_gfatm_location_attribute;
TRUNCATE TABLE tmp_gfatm_location_attribute_type;
TRUNCATE TABLE tmp_gfatm_user_attribute; 
TRUNCATE TABLE tmp_gfatm_user_attribute_type;
TRUNCATE TABLE tmp_gfatm_user_form; 
TRUNCATE TABLE tmp_gfatm_user_form_result;
TRUNCATE TABLE tmp_gfatm_user_form_type; 
TRUNCATE TABLE tmp_gfatm_user_location;
TRUNCATE TABLE tmp_gfatm_user_role; 
TRUNCATE TABLE tmp_gfatm_users;

TRUNCATE TABLE gfatm_element; 
TRUNCATE TABLE gfatm_location;
TRUNCATE TABLE gfatm_location_attribute;
TRUNCATE TABLE gfatm_location_attribute_type;
TRUNCATE TABLE gfatm_user_attribute; 
TRUNCATE TABLE gfatm_user_attribute_type;
TRUNCATE TABLE gfatm_user_form; 
TRUNCATE TABLE gfatm_user_form_result;
TRUNCATE TABLE gfatm_user_form_type; 
TRUNCATE TABLE gfatm_user_location;
TRUNCATE TABLE gfatm_user_role; 
TRUNCATE TABLE gfatm_users;

TRUNCATE TABLE tmp_person; 
TRUNCATE TABLE tmp_person_attribute;
TRUNCATE TABLE tmp_person_attribute_type; 
TRUNCATE TABLE tmp_person_address;
TRUNCATE TABLE tmp_person_name; 
TRUNCATE TABLE tmp_role; 
TRUNCATE TABLE tmp_role_role;
TRUNCATE TABLE tmp_privilege; 
TRUNCATE TABLE tmp_role_privilege; 
TRUNCATE TABLE tmp_users;
TRUNCATE TABLE tmp_user_property; 
TRUNCATE TABLE tmp_user_role;
TRUNCATE TABLE tmp_provider_attribute_type; 
TRUNCATE TABLE tmp_provider;
TRUNCATE TABLE tmp_provider_attribute; 
TRUNCATE TABLE tmp_location_attribute_type;
TRUNCATE TABLE tmp_location;
TRUNCATE TABLE tmp_location_attribute;
TRUNCATE TABLE tmp_location_tag;
TRUNCATE TABLE tmp_location_tag_map;
TRUNCATE TABLE tmp_concept_class;
TRUNCATE TABLE tmp_concept_set;
TRUNCATE TABLE tmp_concept_datatype;
TRUNCATE TABLE tmp_concept_map_type;
TRUNCATE TABLE tmp_concept;
TRUNCATE TABLE tmp_concept_name;
TRUNCATE TABLE tmp_concept_description;
TRUNCATE TABLE tmp_concept_answer;
TRUNCATE TABLE tmp_concept_numeric;
TRUNCATE TABLE tmp_patient_identifier_type;
TRUNCATE TABLE tmp_patient;
TRUNCATE TABLE tmp_patient_identifier;
TRUNCATE TABLE tmp_patient_program;
TRUNCATE TABLE tmp_encounter_type;
TRUNCATE TABLE tmp_form;
TRUNCATE TABLE tmp_encounter_role;
TRUNCATE TABLE tmp_encounter;
TRUNCATE TABLE tmp_encounter_provider;
TRUNCATE TABLE tmp_obs;
TRUNCATE TABLE tmp_visit_type;
TRUNCATE TABLE tmp_visit_attribute_type;
TRUNCATE TABLE tmp_visit_attribute;
TRUNCATE TABLE tmp_field;
TRUNCATE TABLE tmp_field_answer;
TRUNCATE TABLE tmp_field_type;
TRUNCATE TABLE tmp_form_field;

TRUNCATE TABLE person; 
TRUNCATE TABLE person_attribute;
TRUNCATE TABLE person_attribute_type; 
TRUNCATE TABLE person_address;
TRUNCATE TABLE person_name; 
TRUNCATE TABLE role; 
TRUNCATE TABLE role_role;
TRUNCATE TABLE privilege; 
TRUNCATE TABLE role_privilege; 
TRUNCATE TABLE users;
TRUNCATE TABLE user_property; 
TRUNCATE TABLE user_role;
TRUNCATE TABLE provider_attribute_type; 
TRUNCATE TABLE provider;
TRUNCATE TABLE provider_attribute; 
TRUNCATE TABLE location_attribute_type;
TRUNCATE TABLE location;
TRUNCATE TABLE location_attribute;
TRUNCATE TABLE location_tag;
TRUNCATE TABLE location_tag_map;
TRUNCATE TABLE concept_class;
TRUNCATE TABLE concept_set;
TRUNCATE TABLE concept_datatype;
TRUNCATE TABLE concept_map_type;
TRUNCATE TABLE concept;
TRUNCATE TABLE concept_name;
TRUNCATE TABLE concept_description;
TRUNCATE TABLE concept_answer;
TRUNCATE TABLE concept_numeric;
TRUNCATE TABLE patient_identifier_type;
TRUNCATE TABLE patient;
TRUNCATE TABLE patient_identifier;
TRUNCATE TABLE patient_program;
TRUNCATE TABLE encounter_type;
TRUNCATE TABLE form;
TRUNCATE TABLE encounter_role;
TRUNCATE TABLE encounter;
TRUNCATE TABLE encounter_provider;
TRUNCATE TABLE obs;
TRUNCATE TABLE visit_type;
TRUNCATE TABLE visit_attribute_type;
TRUNCATE TABLE visit_attribute;
TRUNCATE TABLE field;
TRUNCATE TABLE field_answer;
TRUNCATE TABLE field_type;
TRUNCATE TABLE form_field;

INSERT INTO gfatm_location_attribute_type (surrogate_id, implementation_id, location_attribute_type_id, attribute_name, validation_regex, required, description, date_created, created_by, created_at, date_changed, changed_by, changed_at, data_type, uuid) 
SELECT 0, 1, location_attribute_type_id, attribute_name, validation_regex, required, description, date_created, created_by, created_at, date_changed, changed_by, changed_at, data_type, uuid FROM gfatm.location_attribute_type;

INSERT INTO gfatm_location (surrogate_id, implementation_id, location_id, location_name, category, description, address1, address2, address3, city_village, state_province, country, landmark1, landmark2, latitude, longitude, primary_contact, secondary_contact, email, fax, parent_location, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid) 
SELECT 0, 1, location_id, location_name, category, description, address1, address2, address3, city_village, state_province, country, landmark1, landmark2, latitude, longitude, primary_contact, secondary_contact, email, fax, parent_location, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid FROM gfatm.location;

INSERT INTO gfatm_location_attribute (surrogate_id, implementation_id, location_attribute_id, attribute_type_id, location_id, attribute_value, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid) 
SELECT 0, 1, location_attribute_id, attribute_type_id, location_id, attribute_value, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid FROM gfatm.location_attribute;

INSERT INTO gfatm_users (surrogate_id, implementation_id, user_id, username, full_name, global_data_access, disabled, reason_disabled, password_hash, password_salt, secret_question, secret_answer_hash, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid) 
SELECT 0, 1, user_id, username, full_name, global_data_access, disabled, reason_disabled, password_hash, password_salt, secret_question, secret_answer_hash, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid FROM gfatm.users;

INSERT INTO gfatm_user_attribute_type (surrogate_id, implementation_id, user_attribute_type_id, attribute_name, data_type, date_changed, date_created, description, required, validation_regex, changed_at, created_at, changed_by, created_by, uuid) 
SELECT 0, 1, user_attribute_type_id, attribute_name, data_type, date_changed, date_created, description, required, validation_regex, changed_at, created_at, changed_by, created_by, uuid FROM gfatm.user_attribute_type;

INSERT INTO gfatm_user_attribute (surrogate_id, implementation_id, user_attribute_id, attribute_value, date_changed, date_created, changed_at, created_at, user_attribute_type_id, user_id, changed_by, created_by, uuid) 
SELECT 0, 1, user_attribute_id, attribute_value, date_changed, date_created, changed_at, created_at, user_attribute_type_id, user_id, changed_by, created_by, uuid FROM gfatm.user_attribute;

INSERT INTO gfatm_user_role (surrogate_id, implementation_id, user_id, role_id, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid) 
SELECT 0, 1, user_id, role_id, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid FROM gfatm.user_role;

INSERT INTO gfatm_user_location (surrogate_id, implementation_id, user_id, location_id, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid) 
SELECT 0, 1, user_id, location_id, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid FROM gfatm.user_location;

INSERT INTO gfatm_element (surrogate_id, implementation_id, element_id, element_name, validation_regex, data_type, description, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid) 
SELECT 0, 1, element_id, element_name, validation_regex, data_type, description, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid FROM gfatm.element;

INSERT INTO gfatm_user_form_type (surrogate_id, implementation_id, user_form_type_id, user_form_type, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid, description) 
SELECT 0, 1, user_form_type_id, user_form_type, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid, description FROM gfatm.user_form_type;

INSERT INTO gfatm_user_form (surrogate_id, implementation_id, user_form_id, user_form_type_id, user_id, duration_seconds, date_entered, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid) 
SELECT 0, 1, user_form_id, user_form_type_id, user_id, duration_seconds, date_entered, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid FROM gfatm.user_form;

INSERT INTO person (surrogate_id, implementation_id, person_id, gender, birthdate, birthdate_estimated, dead, death_date, cause_of_death, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid) 
SELECT 0, 1, person_id, gender, birthdate, birthdate_estimated, dead, death_date, cause_of_death, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid FROM openmrs.person;

INSERT INTO person_attribute_type (surrogate_id, implementation_id, person_attribute_type_id, name, description, format, foreign_key, searchable, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, edit_privilege, sort_weight, uuid) 
SELECT 0, 1, person_attribute_type_id, name, description, format, foreign_key, searchable, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, edit_privilege, sort_weight, uuid FROM openmrs.person_attribute_type;

INSERT INTO person_attribute (surrogate_id, implementation_id, person_attribute_id, person_id, value, person_attribute_type_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid) 
SELECT 0, 1, person_attribute_id, person_id, value, person_attribute_type_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid FROM openmrs.person_attribute;

INSERT INTO person_address (surrogate_id, implementation_id, person_address_id, person_id, preferred, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, start_date, end_date, creator, date_created, voided, voided_by, date_voided, void_reason, county_district, address3, address4, address5, address6, date_changed, changed_by, uuid) 
SELECT 0, 1, person_address_id, person_id, preferred, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, start_date, end_date, creator, date_created, voided, voided_by, date_voided, void_reason, county_district, address3, address4, address5, address6, date_changed, changed_by, uuid FROM openmrs.person_address;

INSERT INTO person_name (surrogate_id, implementation_id, person_name_id, preferred, person_id, prefix, given_name, middle_name, family_name_prefix, family_name, family_name2, family_name_suffix, degree, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid) 
SELECT 0, 1, person_name_id, preferred, person_id, prefix, given_name, middle_name, family_name_prefix, family_name, family_name2, family_name_suffix, degree, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, uuid FROM openmrs.person_name;

INSERT INTO role (surrogate_id, implementation_id, role, description, uuid) 
SELECT 0, 1, role, description, uuid FROM openmrs.role;

INSERT INTO role_role (surrogate_id, implementation_id, parent_role, child_role) 
SELECT 0, 1, parent_role, child_role FROM openmrs.role_role;

INSERT INTO privilege (surrogate_id, implementation_id, privilege, description, uuid) 
SELECT 0, 1, privilege, description, uuid FROM openmrs.privilege;

INSERT INTO role_privilege (surrogate_id, implementation_id, role, privilege) 
SELECT 0, 1, role, privilege FROM openmrs.role_privilege;

INSERT INTO users (surrogate_id, implementation_id, user_id, system_id, username, password, salt, secret_question, secret_answer, creator, date_created, changed_by, date_changed, person_id, retired, retired_by, date_retired, retire_reason, uuid) 
SELECT 0, 1, user_id, system_id, username, password, salt, secret_question, secret_answer, creator, date_created, changed_by, date_changed, person_id, retired, retired_by, date_retired, retire_reason, uuid FROM openmrs.users;

INSERT INTO user_property (surrogate_id, implementation_id, user_id, property, property_value) 
SELECT 0, 1, user_id, property, property_value FROM openmrs.user_property;

INSERT INTO user_role (surrogate_id, implementation_id, user_id, role) 
SELECT 0, 1, user_id, role FROM openmrs.user_role;

INSERT INTO provider_attribute_type (surrogate_id, implementation_id, provider_attribute_type_id, name, description, datatype, datatype_config, preferred_handler, handler_config, min_occurs, max_occurs, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid) 
SELECT 0, 1, provider_attribute_type_id, name, description, datatype, datatype_config, preferred_handler, handler_config, min_occurs, max_occurs, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid FROM openmrs.provider_attribute_type;

INSERT INTO provider (surrogate_id, implementation_id, provider_id, person_id, name, identifier, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid) 
SELECT 0, 1, provider_id, person_id, name, identifier, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid FROM openmrs.provider;

INSERT INTO provider_attribute (surrogate_id, implementation_id, provider_attribute_id, provider_id, attribute_type_id, value_reference, uuid, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason) 
SELECT 0, 1, provider_attribute_id, provider_id, attribute_type_id, value_reference, uuid, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason FROM openmrs.provider_attribute;

INSERT INTO location_attribute_type (surrogate_id, implementation_id, location_attribute_type_id, name, description, datatype, datatype_config, preferred_handler, handler_config, min_occurs, max_occurs, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid) 
SELECT 0, 1, location_attribute_type_id, name, description, datatype, datatype_config, preferred_handler, handler_config, min_occurs, max_occurs, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid FROM openmrs.location_attribute_type;

INSERT INTO location (surrogate_id, implementation_id, location_id, name, description, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, creator, date_created, county_district, address3, address4, address5, address6, retired, retired_by, date_retired, retire_reason, parent_location, uuid, changed_by, date_changed) 
SELECT 0, 1, location_id, name, description, address1, address2, city_village, state_province, postal_code, country, latitude, longitude, creator, date_created, county_district, address3, address4, address5, address6, retired, retired_by, date_retired, retire_reason, parent_location, uuid, changed_by, date_changed FROM openmrs.location;

INSERT INTO location_attribute (surrogate_id, implementation_id, location_attribute_id, location_id, attribute_type_id, value_reference, uuid, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason) 
SELECT 0, 1, location_attribute_id, location_id, attribute_type_id, value_reference, uuid, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason FROM openmrs.location_attribute;

INSERT INTO location_tag (surrogate_id, implementation_id, location_tag_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid, changed_by, date_changed) 
SELECT 0, 1, location_tag_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid, changed_by, date_changed FROM openmrs.location_tag;

INSERT INTO location_tag_map (surrogate_id, implementation_id, location_id, location_tag_id) 
SELECT 0, 1, location_id, location_tag_id FROM openmrs.location_tag_map;

INSERT INTO concept_class (surrogate_id, implementation_id, concept_class_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) 
SELECT 0, 1, concept_class_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid FROM openmrs.concept_class;

INSERT INTO concept_set (surrogate_id, implementation_id, concept_set_id, concept_id, concept_set, sort_weight, creator, date_created, uuid) 
SELECT 0, 1, concept_set_id, concept_id, concept_set, sort_weight, creator, date_created, uuid FROM openmrs.concept_set;

INSERT INTO concept_datatype (surrogate_id, implementation_id, concept_datatype_id, name, hl7_abbreviation, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid) 
SELECT 0, 1, concept_datatype_id, name, hl7_abbreviation, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid FROM openmrs.concept_datatype;

INSERT INTO concept_map_type (surrogate_id, implementation_id, concept_map_type_id, name, description, creator, date_created, changed_by, date_changed, is_hidden, retired, retired_by, date_retired, retire_reason, uuid) 
SELECT 0, 1, concept_map_type_id, name, description, creator, date_created, changed_by, date_changed, is_hidden, retired, retired_by, date_retired, retire_reason, uuid FROM openmrs.concept_map_type;

INSERT INTO concept (surrogate_id, implementation_id, concept_id, retired, short_name, description, form_text, datatype_id, class_id, is_set, creator, date_created, version, changed_by, date_changed, retired_by, date_retired, retire_reason, uuid) 
SELECT 0, 1, concept_id, retired, short_name, description, form_text, datatype_id, class_id, is_set, creator, date_created, version, changed_by, date_changed, retired_by, date_retired, retire_reason, uuid FROM openmrs.concept;

INSERT INTO concept_name (surrogate_id, implementation_id, concept_id, name, locale, creator, date_created, concept_name_id, voided, voided_by, date_voided, void_reason, uuid, concept_name_type, locale_preferred) 
SELECT 0, 1, concept_id, name, locale, creator, date_created, concept_name_id, voided, voided_by, date_voided, void_reason, uuid, concept_name_type, locale_preferred FROM openmrs.concept_name;

INSERT INTO concept_description (surrogate_id, implementation_id, concept_description_id, concept_id, description, locale, creator, date_created, changed_by, date_changed, uuid) 
SELECT 0, 1, concept_description_id, concept_id, description, locale, creator, date_created, changed_by, date_changed, uuid FROM openmrs.concept_description;

INSERT INTO concept_answer (surrogate_id, implementation_id, concept_answer_id, concept_id, answer_concept, answer_drug, creator, date_created, uuid, sort_weight) 
SELECT 0, 1, concept_answer_id, concept_id, answer_concept, answer_drug, creator, date_created, uuid, sort_weight FROM openmrs.concept_answer;

INSERT INTO concept_numeric (surrogate_id, implementation_id, concept_id, hi_absolute, hi_critical, hi_normal, low_absolute, low_critical, low_normal, units, precise, display_precision) 
SELECT 0, 1, concept_id, hi_absolute, hi_critical, hi_normal, low_absolute, low_critical, low_normal, units, precise, display_precision FROM openmrs.concept_numeric;

INSERT INTO patient_identifier_type (surrogate_id, implementation_id, patient_identifier_type_id, name, description, format, check_digit, creator, date_created, required, format_description, validator, location_behavior, retired, retired_by, date_retired, retire_reason, uuid, uniqueness_behavior) 
SELECT 0, 1, patient_identifier_type_id, name, description, format, check_digit, creator, date_created, required, format_description, validator, location_behavior, retired, retired_by, date_retired, retire_reason, uuid, uniqueness_behavior FROM openmrs.patient_identifier_type;

INSERT INTO patient (surrogate_id, implementation_id, patient_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason) 
SELECT 0, 1, patient_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason FROM openmrs.patient;

INSERT INTO patient_identifier (surrogate_id, implementation_id, patient_identifier_id, patient_id, identifier, identifier_type, preferred, location_id, creator, date_created, date_changed, changed_by, voided, voided_by, date_voided, void_reason, uuid) 
SELECT 0, 1, patient_identifier_id, patient_id, identifier, identifier_type, preferred, location_id, creator, date_created, date_changed, changed_by, voided, voided_by, date_voided, void_reason, uuid FROM openmrs.patient_identifier;

INSERT INTO patient_program (surrogate_id, implementation_id, patient_program_id, patient_id, program_id, date_enrolled, date_completed, location_id, outcome_concept_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid) 
SELECT 0, 1, patient_program_id, patient_id, program_id, date_enrolled, date_completed, location_id, outcome_concept_id, creator, date_created, changed_by, date_changed, voided, voided_by, date_voided, void_reason, uuid FROM openmrs.patient_program;

INSERT INTO encounter_type (surrogate_id, implementation_id, encounter_type_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid, edit_privilege, view_privilege) 
SELECT 0, 1, encounter_type_id, name, description, creator, date_created, retired, retired_by, date_retired, retire_reason, uuid, edit_privilege, view_privilege FROM openmrs.encounter_type;

INSERT INTO encounter_role (surrogate_id, implementation_id, encounter_role_id, name, description, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid) 
SELECT 0, 1, encounter_role_id, name, description, creator, date_created, changed_by, date_changed, retired, retired_by, date_retired, retire_reason, uuid FROM openmrs.encounter_role;

INSERT INTO encounter (surrogate_id, implementation_id, encounter_id, encounter_type, patient_id, location_id, form_id, encounter_datetime, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, visit_id, uuid) 
SELECT 0, 1, encounter_id, encounter_type, patient_id, location_id, form_id, encounter_datetime, creator, date_created, voided, voided_by, date_voided, void_reason, changed_by, date_changed, visit_id, uuid FROM openmrs.encounter;

INSERT INTO encounter_provider (surrogate_id, implementation_id, encounter_provider_id, encounter_id, provider_id, encounter_role_id, creator, date_created, changed_by, date_changed, voided, date_voided, voided_by, void_reason, uuid) 
SELECT 0, 1, encounter_provider_id, encounter_id, provider_id, encounter_role_id, creator, date_created, changed_by, date_changed, voided, date_voided, voided_by, void_reason, uuid FROM openmrs.encounter_provider;

INSERT INTO gfatm_user_form_result (surrogate_id, implementation_id, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid) 
SELECT 0, 1, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid FROM gfatm.user_form_result
WHERE user_form_result_id BETWEEN 1 AND 1000000;

INSERT INTO gfatm_user_form_result (surrogate_id, implementation_id, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid) 
SELECT 0, 1, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid FROM gfatm.user_form_result
WHERE user_form_result_id BETWEEN 1000001 AND 2000000;

INSERT INTO gfatm_user_form_result (surrogate_id, implementation_id, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid) 
SELECT 0, 1, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid FROM gfatm.user_form_result
WHERE user_form_result_id BETWEEN 2000001 AND 3000000;

INSERT INTO gfatm_user_form_result (surrogate_id, implementation_id, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid) 
SELECT 0, 1, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid FROM gfatm.user_form_result
WHERE user_form_result_id BETWEEN 3000001 AND 4000000;

INSERT INTO gfatm_user_form_result (surrogate_id, implementation_id, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid) 
SELECT 0, 1, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid FROM gfatm.user_form_result
WHERE user_form_result_id BETWEEN 4000001 AND 5000000;

INSERT INTO gfatm_user_form_result (surrogate_id, implementation_id, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid) 
SELECT 0, 1, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid FROM gfatm.user_form_result
WHERE user_form_result_id BETWEEN 5000001 AND 6000000;

INSERT INTO gfatm_user_form_result (surrogate_id, implementation_id, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid) 
SELECT 0, 1, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid FROM gfatm.user_form_result
WHERE user_form_result_id BETWEEN 6000001 AND 7000000;

INSERT INTO gfatm_user_form_result (surrogate_id, implementation_id, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid) 
SELECT 0, 1, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid FROM gfatm.user_form_result
WHERE user_form_result_id BETWEEN 7000001 AND 8000000;

INSERT INTO gfatm_user_form_result (surrogate_id, implementation_id, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid) 
SELECT 0, 1, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid FROM gfatm.user_form_result
WHERE user_form_result_id BETWEEN 8000001 AND 9000000;

INSERT INTO gfatm_user_form_result (surrogate_id, implementation_id, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid) 
SELECT 0, 1, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid FROM gfatm.user_form_result
WHERE user_form_result_id BETWEEN 9000001 AND 10000000;

INSERT INTO gfatm_user_form_result (surrogate_id, implementation_id, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid) 
SELECT 0, 1, user_form_result_id, user_form_id, element_id, result, date_created, created_by, created_at, date_changed, changed_by, changed_at, uuid FROM gfatm.user_form_result
WHERE user_form_result_id > 10000000;

INSERT INTO obs (surrogate_id, implementation_id, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version) 
SELECT 0, 1, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version FROM openmrs.obs 
WHERE obs_id BETWEEN 1 AND 1000000;

INSERT INTO obs (surrogate_id, implementation_id, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version) 
SELECT 0, 1, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version FROM openmrs.obs 
WHERE obs_id BETWEEN 1000001 AND 2000000;

INSERT INTO obs (surrogate_id, implementation_id, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version) 
SELECT 0, 1, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version FROM openmrs.obs 
WHERE obs_id BETWEEN 2000001 AND 3000000;

INSERT INTO obs (surrogate_id, implementation_id, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version) 
SELECT 0, 1, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version FROM openmrs.obs 
WHERE obs_id BETWEEN 3000001 AND 4000000;

INSERT INTO obs (surrogate_id, implementation_id, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version) 
SELECT 0, 1, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version FROM openmrs.obs 
WHERE obs_id BETWEEN 4000001 AND 5000000;

INSERT INTO obs (surrogate_id, implementation_id, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version) 
SELECT 0, 1, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version FROM openmrs.obs 
WHERE obs_id BETWEEN 5000001 AND 6000000;

INSERT INTO obs (surrogate_id, implementation_id, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version) 
SELECT 0, 1, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version FROM openmrs.obs 
WHERE obs_id BETWEEN 6000001 AND 7000000;

INSERT INTO obs (surrogate_id, implementation_id, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version) 
SELECT 0, 1, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version FROM openmrs.obs 
WHERE obs_id BETWEEN 7000001 AND 8000000;

INSERT INTO obs (surrogate_id, implementation_id, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version) 
SELECT 0, 1, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version FROM openmrs.obs 
WHERE obs_id BETWEEN 8000001 AND 9000000;

INSERT INTO obs (surrogate_id, implementation_id, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version) 
SELECT 0, 1, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version FROM openmrs.obs 
WHERE obs_id BETWEEN 9000001 AND 10000000;

INSERT INTO obs (surrogate_id, implementation_id, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version) 
SELECT 0, 1, obs_id, person_id, concept_id, encounter_id, order_id, obs_datetime, location_id, obs_group_id, accession_number, value_group_id, value_boolean, value_coded, value_coded_name_id, value_drug, value_datetime, value_numeric, value_modifier, value_text, value_complex, comments, creator, date_created, voided, voided_by, date_voided, void_reason, uuid, previous_version FROM openmrs.obs 
WHERE obs_id > 10000000;

END$$
DELIMITER ;
