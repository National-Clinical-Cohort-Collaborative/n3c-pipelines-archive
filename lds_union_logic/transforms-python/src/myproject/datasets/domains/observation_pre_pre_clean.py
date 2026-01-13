from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F
from transforms.verbs import dataframes as D


@transform_df(
    Output("/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/LDS Union/pre_pre_clean/observation"),
    input_df=Input("/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/LDS Union/unioned_observation")
)
def my_compute_function(input_df, ctx):
    foundry_df = input_df.withColumn('temp_id', F.monotonically_increasing_id())
    bad_rows_dfs = []

    # -----PHI_LOINC_CONCEPT_ID
    phi_loinc_concept_id_rows = \
        get_bad_rows_by_column_val(foundry_df,
                                   'observation_concept_id',
                                   PHI_LOINC_CONCEPT_IDS,
                                   'PHI_LOINC_CONCEPT_ID')
    if phi_loinc_concept_id_rows:
        bad_rows_dfs.append(phi_loinc_concept_id_rows)

    # -----POSSIBLE_PHI_LOINC_CONCEPT_ID
    possible_phi_loinc_concept_id_rows = \
        get_bad_rows_by_column_val(foundry_df,
                                   'observation_concept_id',
                                   POSSIBLE_PHI_LOINC_CONCEPT_IDS,
                                   'POSSIBLE_PHI_LOINC_CONCEPT_ID')
    if possible_phi_loinc_concept_id_rows:
        bad_rows_dfs.append(possible_phi_loinc_concept_id_rows)

    # -----AIAN_CONCEPT_ID
    aian_concept_id_rows = \
        get_bad_rows_by_column_val(foundry_df,
                                   'observation_concept_id',
                                   AIAN_CONCEPT_IDS,
                                   'AIAN_CONCEPT_ID')
    if aian_concept_id_rows:
        bad_rows_dfs.append(aian_concept_id_rows)

    # -----PHI_LOINC_SOURCE_VALUE
    phi_loinc_source_value_rows = \
        get_bad_rows_by_column_val(foundry_df,
                                   'observation_source_value',
                                   PHI_LOINC_SOURCE_VALUES,
                                   'PHI_LOINC_SOURCE_VALUE')
    if phi_loinc_source_value_rows:
        bad_rows_dfs.append(phi_loinc_source_value_rows)

    # -----POSSIBLE_PHI_LOINC_SOURCE_VALUE
    possible_phi_loinc_source_value_rows = \
        get_bad_rows_by_column_val(foundry_df,
                                   'observation_source_value',
                                   POSSIBLE_PHI_LOINC_SOURCE_VALUES,
                                   'POSSIBLE_PHI_LOINC_SOURCE_VALUE')
    if possible_phi_loinc_source_value_rows:
        bad_rows_dfs.append(possible_phi_loinc_source_value_rows)

    # -----PHI_SNOMED_CONCEPT_ID
    phi_snomed_concept_id_rows = \
        get_bad_rows_by_column_val(foundry_df,
                                   'observation_concept_id',
                                   PHI_SNOMED_CONCEPT_IDS,
                                   'PHI_SNOMED_CONCEPT_ID')
    if phi_snomed_concept_id_rows:
        bad_rows_dfs.append(phi_snomed_concept_id_rows)

    # REMOVE ROWS
    if len(bad_rows_dfs) > 0:
        removed_rows_df = D.union_many(*bad_rows_dfs, spark_session=ctx.spark_session)
        # Remove dupes -- some rows may be caught by more than one check, but we only want to record it once
        removed_rows_df = removed_rows_df.dropDuplicates(['temp_id'])
        foundry_df = foundry_df.join(removed_rows_df.select('temp_id'),
                                     'temp_id',
                                     "left_anti")

    return foundry_df.drop('temp_id')


# Utility function that identifies bad rows is {domain_df} if {col_name}'s value is in list of {bad_col_vals}
# Returns filtered {bad_rows_df}, which has the same schema as domain_df + 'removal_reason' column
def get_bad_rows_by_column_val(domain_df, col_name, bad_col_vals, removal_reason):
    # Determine bad rows
    bad_rows_df = domain_df \
            .filter(F.col(col_name).isin(bad_col_vals)) \
            .withColumn('removal_reason', F.lit(removal_reason))

    return bad_rows_df


# The following list of codes to filter are consistent with the loinc_codes_to_remove dataset as of 2021-05-21
AIAN_CONCEPT_IDS = [8657]

PHI_LOINC_SOURCE_VALUES = [
    '42077-8'  # phone
    '45401-7',  # zip
    '56799-0',  # address
    '68997-6',  # city
    '87721-7',  # county
    '95727-4',  # Contact Name
    '95728-2',  # Contact Address
    '95729-0',  # Relationship to decedent Contact
    '95730-8',  # Surviving spouse name
    '95731-6',  # Mother's Maiden name
    '96676-2',  # Employer phone number
]

PHI_SNOMED_CONCEPT_IDS = [
    4160030,  # Performed
    4225106,  # Apocynum cannabinum
]

PHI_LOINC_CONCEPT_IDS = [
    723487,	 # Case identifier
    1005883,	 # Current street number
    1005885,	 # Current apartment number
    1006083,	 # Job number
    1007720,	 # Phone number
    1010139,	 # Coroner - medical examiner case number
    1011619,	 # Fax number
    1011621,	 # Phone number extension
    1011699,	 # Cell phone number
    1011700,	 # Home phone number
    1011701,	 # Business phone number extension
    1011702,	 # Business phone number
    1011713,	 # Primary contact phone number
    1011714,	 # Primary contact phone number extension
    1016713,	 # Agency patient number
    1017026,	 # Signature for Social Security Number request
    1029736,	 # Telephone number (HL7 datatype)
    1030404,	 # Death certificate number
    1030799,	 # Military patient number
    1030854,	 # Organ donor ID number
    1031060,	 # SEER record number
    1031826,	 # Tax ID number - IRS form W9 attachment
    1032600,	 # Medicaid number
    1032609,	 # Medical record number
    1032613,	 # Medicare or comparable number
    1032882,	 # Patient number
    1033184,	 # Social Security and Medicare numbers
    1033185,	 # Social Security number
    1034476,	 # Account number
    1175263,	 # Personnel name
    1175536,	 # Proof of encounter certificate
    1176184,	 # Personnel contact information panel
    1176216,	 # Patient Fax number
    1384421,	 # Consent PII: Verified Primary Phone Number
    1585916,	 # Secondary Contact Info: Person One Telephone
    1585932,	 # Secondary Contact Info: Second Contacts Number
    1585950,	 # Social Security: Social Security Number
    3000071,	 # Medical records
    3004992,	 # Death certificate number
    3005526,	 # Patient ID.hospital
    3005917,	 # Birthplace
    3008356,	 # Return visit post GI procedure [Telephone number]
    3010546,	 # History of Occupation Narrative
    3011079,	 # History of Occupation
    3012397,	 # Current employment - Reported
    3013195,	 # Maiden name
    3014497,	 # Military patient number
    3014722,	 # Date of death
    3019644,	 # Mother's hospital number
    3021690,	 # Hospital discharge date
    3021814,	 # Current employment Narrative - Reported
    3022535,	 # Fetal [Identifier] Identifier
    3023170,	 # Hospital admission date
    3023823,	 # Address at cancer diagnosis
    3028792,	 # Patient phone number
    3033158,	 # Archive facility identification number Cancer
    3034648,	 # Name of follow-up contact
    3036589,	 # Mother's name
    3038361,	 # Prior postal code [Location]
    3038919,	 # Patient Information
    3039081,	 # Ambulance transport, Other patient name
    3039620,	 # Driver license
    3040041,	 # Admission date
    3040204,	 # Ambulance transport, Other patient identifier
    3040601,	 # Responsible party, name CPHS
    3041838,	 # Member ID card copy
    3042020,	 # Patient escort information panel
    3042114,	 # Tax ID number - IRS form W9
    3042121,	 # Ambulance transport, Receiving individual accepting responsibility for patient Identifier
    3042411,	 # Discharge date
    3042530,	 # Middle name
    3042753,	 # Social Security and Medicare numbers
    3042888,	 # Middle initial
    3042942,	 # First (Given) name
    3043194,	 # Medicaid number
    3043495,	 # Medical record number [Identifier]
    3043579,	 # Postal code [Location]
    3043627,	 # Deprecated ZIP code of prior primary residence
    3043631,	 # Escort Name
    3043835,	 # Name suffix
    3043935,	 # Responsible party, home phone number CPHS
    3044681,	 # Organ donor ID number Narrative
    3044764,	 # Medicare or comparable number
    3045130,	 # Transfusion band number
    3045837,	 # Social Security number [Identifier]
    3046101,	 # Room number [Location]
    3046810,	 # Last (Family) name
    3046832,	 # Name Set
    3049734,	 # Agency patient number [CMS Assessment]
    3050054,	 # Deprecated Most recent inpatient discharge date [OASIS]
    3050626,	 # Nickname
    3050664,	 # Discharge, transfer, death date [CMS Assessment]
    3050959,	 # Name
    3051283,	 # Name Family member
    3051549,	 # Birth date Family member
    3103763,	 # Patient telephone number
    3236383,	 # Patient mobile telephone number
    3243339,	 # Passport number
    3244993,	 # Secondary Insurer identification number
    3262508,	 # Telephone number
    3263261,	 # Medical record number
    3270348,	 # Patient telephone number
    3270396,	 # Carer - mobile telephone number
    3277135,	 # Carer - work telephone number
    3293126,	 # Social security number
    3294287,	 # Patient home telephone number
    3300045,	 # Legal guardian - home telephone number
    3301454,	 # Primary Insurer Identification Number
    3302377,	 # Patient facsimile number
    3335394,	 # Patient hospital number
    3338042,	 # Patient work telephone number
    3349473,	 # Carer - home telephone number
    3358635,	 # Patient hospital visit number
    3361143,	 # Patient textphone number
    3397534,	 # Legal guardian - mobile telephone number
    3399155,	 # Legal guardian - work telephone number
    3420523,	 # Guarantor phone number
    3421847,	 # Identification number
    3523238,	 # Carer - home telephone number
    3523239,	 # Carer - work telephone number
    3523241,	 # Legal guardian - home telephone number
    3523242,	 # Legal guardian - mobile telephone number
    3523243,	 # Legal guardian - work telephone number
    3530059,	 # Patient mobile telephone number
    3530060,	 # Patient mobile telephone number
    3545223,	 # Mobile telephone number of informal carer
    3545224,	 # Work telephone number of informal carer
    3545225,	 # Home telephone number of informal carer
    3563220,	 # Patient mobile telephone number
    3565411,	 # Patient home telephone number
    3567034,	 # Patient work telephone number
    3571392,	 # Carer - mobile telephone number
    3571555,	 # Carer - home telephone number
    3571556,	 # Carer - mobile telephone number
    3571558,	 # Legal guardian - home telephone number
    3571559,	 # Legal guardian - work telephone number
    3573955,	 # Carer - work telephone number
    3573956,	 # Legal guardian - mobile telephone number
    4083592,	 # Patient telephone number
    4086934,	 # Patient hospital number
    4134571,	 # Guarantor phone number
    4134845,	 # Secondary Insurer ID number
    4161822,	 # Medical record number
    4162224,	 # Social security number
    4177383,	 # Patient mobile telephone number
    4177546,	 # Patient textphone number
    4179066,	 # Patient work telephone number
    4181157,	 # Patient home telephone number
    4182097,	 # Patient facsimile number
    4235257,	 # Caregiver work telephone number
    4235258,	 # Caregiver mobile telephone number
    4235417,	 # Legal guardian - home telephone number
    4235418,	 # Legal guardian - mobile telephone number
    4245003,	 # Identification number
    4247104,	 # Caregiver home telephone number
    4248674,	 # Legal guardian - work telephone number
    4287799,	 # Primary Insurer ID Number
    21492251,  # Resource identifier [URI] for Clinical document
    21492327,  # First name of Guardian or legally authorized representative
    21492328,  # Last name of Guardian or legally authorized representative
    21492865,  # Employer name [Identifier]
    21492866,  # Past employer name [Identifier]
    21492867,  # Employer address
    21492868,  # Past employer address
    21492869,  # Employer city
    21492870,  # Past employer city
    21492873,  # Employer postal code [Location]
    21492874,  # Past employer postal code [Location]
    21493403,  # Date and time pronounced dead [US Standard Certificate of Death]
    21494069,  # Birth certificate ID
    21494073,  # Death registration date
    21494075,  # Father's last name
    21494750,  # Date of death [Date]
    21494752,  # Date and time of death [TimeStamp]
    21498304,  # Driver license
    35918340,  # Social Security Number
    35918400,  # Patient ID Number
    35918643,  # Medical Record Number
    35918674,  # SEER Record Number
    36203969,  # U.S. standard certificate of death - recommended 2003 revision set
    36203971,  # U.S. standard certificate of live birth - recommended 2003 revision set
    36303303,  # Contact Phone number
    36303592,  # Contact email address
    36303873,  # Secondary contact name
    36303900,  # Insurance group number [Identifier]
    36304149,  # Birth registration date
    36304461,  # Legal name of patient - first and last
    36305281,  # County of residence [Location]
    36305883,  # Most recent inpatient discharge date in the last 14 days [CMS Assessment]
    36307315,  # Vendor serial number
    36659649,  # Intensive care unit (ICU) discharge date
    36659660,  # Anticipated discharge date
    36659697,  # Household contacts [#]
    36659961,  # Intensive care unit (ICU) admission date
    36660082,  # Workplace setting
    36660765,  # Intensive care unit (ICU) admission date &#x7C; Patient &#x7C; History
    36661103,  # Intensive care unit (ICU) discharge date &#x7C; Patient &#x7C; History
    36661745,  # Intensive care unit (ICU) admission date
    36661934,  # Intensive care unit (ICU) discharge date
    36716287,  # Patient hospital visit number
    37019501,  # Volunteer Phone number
    37019546,  # Email address Volunteer
    37019557,  # Shelter address Facility
    37019971,  # Volunteer Name
    37020075,  # Shelter name [Identifier] Facility
    37020165,  # Personnel Supervisor email address
    37020310,  # Latitude Event
    37020467,  # Personnel Supervisor address
    37020722,  # Donor contact information panel
    37020891,  # Longitude Event
    37020992,  # Father's administrative information
    37021086,  # Address Volunteer
    37021340,  # Supervisor phone number
    37021366,  # Volunteer contact information panel
    37021436,  # Personnel Birth date
    37021442,  # Personnel Supervisor contact information panel
    37021471,  # Personnel Supervisor name
    37021499,  # Name of Donor
    37108987,  # Passport number
    37118762,  # Telephone number (property)
    40318424,  # Patient telephone number
    40757163,  # CMS certification number (CCN) Provider
    40757164,  # Patient Identification Number or Provider Account Number
    40757634,  # Legal name of patient
    40757635,  # Deprecated Social Security and Medicare numbers
    40758304,  # Patient identification data Set DEEDS
    40759913,  # Internal identifier
    40759914,  # Account number [Identifier]
    40759915,  # Emergency contact information panel
    40759916,  # Episode unique identifier
    40759918,  # Address
    40759980,  # Emergency contact Name
    40759981,  # Emergency contact Address
    40759982,  # Emergency contact Phone number
    40765784,  # PhenX - current address protocol 010801
    40766219,  # Deprecated Birthplace [PhenX]
    40766220,  # Father birthplace
    40766225,  # Current street number [Location] [PhenX]
    40766226,  # Current street name [Location] [PhenX]
    40766227,  # Current apartment number [Location] [PhenX]
    40766228,  # Current city [PhenX]
    40766230,  # Deprecated Current zip code [Location] [PhenX]
    40766449,  # From what date did you live in the residence
    40766450,  # To what date did you live in the residence
    40766451,  # What is the residence address, street - crosstreets, city, state or landmark [PEG]
    40766476,  # What was the name of the company where you first or next worked, for 6 months or longer [NHANES]
    40767001,  # Health insurance card
    40767567,  # In what city or town were you living when you were 18 [PhenX]
    40767568,  # To what city or town did you move to next [PhenX]
    40767570,  # City [PhenX]
    40768957,  # School Name
    40768976,  # Participant [Identifier]
    40770922,  # CMS certification number - CCN Dialysis facility
    40771522,  # City
    40771523,  # U.S. standard certificate of live birth - 2003 revision
    40771926,  # U.S. standard certificate of death - 2003 revision
    40771942,  # Street address where death occurred if not facility
    40771959,  # Coroner - medical examiner case number
    42528934,  # Participant identifier
    42869435,  # Personnel Job title
    42869477,  # Personnel Address
    42869479,  # Personnel Cell phone number
    42869480,  # Personnel Home phone number
    42869481,  # Personnel Business phone number extension
    42869482,  # Personnel Business phone number
    42869483,  # Personnel Fax number
    42869484,  # Personnel Email address
    42869485,  # Personnel Nine-digit postal code
    42869486,  # Personnel Five-digit postal code
    42869487,  # Personnel State
    42869488,  # Personnel City
    42869489,  # Personnel Address 2
    42869490,  # Personnel Preferred mailing address
    42869493,  # Personnel Name suffix
    42869494,  # Personnel Middle initial
    42869495,  # Personnel First (Given) name
    42869496,  # Personnel Last (Family) name
    42869504,  # Personnel unique identifier
    42870173,  # Birth certificate
    42870767,  # Health insurance card
    42870769,  # Death certificate
    42870862,  # Birth certificate
    44786681,  # Family member identifier
    44786682,  # Mother identifier
    44786683,  # Father identifier
    44786684,  # Family pedigree identifier
    44786930,  # Occupation [NTDS]
    44786931,  # Occupation industry [NTDS]
    44787825,  # Social services identification number
    44788529,  # Patient community health index number
    44794716,  # Patient NHS number
    44807166,  # Mobile telephone number of informal carer
    44807167,  # Work telephone number of informal carer
    44814101,  # Home telephone number of informal carer
    44816609,  # Unique device identifier
    45439568,  # Hospital reference number:
    45442757,  # Carer - mobile telephone number
    45499494,  # Legal guardian - home telephone number
    45502821,  # Carer - home telephone number
    45506090,  # Patient mobile telephone number
    45509452,  # Home telephone number of informal carer
    45891921,  # Patient mobile telephone number ? correct
    45919276,  # Birth notification number
    45921880,  # Telephone number of treatment supporter
    45941845,  # contact phone number
    45941972,  # EMERGENCY NUMBER
    45942279,  # Identification number of family member
    45946681,  # Birth certificate number
    46234739,  # Patient identifier
    46234749,  # Patient Email address
    46234758,  # Organization episode of care unique identifier
    46235132,  # Encounter identifier
    46235918,  # Physical therapy Discharge date
    46235921,  # Episode of care unique identifier
]

POSSIBLE_PHI_LOINC_SOURCE_VALUES = [
    '96677-0',  # School Address
    '96678-8',  # School code [ID]
    '96679-6',  # School Identifier assigning authority
]

POSSIBLE_PHI_LOINC_CONCEPT_IDS = [
    723486,  # Contact case identifier
    723488,  # Contact identifier Contact
    1001576,  # Funeral service licensee identifier Facility
    1001979,  # Personnel Funeral service licensee or other agent assuming custody of the body of the deceased
    1002077,  # Funeral facility name [Identifier]
    1002155,  # Body disposition facility address
    1002227,  # Funeral facility address
    1002271,  # Appointment of legal guardian
    1002326,  # Body disposition facility name [Identifier]
    1002540,  # Health insurance card &#x7C; {Setting} &#x7C; Document ontology
    1002761,  # Driver license &#x7C; {Setting} &#x7C; Document ontology
    1003144,  # Birth certificate &#x7C; {Setting} &#x7C; Document ontology
    1003650,  # Certificate &#x7C; {Setting} &#x7C; Document ontology
    1004021,  # Death certificate | {Setting} | Document ontology
    1008209,  # Last four digits of actigraph serial number
    1012530,  # Reporter phone number
    1017024,  # Date of Social Security Number request
    1017025,  # Social Security Number was requested
    1018120,  # Proof of encounter certificate
    1018173,  # Incident commander phone number
    1018349,  # Chief of staff phone number
    1025840,  # Supervisor phone number
    1025891,  # Institution inventory number
    1030184,  # Archive facility identification number
    1030185,  # Archive facility identification number NPI
    1030345,  # Creator report number
    1031775,  # Responsible party
    1031939,  # Medicaid provider number
    1031940,  # CMS certification number (CCN)
    1175229,  # Response team leader title
    1175239,  # Chief of staff email address
    1175269,  # Email address of person responsible for generating report
    1175304,  # Personnel Person assigned to task
    1175347,  # Incident commander email address
    1175433,  # Date of laboratory phone call to provider to communicate test result
    1175544,  # Response team member title
    1175786,  # Task description Narrative
    1175819,  # Name of Contact at Organization
    1175994,  # Personnel Job position
    1176017,  # Person responsible for generating report
    1176023,  # Incident commander phone number
    1176051,  # Chief of staff phone number
    1176323,  # Response team member name
    1176342,  # Deprecated Email address Personnel
    1176371,  # Task identifier
    1176398,  # Chief of staff name
    1176436,  # Response team leader name
    1176483,  # Incident commander name
    3006440,  # Date form completed
    3006878,  # Ambulance transport, Destination site Address
    3017255,  # Ambulance transport, Origination site Address
    3017920,  # Deprecated Psychiatric rehabilitation treatment plan, Author ID Identifier
    3038286,  # Follow-up (referred to) program, address CPHS
    3040127,  # Follow-up (referred to) program, name CPHS
    3040253,  # Ambulance transport, Origination site name and address
    3040714,  # Follow-up (referred to) provider /specialist, address CPHS
    3041026,  # Responsible party
    3041523,  # Ambulance transport, Destination site name and address
    3041900,  # Program participation, name CPHS
    3042191,  # Follow-up (referred to) provider /specialist, name CPHS
    3042213,  # Screen test, other name CPHS
    3042319,  # Follow-up (referred to) program, phone CPHS
    3042600,  # Follow-up (referred to) provider /specialist, phone CPHS
    3046216,  # HIV treatment prior clinic transferred from Address
    3046792,  # HIV treatment clinic transferred to Address
    3049369,  # CMS certification number (CCN) Agency [OASIS]
    3051930,  # Record ID section Set NAACCR v.11
    3276839,  # Hospital reference number:
    3557105,  # Opportunistic verification of patient mobile telephone number
    4087914,  # Hospital reference number
    21491534,  # Death arrangements [Reported]
    21493034,  # First witness
    21493035,  # Second witness
    21493036,  # Third witness
    21493269,  # Certificate
    36203486,  # Occupation industry CDC Census code
    36203487,  # Occupation [Type]
    36203488,  # Occupation CDC Census code
    36203783,  # Death administrative information Document
    36303301,  # Date of fetal death registration
    36303317,  # Billing information panel
    36303558,  # Immigration status
    36304266,  # Date of birth certification
    36304445,  # Social Security Number was requested
    36304448,  # Data items collected at inpatient facility admission or agency discharge only - discharge from agency - death at home [CMS Assessment]
    36304473,  # Guardian or legally authorized representative signature for Social Security Number request
    36304588,  # WIC services recipient
    36305073,  # Date of Social Security Number request
    36305223,  # Title of person completing the form Provider
    36305630,  # OASIS D - Data items collected at inpatient facility admission or agency discharge only - discharge from agency - death at home [CMS Assessment]
    36306117,  # Outcome and assessment information set (OASIS) form - version D, D1 - Discharged from agency - death at home [CMS Assessment]
    36307059,  # Vendor model number
    36660024,  # Affiliated with a tribe
    36660123,  # Enrolled in a tribe
    36660477,  # Tribal affiliation
    37019517,  # Reporter Information
    37020103,  # Response team name
    37020138,  # Document name [Identifier]
    37020209,  # Primary contact information panel Facility
    37020329,  # Provider supplied facility fetal death report Document
    37020333,  # Personnel Response team leader information panel
    37020433,  # Date and time of risk assessment meeting
    37020493,  # Volunteer age
    37020713,  # Date of interview
    37020851,  # Provider supplied mother's fetal death report Document
    37020926,  # Jurisdiction fetal death report Document
    37021009,  # Supply owner [Identifier] Supply
    37021123,  # Location of meeting
    37021237,  # Personnel Job experience
    37021394,  # Employment net income in past 30 days
    37021525,  # Address type
    40763560,  # Record ID section Set NAACCR v.12
    40765110,  # FISH probe name panel - Laboratory device
    40767008,  # Death certificate
    40769166,  # Image name Hand [PhenX]
    40769169,  # Image name Knee [PhenX]
    40769172,  # Image name Hip [PhenX]
    40769226,  # Image name Femur [PhenX]
    40769227,  # Image name Spine [PhenX]
    40770480,  # Tribal enrollment
    40771961,  # Death date comment
    40773022,  # Patient information
    40791217,  # Summary of death note
    42527955,  # Occupation industry [Type]
    42527981,  # Compensation and sector employment type
    42528777,  # Clinical data [Date and Time Range]
    42869491,  # Personnel Credentials
    42869492,  # Personnel Name prefix
    42869505,  # Primary contact phone number extension Facility
    42869506,  # Primary contact phone number Facility
    42869507,  # Primary contact first name Facility
    42869508,  # Primary contact last name Facility
    42870863,  # Certificate
    43533790,  # Place where birth occurred [US Standard Certificate of Live Birth]
    43533798,  # Onset of labor [US Standard Certificate of Live Birth]
    44802544,  # Opportunistic verification of patient mobile telephone number
    44816608,  # Asset tag number
    44816611,  # Other unique product identifier [Type]
    44816624,  # Other unique product identifier
    44816632,  # Death certifier details
    44817173,  # Death pronouncer details
    44817219,  # Reporter email
    44817220,  # Reporter phone number
    45917764,  # REGISTER SERIAL NUMBER
    45949764,  # Service delivery point number
    46235581,  # Primary healthcare agent [Reported]
    46235582,  # First alternate healthcare agent [Reported]
    46235583,  # Second alternate healthcare agent [Reported]
    46236889,  # Reporting county
    46236893,  # Earliest date reported to county
    46236902,  # Case outbreak name
    46236908,  # County of exposure to illness [Location]
]
