CREATE TABLE `/UNITE/CMS Raw Data Parsing - RWD Pipeline - N3C COVID Replica/raw/trimmed/pde_trim` AS
    SELECT 
    BID,  --replace with n3c ID
    --hash(concat('djeiu908-', RECID)) as pseudo_recid_id,
    RECID,
    RX_DOS_DT,
    FILL_NUM,
    COMPUND_CD,
    DAW_CD,
    QUANTITY_DISPENSED,
    DAYS_SUPPLY,
    COVERAGE_CD,
    NON_STAND_FMT_CD,
    PAID_DT,
    COVERAGE_STAT_CD,
    PROD_SERVICE_ID,
    case when SRVC_PROVIDER_ID='' then '' else hash(SRVC_PROVIDER_ID+4987652) end as pseudo_provider_id,
    case when PRESCRIBER_ID='' then '' else hash(PRESCRIBER_ID+4987652) end as pseudo_prescriber_id,
    BRND_GNRC_CD,
    BGNG_BNFT_PHASE,
    ENDG_BNFT_PHASE,
    FRMLRY_CD,
    PHARM_SVC_TYPE,
    TOTAL_CST 
 FROM `/UNITE/CMS Raw Data Parsing - RWD Pipeline - N3C COVID Replica/CMS Medicare Parsing/transform/02 - schema applied/pde`
 order by BID, RECID, RX_DOS_DT