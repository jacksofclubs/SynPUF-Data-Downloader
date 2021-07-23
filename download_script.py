################################################################################
##### CMS 2008-2010 Data Entrepreneurs Synthetic PUF Import Script
################################################################################

import pymysql
import os
import zipfile
from urllib.request import urlopen

################################################################################ 
##### Connect to database
################################################################################

# @TODO ask user for username and password
# @TODO don't connect to database in the initial connection
# @TODO remove hardcoded username and passwords
print('Connecting to database')
conn = pymysql.connect(host='localhost', 
                       user='',
                       passwd='',
                       db='synpuf',
                       local_infile=True)
print('Connection established')

c = conn.cursor()

# @TODO create database if not exists and connect to database

################################################################################
##### Drop existing tables
################################################################################

print('Deleting existing tables')
c.execute('DROP TABLE IF EXISTS beneficiary_summary')
c.execute('DROP TABLE IF EXISTS ip_claims')
c.execute('DROP TABLE IF EXISTS op_claims')
c.execute('DROP TABLE IF EXISTS carrier_claims')
c.execute('DROP TABLE IF EXISTS prescription_drug_events')
print('Existing tables deleted')

################################################################################
##### Create tables
################################################################################

print('Creating tables')
stmt = """ CREATE TABLE beneficiary_summary (
             DESYNPUF_ID VARCHAR(20) NOT NULL, 
             BENE_BIRTH_DT INT(8), 
             BENE_DEATH_DT INT(8), 
             BENE_SEX_IDENT_CD CHAR(1), 
             BENE_RACE_CD CHAR(1), 
             BENE_ESRD_IND CHAR(1), 
             SP_STATE_CODE CHAR(2), 
             BENE_COUNTY_CD CHAR(3), 
             BENE_HI_CVRAGE_TOT_MONS TINYINT(2), 
             BENE_SMI_CVRAGE_TOT_MONS TINYINT(2), 
             BENE_HMO_CVRAGE_TOT_MONS TINYINT(2), 
             PLAN_CVRG_MOS_NUM TINYINT(2), 
             SP_ALZHDMTA TINYINT(2), 
             SP_CHF TINYINT(2), 
             SP_CHRNKIDN TINYINT(2), 
             SP_CNCR TINYINT(2), 
             SP_COPD TINYINT(2), 
             SP_DEPRESSN TINYINT(2), 
             SP_DIABETES TINYINT(2), 
             SP_ISCHMCHT TINYINT(2), 
             SP_OSTEOPRS TINYINT(2), 
             SP_RA_OA TINYINT(2), 
             SP_STRKETIA TINYINT(2), 
             MEDREIMB_IP FLOAT(10,2), 
             BENRES_IP FLOAT(10,2), 
             PPPYMT_IP FLOAT(10,2), 
             MEDREIMB_OP FLOAT(10,2), 
             BENRES_OP FLOAT(10,2), 
             PPPYMT_OP FLOAT(10,2), 
             MEDREIMB_CAR FLOAT(10,2), 
             BENRES_CAR FLOAT(10,2), 
             PPPYMT_CAR FLOAT(10,2)) """
c.execute(stmt)

stmt = """ CREATE TABLE ip_claims (
             DESYNPUF_ID VARCHAR(20) NOT NULL, 
             CLM_ID VARCHAR(20), 
             SEGMENT VARCHAR(20), 
             CLM_FROM_DT INT(8), 
             CLM_THRU_DT INT(8), 
             PRVDR_NUM VARCHAR(20), 
             CLM_PMT_AMT FLOAT(10,2), 
             NCH_PRMRY_PYR_CLM_PD_AMT FLOAT(10,2), 
             AT_PHYSN_NPI VARCHAR(20), 
             OP_PHYSN_NPI VARCHAR(20), 
             OT_PHYSN_NPI VARCHAR(20), 
             CLM_ADMSN_DT INT(8), 
             ADMTNG_ICD9_DGNS_CD VARCHAR(20), 
             CLM_PASS_THRU_PER_DIEM_AMT FLOAT(10,2), 
             NCH_BENE_IP_DDCTBL_AMT FLOAT(10,2), 
             NCH_BENE_PTA_COINSRNC_LBLTY_AM FLOAT(10,2), 
             NCH_BENE_BLOOD_DDCTBL_LBLTY_AM FLOAT(10,2), 
             CLM_UTLZTN_DAY_CNT INT(8), 
             NCH_BENE_DSCHRG_DT INT(8), 
             CLM_DRG_CD VARCHAR(20), 
             ICD9_DGNS_CD_1 VARCHAR(20), 
             ICD9_DGNS_CD_2 VARCHAR(20), 
             ICD9_DGNS_CD_3 VARCHAR(20), 
             ICD9_DGNS_CD_4 VARCHAR(20), 
             ICD9_DGNS_CD_5 VARCHAR(20), 
             ICD9_DGNS_CD_6 VARCHAR(20), 
             ICD9_DGNS_CD_7 VARCHAR(20), 
             ICD9_DGNS_CD_8 VARCHAR(20), 
             ICD9_DGNS_CD_9 VARCHAR(20), 
             ICD9_DGNS_CD_10 VARCHAR(20), 
             ICD9_PRCDR_CD_1 VARCHAR(20), 
             ICD9_PRCDR_CD_2 VARCHAR(20), 
             ICD9_PRCDR_CD_3 VARCHAR(20), 
             ICD9_PRCDR_CD_4 VARCHAR(20), 
             ICD9_PRCDR_CD_5 VARCHAR(20), 
             ICD9_PRCDR_CD_6 VARCHAR(20), 
             HCPCS_CD_1 VARCHAR(20), 
             HCPCS_CD_2 VARCHAR(20), 
             HCPCS_CD_3 VARCHAR(20), 
             HCPCS_CD_4 VARCHAR(20), 
             HCPCS_CD_5 VARCHAR(20), 
             HCPCS_CD_6 VARCHAR(20), 
             HCPCS_CD_7 VARCHAR(20), 
             HCPCS_CD_8 VARCHAR(20), 
             HCPCS_CD_9 VARCHAR(20), 
             HCPCS_CD_10 VARCHAR(20), 
             HCPCS_CD_11 VARCHAR(20), 
             HCPCS_CD_12 VARCHAR(20), 
             HCPCS_CD_13 VARCHAR(20), 
             HCPCS_CD_14 VARCHAR(20), 
             HCPCS_CD_15 VARCHAR(20), 
             HCPCS_CD_16 VARCHAR(20), 
             HCPCS_CD_17 VARCHAR(20), 
             HCPCS_CD_18 VARCHAR(20), 
             HCPCS_CD_19 VARCHAR(20), 
             HCPCS_CD_20 VARCHAR(20), 
             HCPCS_CD_21 VARCHAR(20), 
             HCPCS_CD_22 VARCHAR(20), 
             HCPCS_CD_23 VARCHAR(20), 
             HCPCS_CD_24 VARCHAR(20), 
             HCPCS_CD_25 VARCHAR(20), 
             HCPCS_CD_26 VARCHAR(20), 
             HCPCS_CD_27 VARCHAR(20), 
             HCPCS_CD_28 VARCHAR(20), 
             HCPCS_CD_29 VARCHAR(20), 
             HCPCS_CD_30 VARCHAR(20), 
             HCPCS_CD_31 VARCHAR(20), 
             HCPCS_CD_32 VARCHAR(20), 
             HCPCS_CD_33 VARCHAR(20), 
             HCPCS_CD_34 VARCHAR(20), 
             HCPCS_CD_35 VARCHAR(20), 
             HCPCS_CD_36 VARCHAR(20), 
             HCPCS_CD_37 VARCHAR(20), 
             HCPCS_CD_38 VARCHAR(20), 
             HCPCS_CD_39 VARCHAR(20), 
             HCPCS_CD_40 VARCHAR(20), 
             HCPCS_CD_41 VARCHAR(20), 
             HCPCS_CD_42 VARCHAR(20), 
             HCPCS_CD_43 VARCHAR(20), 
             HCPCS_CD_44 VARCHAR(20), 
             HCPCS_CD_45 VARCHAR(20)) """
c.execute(stmt)

stmt = """ CREATE TABLE op_claims (
             DESYNPUF_ID VARCHAR(20) NOT NULL, 
             CLM_ID VARCHAR(20), 
             SEGMENT VARCHAR(20), 
             CLM_FROM_DT INT(8), 
             CLM_THRU_DT INT(8), 
             PRVDR_NUM VARCHAR(20), 
             CLM_PMT_AMT FLOAT(10,2), 
             NCH_PRMRY_PYR_CLM_PD_AMT FLOAT(10,2), 
             AT_PHYSN_NPI VARCHAR(20), 
             OP_PHYSN_NPI VARCHAR(20), 
             OT_PHYSN_NPI VARCHAR(20), 
             NCH_BENE_BLOOD_DDCTBL_LBLTY_AM FLOAT(10,2), 
             ICD9_DGNS_CD_1 VARCHAR(20), 
             ICD9_DGNS_CD_2 VARCHAR(20), 
             ICD9_DGNS_CD_3 VARCHAR(20), 
             ICD9_DGNS_CD_4 VARCHAR(20), 
             ICD9_DGNS_CD_5 VARCHAR(20), 
             ICD9_DGNS_CD_6 VARCHAR(20), 
             ICD9_DGNS_CD_7 VARCHAR(20), 
             ICD9_DGNS_CD_8 VARCHAR(20), 
             ICD9_DGNS_CD_9 VARCHAR(20), 
             ICD9_DGNS_CD_10 VARCHAR(20), 
             ICD9_PRCDR_CD_1 VARCHAR(20), 
             ICD9_PRCDR_CD_2 VARCHAR(20), 
             ICD9_PRCDR_CD_3 VARCHAR(20), 
             ICD9_PRCDR_CD_4 VARCHAR(20), 
             ICD9_PRCDR_CD_5 VARCHAR(20), 
             ICD9_PRCDR_CD_6 VARCHAR(20), 
             NCH_BENE_PTB_DDCTBL_AMT FLOAT(10,2), 
             NCH_BENE_PTB_COINSRNC_AMT FLOAT(10,2), 
             ADMTNG_ICD9_DGNS_CD VARCHAR(20), 
             HCPCS_CD_1 VARCHAR(20), 
             HCPCS_CD_2 VARCHAR(20), 
             HCPCS_CD_3 VARCHAR(20), 
             HCPCS_CD_4 VARCHAR(20), 
             HCPCS_CD_5 VARCHAR(20), 
             HCPCS_CD_6 VARCHAR(20), 
             HCPCS_CD_7 VARCHAR(20), 
             HCPCS_CD_8 VARCHAR(20), 
             HCPCS_CD_9 VARCHAR(20), 
             HCPCS_CD_10 VARCHAR(20), 
             HCPCS_CD_11 VARCHAR(20), 
             HCPCS_CD_12 VARCHAR(20), 
             HCPCS_CD_13 VARCHAR(20), 
             HCPCS_CD_14 VARCHAR(20), 
             HCPCS_CD_15 VARCHAR(20), 
             HCPCS_CD_16 VARCHAR(20), 
             HCPCS_CD_17 VARCHAR(20), 
             HCPCS_CD_18 VARCHAR(20), 
             HCPCS_CD_19 VARCHAR(20), 
             HCPCS_CD_20 VARCHAR(20), 
             HCPCS_CD_21 VARCHAR(20), 
             HCPCS_CD_22 VARCHAR(20), 
             HCPCS_CD_23 VARCHAR(20), 
             HCPCS_CD_24 VARCHAR(20), 
             HCPCS_CD_25 VARCHAR(20), 
             HCPCS_CD_26 VARCHAR(20), 
             HCPCS_CD_27 VARCHAR(20), 
             HCPCS_CD_28 VARCHAR(20), 
             HCPCS_CD_29 VARCHAR(20), 
             HCPCS_CD_30 VARCHAR(20), 
             HCPCS_CD_31 VARCHAR(20), 
             HCPCS_CD_32 VARCHAR(20), 
             HCPCS_CD_33 VARCHAR(20), 
             HCPCS_CD_34 VARCHAR(20), 
             HCPCS_CD_35 VARCHAR(20), 
             HCPCS_CD_36 VARCHAR(20), 
             HCPCS_CD_37 VARCHAR(20), 
             HCPCS_CD_38 VARCHAR(20), 
             HCPCS_CD_39 VARCHAR(20), 
             HCPCS_CD_40 VARCHAR(20), 
             HCPCS_CD_41 VARCHAR(20), 
             HCPCS_CD_42 VARCHAR(20), 
             HCPCS_CD_43 VARCHAR(20), 
             HCPCS_CD_44 VARCHAR(20), 
             HCPCS_CD_45 VARCHAR(20)) """
c.execute(stmt)

stmt = """ CREATE TABLE carrier_claims (
             DESYNPUF_ID VARCHAR(20) NOT NULL, 
             CLM_ID VARCHAR(20), 
             CLM_FROM_DT INT(8), 
             CLM_THRU_DT INT(8), 
             ICD9_DGNS_CD_1 VARCHAR(20), 
             ICD9_DGNS_CD_2 VARCHAR(20), 
             ICD9_DGNS_CD_3 VARCHAR(20), 
             ICD9_DGNS_CD_4 VARCHAR(20), 
             ICD9_DGNS_CD_5 VARCHAR(20), 
             ICD9_DGNS_CD_6 VARCHAR(20), 
             ICD9_DGNS_CD_7 VARCHAR(20), 
             ICD9_DGNS_CD_8 VARCHAR(20), 
             PRF_PHYSN_NPI_1 VARCHAR(20), 
             PRF_PHYSN_NPI_2 VARCHAR(20), 
             PRF_PHYSN_NPI_3 VARCHAR(20), 
             PRF_PHYSN_NPI_4 VARCHAR(20), 
             PRF_PHYSN_NPI_5 VARCHAR(20), 
             PRF_PHYSN_NPI_6 VARCHAR(20), 
             PRF_PHYSN_NPI_7 VARCHAR(20), 
             PRF_PHYSN_NPI_8 VARCHAR(20), 
             PRF_PHYSN_NPI_9 VARCHAR(20), 
             PRF_PHYSN_NPI_10 VARCHAR(20), 
             PRF_PHYSN_NPI_11 VARCHAR(20), 
             PRF_PHYSN_NPI_12 VARCHAR(20), 
             PRF_PHYSN_NPI_13 VARCHAR(20), 
             TAX_NUM_1 VARCHAR(20), 
             TAX_NUM_2 VARCHAR(20), 
             TAX_NUM_3 VARCHAR(20), 
             TAX_NUM_4 VARCHAR(20), 
             TAX_NUM_5 VARCHAR(20), 
             TAX_NUM_6 VARCHAR(20), 
             TAX_NUM_7 VARCHAR(20), 
             TAX_NUM_8 VARCHAR(20), 
             TAX_NUM_9 VARCHAR(20), 
             TAX_NUM_10 VARCHAR(20), 
             TAX_NUM_11 VARCHAR(20), 
             TAX_NUM_12 VARCHAR(20), 
             TAX_NUM_13 VARCHAR(20), 
             HCPCS_CD_1 VARCHAR(20), 
             HCPCS_CD_2 VARCHAR(20), 
             HCPCS_CD_3 VARCHAR(20), 
             HCPCS_CD_4 VARCHAR(20), 
             HCPCS_CD_5 VARCHAR(20), 
             HCPCS_CD_6 VARCHAR(20), 
             HCPCS_CD_7 VARCHAR(20), 
             HCPCS_CD_8 VARCHAR(20), 
             HCPCS_CD_9 VARCHAR(20), 
             HCPCS_CD_10 VARCHAR(20), 
             HCPCS_CD_11 VARCHAR(20), 
             HCPCS_CD_12 VARCHAR(20), 
             HCPCS_CD_13 VARCHAR(20), 
             LINE_NCH_PMT_AMT_1 FLOAT(10,2), 
             LINE_NCH_PMT_AMT_2 FLOAT(10,2), 
             LINE_NCH_PMT_AMT_3 FLOAT(10,2), 
             LINE_NCH_PMT_AMT_4 FLOAT(10,2), 
             LINE_NCH_PMT_AMT_5 FLOAT(10,2), 
             LINE_NCH_PMT_AMT_6 FLOAT(10,2), 
             LINE_NCH_PMT_AMT_7 FLOAT(10,2), 
             LINE_NCH_PMT_AMT_8 FLOAT(10,2), 
             LINE_NCH_PMT_AMT_9 FLOAT(10,2), 
             LINE_NCH_PMT_AMT_10 FLOAT(10,2), 
             LINE_NCH_PMT_AMT_11 FLOAT(10,2), 
             LINE_NCH_PMT_AMT_12 FLOAT(10,2), 
             LINE_NCH_PMT_AMT_13 FLOAT(10,2), 
             LINE_BENE_PTB_DDCTBL_AMT_1 FLOAT(10,2), 
             LINE_BENE_PTB_DDCTBL_AMT_2 FLOAT(10,2), 
             LINE_BENE_PTB_DDCTBL_AMT_3 FLOAT(10,2), 
             LINE_BENE_PTB_DDCTBL_AMT_4 FLOAT(10,2), 
             LINE_BENE_PTB_DDCTBL_AMT_5 FLOAT(10,2), 
             LINE_BENE_PTB_DDCTBL_AMT_6 FLOAT(10,2), 
             LINE_BENE_PTB_DDCTBL_AMT_7 FLOAT(10,2), 
             LINE_BENE_PTB_DDCTBL_AMT_8 FLOAT(10,2), 
             LINE_BENE_PTB_DDCTBL_AMT_9 FLOAT(10,2), 
             LINE_BENE_PTB_DDCTBL_AMT_10 FLOAT(10,2), 
             LINE_BENE_PTB_DDCTBL_AMT_11 FLOAT(10,2), 
             LINE_BENE_PTB_DDCTBL_AMT_12 FLOAT(10,2), 
             LINE_BENE_PTB_DDCTBL_AMT_13 FLOAT(10,2), 
             LINE_BENE_PRMRY_PYR_PD_AMT_1 FLOAT(10,2), 
             LINE_BENE_PRMRY_PYR_PD_AMT_2 FLOAT(10,2), 
             LINE_BENE_PRMRY_PYR_PD_AMT_3 FLOAT(10,2), 
             LINE_BENE_PRMRY_PYR_PD_AMT_4 FLOAT(10,2), 
             LINE_BENE_PRMRY_PYR_PD_AMT_5 FLOAT(10,2), 
             LINE_BENE_PRMRY_PYR_PD_AMT_6 FLOAT(10,2), 
             LINE_BENE_PRMRY_PYR_PD_AMT_7 FLOAT(10,2), 
             LINE_BENE_PRMRY_PYR_PD_AMT_8 FLOAT(10,2), 
             LINE_BENE_PRMRY_PYR_PD_AMT_9 FLOAT(10,2), 
             LINE_BENE_PRMRY_PYR_PD_AMT_10 FLOAT(10,2), 
             LINE_BENE_PRMRY_PYR_PD_AMT_11 FLOAT(10,2), 
             LINE_BENE_PRMRY_PYR_PD_AMT_12 FLOAT(10,2), 
             LINE_BENE_PRMRY_PYR_PD_AMT_13 FLOAT(10,2), 
             LINE_COINSRNC_AMT_1 FLOAT(10,2), 
             LINE_COINSRNC_AMT_2 FLOAT(10,2), 
             LINE_COINSRNC_AMT_3 FLOAT(10,2), 
             LINE_COINSRNC_AMT_4 FLOAT(10,2), 
             LINE_COINSRNC_AMT_5 FLOAT(10,2), 
             LINE_COINSRNC_AMT_6 FLOAT(10,2), 
             LINE_COINSRNC_AMT_7 FLOAT(10,2), 
             LINE_COINSRNC_AMT_8 FLOAT(10,2), 
             LINE_COINSRNC_AMT_9 FLOAT(10,2), 
             LINE_COINSRNC_AMT_10 FLOAT(10,2), 
             LINE_COINSRNC_AMT_11 FLOAT(10,2), 
             LINE_COINSRNC_AMT_12 FLOAT(10,2), 
             LINE_COINSRNC_AMT_13 FLOAT(10,2), 
             LINE_ALOWD_CHRG_AMT_1 FLOAT(10,2), 
             LINE_ALOWD_CHRG_AMT_2 FLOAT(10,2), 
             LINE_ALOWD_CHRG_AMT_3 FLOAT(10,2), 
             LINE_ALOWD_CHRG_AMT_4 FLOAT(10,2), 
             LINE_ALOWD_CHRG_AMT_5 FLOAT(10,2), 
             LINE_ALOWD_CHRG_AMT_6 FLOAT(10,2), 
             LINE_ALOWD_CHRG_AMT_7 FLOAT(10,2), 
             LINE_ALOWD_CHRG_AMT_8 FLOAT(10,2), 
             LINE_ALOWD_CHRG_AMT_9 FLOAT(10,2), 
             LINE_ALOWD_CHRG_AMT_10 FLOAT(10,2), 
             LINE_ALOWD_CHRG_AMT_11 FLOAT(10,2), 
             LINE_ALOWD_CHRG_AMT_12 FLOAT(10,2), 
             LINE_ALOWD_CHRG_AMT_13 FLOAT(10,2), 
             LINE_PRCSG_IND_CD_1 VARCHAR(2), 
             LINE_PRCSG_IND_CD_2 VARCHAR(2), 
             LINE_PRCSG_IND_CD_3 VARCHAR(2), 
             LINE_PRCSG_IND_CD_4 VARCHAR(2), 
             LINE_PRCSG_IND_CD_5 VARCHAR(2), 
             LINE_PRCSG_IND_CD_6 VARCHAR(2), 
             LINE_PRCSG_IND_CD_7 VARCHAR(2), 
             LINE_PRCSG_IND_CD_8 VARCHAR(2), 
             LINE_PRCSG_IND_CD_9 VARCHAR(2), 
             LINE_PRCSG_IND_CD_10 VARCHAR(2), 
             LINE_PRCSG_IND_CD_11 VARCHAR(2), 
             LINE_PRCSG_IND_CD_12 VARCHAR(2), 
             LINE_PRCSG_IND_CD_13 VARCHAR(2), 
             LINE_ICD9_DGNS_CD_1 VARCHAR(20), 
             LINE_ICD9_DGNS_CD_2 VARCHAR(20), 
             LINE_ICD9_DGNS_CD_3 VARCHAR(20), 
             LINE_ICD9_DGNS_CD_4 VARCHAR(20), 
             LINE_ICD9_DGNS_CD_5 VARCHAR(20), 
             LINE_ICD9_DGNS_CD_6 VARCHAR(20), 
             LINE_ICD9_DGNS_CD_7 VARCHAR(20), 
             LINE_ICD9_DGNS_CD_8 VARCHAR(20), 
             LINE_ICD9_DGNS_CD_9 VARCHAR(20), 
             LINE_ICD9_DGNS_CD_10 VARCHAR(20), 
             LINE_ICD9_DGNS_CD_11 VARCHAR(20), 
             LINE_ICD9_DGNS_CD_12 VARCHAR(20), 
             LINE_ICD9_DGNS_CD_13 VARCHAR(20)) """
c.execute(stmt)

stmt = """ CREATE TABLE prescription_drug_events (
             DESYNPUF_ID VARCHAR(20) NOT NULL, 
             PDE_ID VARCHAR(20), 
             SRVC_DT INT(8), 
             PROD_SRVC_ID VARCHAR(20), 
             QTY_DSPNSD_NUM FLOAT, 
             DAYS_SUPLY_NUM INT(11), 
             PTNT_PAY_AMT FLOAT, 
             TOT_RX_CST_AMT FLOAT) """
c.execute(stmt)
print('Tables created')

################################################################################
##### Functions to download and unzip files
################################################################################

def download_file(file_name, file_path):
    if not os.path.exists(file_name) :
        print('Downloading file from  %s' % file_path)
        u = urlopen(file_path)
        localFile = open(file_name, 'wb')
        localFile.write(u.read())
        localFile.close()
        print('Downloaded file  %s' % file_name)
    else :
        print('File %s exists. Skipping download.' % file_name)

def unzip_file(file_name, csv_file):
    if not os.path.exists(csv_file) :
        zip_file = zipfile.ZipFile(file_name, 'r')
        print('Extracting %s' % file_name)
        zip_file_contents = zip_file.namelist()
        for f in zip_file_contents:
            if ('.csv' in f):
                zip_file.extract(f)
        zip_file.close()
        print('Extracted file  %s' % file_name)
    else :
        print('File %s exists. Skipping unzip.' % csv_file)

# @TODO delete zip file to free up space as we go
# @TODO insert into database, then delete csv file to free up space as we go
# @TODO create indexes after data is imported

def import_file(file_name):
    print('Routing file %s' % file_name)
    if 'Beneficiary_Summary_File' in file_name:
        beneficiary_summary_import(file_name)
    elif 'Carrier_Claims' in file_name:
        carrier_claims_import(file_name)
    elif 'Inpatient_Claims' in file_name:
        ip_claims_import(file_name)
    elif 'Outpatient_Claims' in file_name:
        op_claims_import(file_name)
    elif 'Prescription_Drug_Events' in file_name:
        prescription_drug_events_import(file_name)

def beneficiary_summary_import(file):
    print('Importing file %s into beneficiary_summary' % file)

def carrier_claims_import(file):
    print('Importing file %s into carrier_claims' % file)

def ip_claims_import(file):
    print('Importing file %s into ip_claims' % file)

def op_claims_import(file):
    print('Importing file %s into op_claims' % file)

def prescription_drug_events_import(file):
    print('Importing file %s into prescription_drug_events' % file)
    stmt = """ LOAD DATA LOCAL INFILE %s INTO TABLE prescription_drug_events 
               FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' 
               IGNORE 1 LINES (
               DESYNPUF_ID, 
               PDE_ID, 
               SRVC_DT, 
               PROD_SRVC_ID, 
               QTY_DSPNSD_NUM, 
               DAYS_SUPLY_NUM, 
               PTNT_PAY_AMT, 
               TOT_RX_CST_AMT)"""
    c.execute(stmt, (file,))
    conn.commit()

# TODO set up these files so that they can be downloaded as a set.  User will be able to choose which sets they would like to download
# give them the option to download all sets, or choose 1 set, or a range of sets (input as 1-5)

sample_01 = {
    'DE1_0_2008_Beneficiary_Summary_File_Sample_1': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_1.zip',
    'DE1_0_2009_Beneficiary_Summary_File_Sample_1': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_1.zip',
    'DE1_0_2010_Beneficiary_Summary_File_Sample_1': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_1.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_1A': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_1A.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_1B': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.zip',
    'DE1_0_2008_to_2010_Inpatient_Claims_Sample_1': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.zip',
    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_1': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.zip',
    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_1': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_1.zip'
}

# Download, unzip, and import data
for file,location in sample_01.items():
    zip_file = file + '.zip'
    csv_file = file + '.csv'
    download_file(zip_file, location)
    unzip_file(zip_file, csv_file)
    import_file(csv_file)


sample_02 = {
    'DE1_0_2008_Beneficiary_Summary_File_Sample_2': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_2.zip', 	
    'DE1_0_2009_Beneficiary_Summary_File_Sample_2': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_2.zip', 
    'DE1_0_2010_Beneficiary_Summary_File_Sample_2': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_2.zip', 
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_2A': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_2A.zip', 
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_2B': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_2B.zip', 
    'DE1_0_2008_to_2010_Inpatient_Claims_Sample_2': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_2.zip', 
    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_2': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_2.zip', 
    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_2': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_2.zip'
}

sample_03 = {
    'DE1_0_2008_Beneficiary_Summary_File_Sample_3': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_3.zip', 	
    'DE1_0_2009_Beneficiary_Summary_File_Sample_3': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_3.zip', 
    'DE1_0_2010_Beneficiary_Summary_File_Sample_3': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_3.zip', 
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_3A': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_3A.zip', 
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_3B': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_3B.zip', 
    'DE1_0_2008_to_2010_Inpatient_Claims_Sample_3': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_3.zip', 
    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_3': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_3.zip', 
    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_3': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_3.zip'
}

sample_04 = {
    'DE1_0_2008_Beneficiary_Summary_File_Sample_4': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_4.zip', 	
    'DE1_0_2009_Beneficiary_Summary_File_Sample_4': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_4.zip', 
    'DE1_0_2010_Beneficiary_Summary_File_Sample_4': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_4.zip', 
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_4A': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_4A.zip', 
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_4B': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_4B.zip', 
    'DE1_0_2008_to_2010_Inpatient_Claims_Sample_4': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_4.zip', 
    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_4': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_4.zip', 
    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_4': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_4.zip'
}

sample_05 = {
    'DE1_0_2008_Beneficiary_Summary_File_Sample_5': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_5.zip', 	
    'DE1_0_2009_Beneficiary_Summary_File_Sample_5': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_5.zip', 
    'DE1_0_2010_Beneficiary_Summary_File_Sample_5': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_5.zip', 
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_5A': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_5A.zip', 
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_5B': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_5B.zip', 
    'DE1_0_2008_to_2010_Inpatient_Claims_Sample_5': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_5.zip', 
    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_5': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_5.zip', 
    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_5': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_5.zip'
}

sample_06 = {
    'DE1_0_2008_Beneficiary_Summary_File_Sample_6': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_6.zip',
    'DE1_0_2009_Beneficiary_Summary_File_Sample_6': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_6.zip',
    'DE1_0_2010_Beneficiary_Summary_File_Sample_6': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_6.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_6A': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_6A.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_6B': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_6B.zip',
    'DE1_0_2008_to_2010_Inpatient_Claims_Sample_6': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_6.zip',
    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_6': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_6.zip',
    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_6': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_6.zip'
}

sample_07 = {
    'DE1_0_2008_Beneficiary_Summary_File_Sample_7': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_7.zip',
    'DE1_0_2009_Beneficiary_Summary_File_Sample_7': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_7.zip',
    'DE1_0_2010_Beneficiary_Summary_File_Sample_7': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_7.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_7A': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_7A.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_7B': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_7B.zip',
    'DE1_0_2008_to_2010_Inpatient_Claims_Sample_7': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_7.zip',
    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_7': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_7.zip',
    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_7': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_7.zip'
}

sample_08 = {
    'DE1_0_2008_Beneficiary_Summary_File_Sample_8': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_8.zip',
    'DE1_0_2009_Beneficiary_Summary_File_Sample_8': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_8.zip',
    'DE1_0_2010_Beneficiary_Summary_File_Sample_8': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_8.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_8A': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_8A.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_8B': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_8B.zip',
    'DE1_0_2008_to_2010_Inpatient_Claims_Sample_8': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_8.zip',
    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_8': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_8.zip',
    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_8': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_8.zip'
}

sample_09 = {
    'DE1_0_2008_Beneficiary_Summary_File_Sample_9': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_9.zip',
    'DE1_0_2009_Beneficiary_Summary_File_Sample_9': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_9.zip',
    'DE1_0_2010_Beneficiary_Summary_File_Sample_9': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_9.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_9A': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_9A.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_9B': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_9B.zip',
    'DE1_0_2008_to_2010_Inpatient_Claims_Sample_9': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_9.zip',
    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_9': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_9.zip',
    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_9': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_9.zip'
}

sample_10 = {
    'DE1_0_2008_Beneficiary_Summary_File_Sample_10': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_10.zip',
    'DE1_0_2009_Beneficiary_Summary_File_Sample_10': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_10.zip',
    'DE1_0_2010_Beneficiary_Summary_File_Sample_10': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_10.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_10A': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_10A.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_10B': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_10B.zip',
    'DE1_0_2008_to_2010_Inpatient_Claims_Sample_10': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_10.zip',
    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_10': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_10.zip',
    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_10': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_10.zip'
}

sample_11 = {
    'DE1_0_2008_Beneficiary_Summary_File_Sample_11': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_11.zip',
    'DE1_0_2009_Beneficiary_Summary_File_Sample_11': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_11.zip',
    'DE1_0_2010_Beneficiary_Summary_File_Sample_11': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_11.zip',
    '0_2008_to_2010_Carrier_Claims_Sample_11A.zip': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_11A.csv.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_11B': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_11B.zip',
    'DE1_0_2008_to_2010_Inpatient_Claims_Sample_11': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_11.zip',
    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_11': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_11.zip',
    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_11': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_11.zip'
}

sample_12 = {
    'DE1_0_2008_Beneficiary_Summary_File_Sample_12': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_12.zip',
    'DE1_0_2009_Beneficiary_Summary_File_Sample_12': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_12.zip',
    'DE1_0_2010_Beneficiary_Summary_File_Sample_12': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_12.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_12A': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_12A.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_12B': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_12B.zip',
    'DE1_0_2008_to_2010_Inpatient_Claims_Sample_12': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_12.zip',
    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_12': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_12.zip',
    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_12': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_12.zip'
}

sample_13 = {
    'DE1_0_2008_Beneficiary_Summary_File_Sample_13': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_13.zip',
    'DE1_0_2009_Beneficiary_Summary_File_Sample_13': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_13.zip',
    'DE1_0_2010_Beneficiary_Summary_File_Sample_13': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_13.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_13A': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_13A.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_13B': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_13B.zip',
    'DE1_0_2008_to_2010_Inpatient_Claims_Sample_13': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_13.zip',
    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_13': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_13.zip',
    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_13': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_13.zip'
}

sample_14 = {
    'DE1_0_2008_Beneficiary_Summary_File_Sample_14': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_14.zip',
    'DE1_0_2009_Beneficiary_Summary_File_Sample_14': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_14.zip',
    'DE1_0_2010_Beneficiary_Summary_File_Sample_14': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_14.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_14A': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_14A.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_14B': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_14B.zip',
    'DE1_0_2008_to_2010_Inpatient_Claims_Sample_14': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_14.zip',
    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_14': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_14.zip',
    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_14': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_14.zip'
}

sample_15 = {
    'DE1_0_2008_Beneficiary_Summary_File_Sample_15': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_15.zip',
    'DE1_0_2009_Beneficiary_Summary_File_Sample_15': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_15.zip',
    'DE1_0_2010_Beneficiary_Summary_File_Sample_15': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_15.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_15A': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_15A.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_15B': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_15B.zip',
    'DE1_0_2008_to_2010_Inpatient_Claims_Sample_15': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_15.zip',
    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_15': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_15.zip',
    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_15': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_15.zip'
}

sample_16 = {
    'DE1_0_2008_Beneficiary_Summary_File_Sample_16': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_16.zip',
    'DE1_0_2009_Beneficiary_Summary_File_Sample_16': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_16.zip',
    'DE1_0_2010_Beneficiary_Summary_File_Sample_16': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_16.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_16A': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_16A.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_16B': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_16B.zip',
    'DE1_0_2008_to_2010_Inpatient_Claims_Sample_16': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_16.zip',
    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_16': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_16.zip',
    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_16': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_16.zip'
}

sample_17 = {
    'DE1_0_2008_Beneficiary_Summary_File_Sample_17': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_17.zip',
    'DE1_0_2009_Beneficiary_Summary_File_Sample_17': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_17.zip',
    'DE1_0_2010_Beneficiary_Summary_File_Sample_17': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_17.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_17A': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_17A.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_17B': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_17B.zip',
    'DE1_0_2008_to_2010_Inpatient_Claims_Sample_17': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_17.zip',
    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_17': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_17.zip',
    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_17': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_17.zip'
}

sample_18 = {
    'DE1_0_2008_Beneficiary_Summary_File_Sample_18': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_18.zip',
    'DE1_0_2009_Beneficiary_Summary_File_Sample_18': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_18.zip',
    'DE1_0_2010_Beneficiary_Summary_File_Sample_18': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_18.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_18A': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_18A.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_18B': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_18B.zip',
    'DE1_0_2008_to_2010_Inpatient_Claims_Sample_18': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_18.zip',
    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_18': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_18.zip',
    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_18': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_18.zip'
}

sample_19 = {
    'DE1_0_2008_Beneficiary_Summary_File_Sample_19': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_19.zip',
    'DE1_0_2009_Beneficiary_Summary_File_Sample_19': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_19.zip',
    'DE1_0_2010_Beneficiary_Summary_File_Sample_19': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_19.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_19A': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_19A.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_19B': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_19B.zip',
    'DE1_0_2008_to_2010_Inpatient_Claims_Sample_19': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_19.zip',
    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_19': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_19.zip',
    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_19': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_19.zip'
}

sample_20 = {
    'DE1_0_2008_Beneficiary_Summary_File_Sample_20': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_Beneficiary_Summary_File_Sample_20.zip',
    'DE1_0_2009_Beneficiary_Summary_File_Sample_20': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2009_Beneficiary_Summary_File_Sample_20.zip',
    'DE1_0_2010_Beneficiary_Summary_File_Sample_20': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2010_Beneficiary_Summary_File_Sample_20.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_20A': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_20A.zip',
    'DE1_0_2008_to_2010_Carrier_Claims_Sample_20B': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Carrier_Claims_Sample_20B.zip',
    'DE1_0_2008_to_2010_Inpatient_Claims_Sample_20': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Inpatient_Claims_Sample_20.zip',
    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_20': 'https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads/DE1_0_2008_to_2010_Outpatient_Claims_Sample_20.zip',
    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_20': 'http://downloads.cms.gov/files/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_20.zip'
}

c.close()
conn.close()
print('Import Completed')