----------------------------------------------------------------------
-- Create tables for Tier0 Data Service Database.

CREATE TABLE express_config (
  run int not null,
  stream varchar2(255) not null,
  cmssw varchar2(255) not null,
  scram_arch varchar2(50) not null,
  global_tag varchar2(50) not null,
  scenario varchar2(50) not null,
  multicore int not null default 4,
  write_tiers varchar2(255),
  write_dqm int default 0 not null,
  reco_cmssw varchar2(255),
  reco_scram_arch varchar2(50),
  alca_skim varchar2(700),
  dqm_seq varchar2(700),
  primary key (run, stream)
) ORGANIZATION INDEX;

CREATE TABLE reco_config (
  run int not null,
  primds varchar2(255) not null,
  cmssw varchar2(255) not null,
  scram_arch varchar2(50) not null,
  global_tag varchar2(50) not null,
  scenario varchar2(50) not null,
  multicore int default 4 not null,
  write_reco int default 0 not null,
  write_dqm int default 0 not null,
  write_aod int default 0 not null,
  write_miniaod int default 0 not null,
  write_nanoaod int default 0 not null,
  alca_skim varchar2(700),
  physics_skim varchar2(700),
  dqm_seq varchar2(700),
  primary key (run, primds)
) ORGANIZATION INDEX;

CREATE TABLE reco_locked (
  run int not null,
  locked int not null,
  primary key (run)
) ORGANIZATION INDEX;

CREATE TABLE run_stream_done (
  run int not null,
  stream varchar2(255) not null,
  primary key (run, stream)
) ORGANIZATION INDEX;

CREATE TABLE run_primds_done (
  run int not null,
  primds varchar2(255) not null,
  finished int default 0 not null,
  primary key (run, primds)
) ORGANIZATION INDEX;

CREATE TABLE dataset_locked (
  path varchar2(1000) not null,
  primary key (path)
) ORGANIZATION INDEX;

CREATE TABLE run_config (
  run int not null,
  acq_era varchar2(255) not null,
  primary key (run)
) ORGANIZATION INDEX;

CREATE TABLE promptreco_status (
 status INT NOT NULL,
 change_time TIMESTAMP(6) NOT NULL
);
INSERT INTO promptreco_status (status, change_time) VALUES (1, CURRENT_TIMESTAMP);

CREATE TABLE skipped_streamers (
  run int not null,
  stream varchar2(255) not null,
  lumi int not null,
  events int not null,
  primary key (run, stream, lumi)
) ORGANIZATION INDEX;

CREATE TABLE FILE_TRANSFER_STATUS_OFFLINE (
  P5_FILEID               NUMBER(27)     NOT NULL,
  FILENAME                VARCHAR2(1000) NOT NULL,
  T0_CHECKED_TIME         TIMESTAMP(6)   NOT NULL,
  T0_REPACKED_TIME        TIMESTAMP(6),
  CHECKED_RETRIEVE        NUMBER(1),
  REPACKED_RETRIEVE       NUMBER(1),
  PRIMARY KEY (P5_FILEID)
);

CREATE INDEX idx_ftso_1 ON FILE_TRANSFER_STATUS_OFFLINE (CHECKED_RETRIEVE);
CREATE INDEX idx_ftso_2 ON FILE_TRANSFER_STATUS_OFFLINE (REPACKED_RETRIEVE);

GRANT SELECT, INSERT ON promptreco_status TO CMS_TIER0DATASVC_ORM;
