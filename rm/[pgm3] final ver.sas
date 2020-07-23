/*================================================================================
/ Organization	: []Health, Inc. 
/ Program		: 
/ Version		: 
/ Date			: 14 Jan 2019
/ Contact:		: 
/ Purpose		: collect and export in excel
/			  	  
/ SubMacros		: 
/ Notes			: 
/ Usage			: 
/				  
/================================================================================
/  PARAMETERS:
/-------name------- -------------------------description-------------------------
/ client            SAS Short name for an organization. 
/ rstart           	Claims starting date.
/ rend           	Claims through date.
/ source			Select member base to use.  (Default=source3) 
/					- source1 =carrier's member file if available, else eligibility
					- source2 =claims plus active members
                    - source3 =use eligibility 
					- source4 =use carrier's member file
/ out             	Libname of input dataset.
/ OutDir			Output path of the output file. 
/================================================================================
/  AMENDMENT HISTORY:
/-------date------- -------------------------description-------------------------

libname SASO "O:\SAS\Data\[]";
libname bgdd   "O:\SAS\Data\[] standard data";

libname ccs_list odbc dsn="ccs_member_list";
libname []phc odbc dsn="[]_PHC";
libname memphis odbc dsn="MemPHIS" schema=crm;
libname memphis2 odbc dsn="MemPHIS" schema=dbo;
libname online odbc dsn="MemPHIS" schema=OnlineProgram;
libname tracker odbc dsn="MemPHIS" schema=Tracker;
libname well odbc dsn="MemPHIS" schema=Wellness;
libname workshop odbc dsn="MemPHIS" schema=Workshop;

%let version =v6;

libname ref 'O:\SAS\PGM\Standard Data\[]';
libname hedisfm v9  "O:\SAS\Formats\HEDIS\2017";

libname hq2 odbc   dsn="ENGAGEDB_HQ2";
libname feeds odbc dsn="ENGAGEDB_DATAFEEDS";
libname mmdb  odbc dsn="MMDB";
libname []fmt v9  "O:\SAS\formats\[]\&version.";

libname ce_in (work);
libname ce_out (work);
options mprint symbolgen fmtsearch=(work hedisfm ref []fmt) ;

%include "C:\Users\ssong\Downloads\mass run\lib\member0.sas";
%include "C:\Users\ssong\Downloads\mass run\lib\CE_rpt.sas";
%include "C:\Users\ssong\Downloads\mass run\lib\CE_eng_t_no_trunc.sas";
%include "C:\Users\ssong\Downloads\mass run\lib\CE_eng_no_trunc.sas";
%include "O:\SAS\Macros\[]\v6\roi\memid_ccs.sas";
%include "O:\SAS\Macros\[]\v6\roi\ma.sas";
%include "O:\SAS\Macros\[]\v6\roi\medication_adherence.sas";
%include "O:\SAS\Macros\[]\v6\roi\Preventive_care.sas";
%include "O:\SAS\Macros\[]\v6\roi\ID_cad_[]_v6.sas";
%include "O:\SAS\Macros\[]\v6\roi\ID_hchol_[]_v6.sas";
%include "O:\SAS\Macros\[]\v6\roi\ID_dep_[]_v6.sas";
%include "O:\SAS\Macros\[]\v6\roi\ID_hf_[]_v6.sas";
%include "O:\SAS\Macros\[]\v6\roi\ID_diab_[]_v6.sas";
%include "O:\SAS\Macros\[]\v6\roi\ID_copd_[]_v6.sas";
%include "O:\SAS\Macros\[]\v6\roi\ID_ast_[]_v6.sas";
%include "O:\SAS\Macros\[]\v6\roi\ID_htn_[]_v6.sas";
*%include "O:\SAS\Macros\[]\v6\roi\ID_mets_[]_v6.sas";

%include "O:\SAS\Macros\[]\v6\roi\rx_adhcad_[]_v6.sas";
%include "O:\SAS\Macros\[]\v6\roi\rx_adhhchol_[]_v6.sas";
%include "O:\SAS\Macros\[]\v6\roi\rx_adhast_[]_v6.sas";
%include "O:\SAS\Macros\[]\v6\roi\rx_adhdep_[]_v6.sas";
%include "O:\SAS\Macros\[]\v6\roi\rx_adhhf_[]_v6.sas";
%include "O:\SAS\Macros\[]\v6\roi\rx_adhdiab_[]_v6.sas";
%include "O:\SAS\Macros\[]\v6\roi\rx_adhcopd_[]_v6.sas";
%include "O:\SAS\Macros\[]\v6\roi\rx_adhhtn_[]_v6.sas";
/*%include "O:\SAS\Macros\[]\v6\roi\rx_adhmets1_[]_v6.sas";
%include "O:\SAS\Macros\[]\v6\roi\rx_adhmets2_[]_v6.sas";
%include "O:\SAS\Macros\[]\v6\roi\rx_adhmets3_[]_v6.sas";*/
%include "O:\SAS\Macros\[]\v6\roi\Prev_chol_[]_v6.sas";
%include "O:\SAS\Macros\[]\v6\roi\Prev_colon_[]_v6.sas";
%include "O:\SAS\Macros\[]\v6\roi\Prev_pap_[]_v6.sas";
%include "O:\SAS\Macros\[]\v6\roi\Prev_mammo_[]_v6.sas";
%include "O:\SAS\Macros\[]\v6\roi\Sup_adh_[]_v6.sas";

options dlcreatedir;


%macro pgm3(sysparm,client,orgid1,path) /secure store ;

proc datasets lib=work kill nolist;run;quit;
dm 'clear log';

%member0;

%let m1s=%substr(%scan(&sysparm,1,%str(' ')),5,2);
%let d1s=%substr(%scan(&sysparm,1,%str(' ')),7,2);
%let y1s=%substr(%scan(&sysparm,1,%str(' ')),1,4);
%let m1e=%substr(%scan(&sysparm,2,%str(' ')),5,2);
%let d1e=%substr(%scan(&sysparm,2,%str(' ')),7,2);
%let y1e=%substr(%scan(&sysparm,2,%str(' ')),1,4);
%let m2s=%substr(%scan(&sysparm,3,%str(' ')),5,2);
%let d2s=%substr(%scan(&sysparm,3,%str(' ')),7,2);
%let y2s=%substr(%scan(&sysparm,3,%str(' ')),1,4);
%let m2e=%substr(%scan(&sysparm,4,%str(' ')),5,2);
%let d2e=%substr(%scan(&sysparm,4,%str(' ')),7,2);
%let y2e=%substr(%scan(&sysparm,4,%str(' ')),1,4);


%let rstart=%sysfunc(mdy(&m1s.,&d1s.,&y1s.));
%let rend=%sysfunc(mdy(&m1e.,&d1e.,&y1e.));
  /*
%let rstart2=%sysfunc(mdy(&m2s.,&d2s.,&y2s.));
%let rend2=%sysfunc(mdy(&m2e.,&d2e.,&y2e.));
  */
 

%ce_eng_t_no_trunc;

proc sql; create table ce_out.&client._e_ce_yr1
as select distinct memid from &client._engaged3;
quit;

proc sql; create table ce_out.&client._n_ce_yr1  
as select distinct memid from &client._nonengaged;
quit;

%ce_eng_no_trunc;
proc sql; create table ce_out.&client._eo_ce_yr1
as select distinct memid from &client._engaged3t;
quit;

proc sql; create table ce_out.&client._no_ce_yr1 
as select distinct memid from &client._nonengaged;
quit;

%let rstart=%sysfunc(mdy(&m2s.,&d2s.,&y2s.));
%let rend=%sysfunc(mdy(&m2e.,&d2e.,&y2e.));

%ce_eng_t_no_trunc;
 
proc sql; create table ce_out.&client._e_ce_yr2
as select distinct memid from &client._engaged3;
quit;

proc sql; create table ce_out.&client._n_ce_yr2  
as select distinct memid from &client._nonengaged;
quit;

%ce_eng_no_trunc;
proc sql; create table ce_out.&client._eo_ce_yr2
as select distinct memid from &client._engaged3t;
quit;

proc sql; create table ce_out.&client._no_ce_yr2
as select distinct memid from &client._nonengaged;
quit;


%macro metric_prep(values=, rstart=,rend=,);
%let count=%sysfunc(countw(&values)); 
%let indic=%qscan(&values,2,%str('_'));
%let rstart=&rstart.;
%let rend=&rend.;
 
%do i = 1 %to &count;                                                                                                                
  %let n&i.= %scan(&values,&i.,%str(' '));
%end;
data   %&n1;
set &client._medical
(keep=memid OrganizationID Organizationname carrier claimid claimline startdate enddate dx1-dx10 cpt modifier proc1-proc6 rev pos billtype paid copay deductible coinsurance specialty);
array dx $ dx1-dx10;
array proc $ proc1-proc6;
 
  %&n2; 
run;

data %&n3;

set &client._pharmacy
(keep=memid OrganizationID Organizationname carrier claimid startdate ndc supply unit_flg);

%&n4;

run;

data 
%&n5;
set &client._Biometrics;
%&n6;
run;

data 
%&n7;
set &client._HRA;
%&n8;
run;
%mend;


%macro repeat(values=,rstart=,rend=);
%let count=%sysfunc(countw(&values));  
%let n=1; 
%do n = 1 %to &count ;                                                                                                                
  %let ind= %scan(&values,&n,%str(' '));
%metric_prep(values=prev_&ind._[]_v6_med_1 prev_&ind._[]_v6_med_2 prev_&ind._[]_v6_rx_1 prev_&ind._[]_v6_rx_2 prev_&ind._[]_v6_bio_1 prev_&ind._[]_v6_bio_2 prev_&ind._[]_v6_hra_1 prev_&ind._[]_v6_hra_2, rstart=&rstart.,rend=&rend. );
%prev_&ind._[]_v6;
%put &n;
%end;
%mend;

%macro repeat2(values=,rstart=,rend=);
%let count=%sysfunc(countw(&values));   
%let n=1; 
%do n = 1 %to &count ;                                                                                                                
  %let ind= %scan(&values,&n,%str(' '));
%metric_prep(values=id_&ind._[]_v6_med_1 id_&ind._[]_v6_med_2 id_&ind._[]_v6_rx_1 id_&ind._[]_v6_rx_2 id_&ind._[]_v6_bio_1 id_&ind._[]_v6_bio_2 id_&ind._[]_v6_hra_1 id_&ind._[]_v6_hra_2, rstart=&rstart.,rend=&rend. )
%id_&ind._[]_v6;
%metric_prep(values=rx_adh&ind._[]_v6_med_1 rx_adh&ind._[]_v6_med_2 rx_adh&ind._[]_v6_rx_1 rx_adh&ind._[]_v6_rx_2, rstart=&rstart.,rend=&rend.);
%if &ind.^='mets' %then  %do;
%rx_adh&ind._[]_v6;
%end;
%else %do;
%rx_adh&ind.1_[]_v6;
%rx_adh&ind.2_[]_v6;
%rx_adh&ind.3_[]_v6;
%end;
%end;
%mend;

%macro repeat3(cohort=, rpt=, rstart=, rend=);
%let cohort=&cohort.;
%let rpt=&rpt.;
%let rstart=&rstart.;
%let rend=&rend.;

%memid_ccs;

%repeat(values=chol colon mammo pap, rstart=&rstart.,rend=&rend.);
%Preventive_care;

%repeat2(values=ast cad copd dep diab hchol hf htn, rstart=&rstart.,rend=&rend.);
%Medication_Adherence;

data &client._mh_&cohort._&rpt.;
set &client._mh_list;
run;

data &client._pc_&cohort._&rpt.;
set &client._pc;
run;
%mend;

%if %sysfunc(exist(ce_out.&client._e_ce_yr1)) %then %do;
%repeat3(cohort=e, rpt=yr1, rstart=%sysfunc(mdy(&m1s.,&d1s.,&y1s.)),rend= %sysfunc(mdy(&m1e.,&d1e.,&y1e.)));
%end;
dm 'clear log';
%if %sysfunc(exist(ce_out.&client._n_ce_yr1)) %then %do;
%repeat3(cohort=n, rpt=yr1, rstart=%sysfunc(mdy(&m1s.,&d1s.,&y1s.)),rend= %sysfunc(mdy(&m1e.,&d1e.,&y1e.)));
%end;
dm 'clear log';
%if %sysfunc(exist(ce_out.&client._e_ce_yr2)) %then %do;
%repeat3(cohort=e, rpt=yr2, rstart=%sysfunc(mdy(&m2s.,&d2s.,&y2s.)),rend= %sysfunc(mdy(&m2e.,&d2e.,&y2e.)) );
%end;
dm 'clear log';
%if %sysfunc(exist(ce_out.&client._n_ce_yr2)) %then %do;
%repeat3(cohort=n, rpt=yr2, rstart=%sysfunc(mdy(&m2s.,&d2s.,&y2s.)),rend= %sysfunc(mdy(&m2e.,&d2e.,&y2e.)));
%end;
dm 'clear log';
%if %sysfunc(exist(ce_out.&client._eo_ce_yr1)) %then %do;
%repeat3(cohort=eo, rpt=yr1, rstart=%sysfunc(mdy(&m1s.,&d1s.,&y1s.)),rend= %sysfunc(mdy(&m1e.,&d1e.,&y1e.)));
%end;
dm 'clear log';
%if %sysfunc(exist(ce_out.&client._no_ce_yr1)) %then %do;
%repeat3(cohort=no, rpt=yr1, rstart=%sysfunc(mdy(&m1s.,&d1s.,&y1s.)),rend= %sysfunc(mdy(&m1e.,&d1e.,&y1e.)));
%end;
dm 'clear log';
%if %sysfunc(exist(ce_out.&client._eo_ce_yr2)) %then %do;
%repeat3(cohort=eo, rpt=yr2, rstart=%sysfunc(mdy(&m2s.,&d2s.,&y2s.)),rend= %sysfunc(mdy(&m2e.,&d2e.,&y2e.)) );
%end;
dm 'clear log';
%if %sysfunc(exist(ce_out.&client._no_ce_yr2)) %then %do;
%repeat3(cohort=no, rpt=yr2, rstart=%sysfunc(mdy(&m2s.,&d2s.,&y2s.)),rend= %sysfunc(mdy(&m2e.,&d2e.,&y2e.)));
%end;
dm 'clear log';

data &client._pc_e_yr1;
length group1 $20. group2 $20.;
set &client._pc_e_yr1;
group1='total';
group2='engaged';
year='yr1';
run;
data &client._pc_e_yr2;
length group1 $20. group2 $20.;
set &client._pc_e_yr2;
group1='total';
group2='engaged';
year='yr2';
run;
data &client._pc_n_yr1;
length group1 $20. group2 $20.;
set &client._pc_n_yr1;
group1='total';
group2='nonengaged';
year='yr1';
run;
data &client._pc_n_yr2;
length group1 $20. group2 $20.;
set &client._pc_n_yr2;
group1='total';
group2='nonengaged';
year='yr2';
run;

data &client._mh_e_yr1;
length group1 $20. group2 $20.;
set &client._mh_e_yr1;
group1='total';
group2='engaged';
year='yr1';
run;
data &client._mh_e_yr2;
length group1 $20. group2 $20.;
set &client._mh_e_yr2;
group1='total';
group2='engaged';
year='yr2';
run;
data &client._mh_n_yr1;
length group1 $20. group2 $20.;
set &client._mh_n_yr1;
group1='total';
group2='nonengaged';
year='yr1';
run;
data &client._mh_n_yr2;
length group1 $20. group2 $20.;
set &client._mh_n_yr2;
group1='total';
group2='nonengaged';
year='yr2';
run;

data &client._pc_eo_yr1;
length group1 $20. group2 $20.;
set &client._pc_eo_yr1;
group1='outreach';
group2='engaged';
year='yr1';
run;
data &client._pc_eo_yr2;
length group1 $20. group2 $20.;
set &client._pc_eo_yr2;
group1='outreach';
group2='engaged';
year='yr2';
run;
data &client._pc_no_yr1;
length group1 $20. group2 $20.;
set &client._pc_no_yr1;
group1='outreach';
group2='nonengaged';
year='yr1';
run;
data &client._pc_no_yr2;
length group1 $20. group2 $20.;
set &client._pc_no_yr2;
group1='outreach';
group2='nonengaged';
year='yr2';
run;

data &client._mh_eo_yr1;
length group1 $20. group2 $20.;
set &client._mh_eo_yr1;
group1='outreach';
group2='engaged';
year='yr1';
run;
data &client._mh_eo_yr2;
length group1 $20. group2 $20.;
set &client._mh_eo_yr2;
group1='outreach';
group2='engaged';
year='yr2';
run;
data &client._mh_no_yr1;
length group1 $20. group2 $20.;
set &client._mh_no_yr1;
group1='outreach';
group2='nonengaged';
year='yr1';
run;
data &client._mh_no_yr2;
length group1 $20. group2 $20.;
set &client._mh_no_yr2;
group1='outreach';
group2='nonengaged';
year='yr2';
run;

proc append base=pc data=&client._pc_e_yr1; run;
proc append base=pc data=&client._pc_e_yr2; run;
proc append base=pc data=&client._pc_n_yr1; run;
proc append base=pc data=&client._pc_n_yr2; run;
proc append base=pc data=&client._pc_eo_yr1; run;
proc append base=pc data=&client._pc_eo_yr2; run;
proc append base=pc data=&client._pc_no_yr1; run;
proc append base=pc data=&client._pc_no_yr2; run;

proc append base=mh data=&client._mh_e_yr1; run;
proc append base=mh data=&client._mh_e_yr2; run;
proc append base=mh data=&client._mh_n_yr1; run;
proc append base=mh data=&client._mh_n_yr2; run;
proc append base=mh data=&client._mh_eo_yr1; run;
proc append base=mh data=&client._mh_eo_yr2; run;
proc append base=mh data=&client._mh_no_yr1; run;
proc append base=mh data=&client._mh_no_yr2; run;


%let dt = %sysfunc(translate(%sysfunc(datetime(),datetime.),--,.:)); ;   

libname path "&path.\&client.";
%let path= %sysfunc(pathname(path));

%let name=&path.\&client._mh_&y1s..&m1s.-&y2e..&m2e._&dt..csv;
%let name2=&path.\&client._pc_&y1s..&m1s.-&y2e..&m2e._&dt..csv;

proc export data=mh outfile="&name."  dbms=csv replace;
run;

proc export data=pc outfile="&name2."  dbms=csv replace;
run;

%mend;

%pgm3(sysparm='20160801 20170731 20170801 20180731', client= cae, orgid1=1071096,path= C:\Users\ssong\Downloads\mass run);
 

?
