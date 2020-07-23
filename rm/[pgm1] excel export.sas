/*================================================================================
/ Organization	: []Health, Inc. 
/ Program		: 
/ Version		: 
/ Date			: 14 Jan 2019
/ Contact:		: 
/ Purpose		: output ROI results 
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


/================================================================================
/  This is proprietary and confidential information. Not to be disclosed. 
/================================================================================
/
/
/================================================================================
/  Apply settings #1 from macro
/===============================================================================*/

libname ccs_list odbc dsn="ccs_member_list";
libname []phc odbc dsn="[]_PHC";
libname feeds odbc dsn="ENGAGEDB_DATAFEEDS";
libname memphis odbc dsn="MemPHIS" schema=crm;
libname memphis2 odbc dsn="MemPHIS" schema=dbo;
libname mara odbc dsn="MARA_OUTPUT_STAGING" schema=dbo;

libname online odbc dsn="MemPHIS" schema=OnlineProgram;

libname tracker odbc dsn="MemPHIS" schema=Tracker;
libname well odbc dsn="MemPHIS" schema=Wellness;
libname workshop odbc dsn="MemPHIS" schema=Workshop;

libname SASO "O:\SAS\Data\[]";
libname bgdd 	  "O:\SAS\Data\[]standard data";

libname ref 'O:\SAS\PGM\Standard Data\[]';
libname []v6 v9  "O:\SAS\formats\[]\v6";
libname hedisfm v9  "O:\SAS\Formats\HEDIS\2017";

options mprint symbolgen fmtsearch=(work hedisfm ref []v6) ;

libname hce odbc dsn="HCEPlus_Analysis" schema=dbo;

options dlcreatedir;
/*
%macro lib;
%if (%sysfunc(libref(MACROS))) %then %do;
libname macros "O:\SAS\Macros\[]\v6\sasmstore";
%end;
%mend lib;

%lib;
options mstored sasmstore=macros;
*/
options no mstored;

/*%let client=xtrspace;
%let orgid=1127631;
%let orgid1=1127631 1043255;
*/
/*options dlcreatedir;*/



/*************************************************************************************************/


%macro export(client= , orgid=, orgid1=, y1s=, y1e=, m1s=, m1e=, d1s=, d1e=, y2s=, y2e=, m2s=, m2e=,
d2s=,d2e=,  jobid1=, jobid2=, jobid1a=,jobid2a=, truncate=,path= );

%include "C:\Users\ssong\Downloads\mass run\lib\member0.sas";
%include "C:\Users\ssong\Downloads\mass run\lib\cost0.sas";

libname ce_in "c:\users\ssong\downloads\mass run\data";
libname ce_out "C:\Users\ssong\Downloads\mass run\data";

proc datasets lib=work memtype=data kill nolist;run;quit;
dm 'clear log';


%member0;
%cost0;

%include "C:\Users\ssong\Downloads\mass run\lib\run1.sas";
%include "C:\Users\ssong\Downloads\mass run\lib\run1tier.sas";

%include "C:\Users\ssong\Downloads\mass run\lib\truncate.sas";

%include "C:\Users\ssong\Downloads\mass run\lib\CE_rpt.sas";
%include "C:\Users\ssong\Downloads\mass run\lib\ce_eng.sas";
%include "C:\Users\ssong\Downloads\mass run\lib\ce_eng_t.sas";

%include "C:\Users\ssong\Downloads\mass run\lib\ce_eng_t_no_trunc.sas";
%include "C:\Users\ssong\Downloads\mass run\lib\ce_eng_no_trunc.sas";

%include "C:\Users\ssong\Downloads\mass run\lib\ce_rs.sas";

/*04.05 extra*/
%include "C:\Users\ssong\Downloads\mass run\lib\ce_eng_no_trunc - age.sas";
%include "C:\Users\ssong\Downloads\mass run\lib\run1 - age.sas";
%include "C:\Users\ssong\Downloads\mass run\lib\ce_eng - age.sas";

%let rstart1=%sysfunc(mdy(&m1s.,&d1s.,&y1s.));
%let rend1=%sysfunc(mdy(&m1e.,&d1e.,&y1e.));
%let factor1=0.728098;
%let jobid1=&jobid1.;
%let yearmonth1=&y1e.&m1e.;
%let year1=&y1e.;

%let rstart2=%sysfunc(mdy(&m2s.,&d2s.,&y2s.));
%let rend2=%sysfunc(mdy(&m2e.,&d2e.,&y2e.));
%let factor2=0.844745;
%let jobid2=&jobid2.;
%let yearmonth2=&y2e.&m2e.;
%let year2=&y2e.;

/*RUN 1*/
%let runtype=run1;
%let rstart =&rstart1.;
%let rend=&rend1.;
%let year=&year1.;
%let factor=&factor1.;

%let yearmonth=&yearmonth1.;
%let jobid = &jobid1.;
%let jobida = &jobid1a.;
    

data &client._member;
length agecat $55.;
set &client._member;
dob=input(compress(memberdob,"/"),anydtdte.);
format dob mmddyy10.;
age = floor((intck('month',dob,&rend) - (day(&rend) < day(dob))) / 12);
if age <18 then agecat='<18';
else if 18<=age<36 then agecat='18-35';
else if 36<=age<51 then agecat='36-50';
else if 51<=age<=65 then agecat='51-65';
else if age>65 then agecat='>65';
run;

%if &truncate.=0 %then  
%ce_eng_no_trunc;
 %else %do;
%truncate(&truncate.);
%ce_eng;
%end;
%ce_rs;
%run1;

%run1tier;

/*extra 04.05*/
%ce_eng_no_trunc_extra;
%run1extra;

%let rstart =&rstart2.;
%let rend=&rend2.;
%let year=&year2.;
%let factor=&factor2.;

%let yearmonth=&yearmonth2.;
%let jobid =&jobid2. ;
%let jobida =&jobid2a. ;

data &client._member;
length agecat $55.;
set &client._member;
dob=input(compress(memberdob,"/"),anydtdte.);
format dob mmddyy10.;
age = floor((intck('month',dob,&rend) - (day(&rend) < day(dob))) / 12);
if age <18 then agecat='<18';
else if 18<=age<36 then agecat='18-35';
else if 36<=age<51 then agecat='36-50';
else if 51<=age<=65 then agecat='51-65';
else if age>65 then agecat='>65';
run;
 
%if &truncate.=0 %then  
%ce_eng_no_trunc;
 %else %do;
%truncate(&truncate.);
%ce_eng;
%end;

%ce_rs;

%run1;
%run1tier;

/*extra 04.05*/
%if &truncate.=0 %then  
%ce_eng_no_trunc_extra;
 %else %do;
%truncate(&truncate.);
%ce_eng_extra;
%end;

%run1extra;

data &client._all_final_overall;
length population $55. run $10. group $55. year 8.  contact_Type $55. cost_location $55. 
cost_attributed $55. total_cost 8  visits 8;
set &client._all_final_&year1. &client._all_final_&year2.;
run;



data &client._all_final_hier;
length population $55. run $10. group $55. year 8.  contact_Type $55. cost_location $55. 
cost_attributed $55. total_cost 8  visits 8;

/*set &client._all_final_&year1._hier &client._all_final_&year2._hier;*/
set &client._all_final_&year1. &client._all_final_&year2.;

if contact_type='Biometrics' then hierarchy=5;
else if contact_type='HA Case Calls' then hierarchy=4;
else if contact_type='MEP Login' then hierarchy=7; 
else if contact_type='HCE' then hierarchy=8;
else if contact_type='Wellness Challenges' then hierarchy=2;
else if contact_type='Wellness Workshops' then hierarchy=1;
else if contact_type='Wellness Tracker' then hierarchy=3;
else if contact_type='Wellness Commitments' then hierarchy=6;

if group='Engaged' and hierarchy ne .;

run;

proc sort data=&client._all_final_hier; by run year population hierarchy ; run;

/*ti
%let rstart =&rstart1.;
%let rend=&rend1.;
%let year=&year1.;
%let rstart =&rstart2.;
%let rend=&rend2.;
%let year=&year2.;
*/

data &client._all_final_tier;
length population $55. run $10. group $55. year 8.  contact_Type $55. cost_location $55. 
cost_attributed $55. total_cost 8  visits 8;
set &client._all_final_&year1._tier (rename=(engage_tier=tier)) &client._all_final_&year2._tier (rename=(engage_tier=tier));

if tier =1 then engage_tier='High';
else if tier=2 then engage_tier='Low';
drop tier;

run;

data &client._all_final_tier;
length population $55. run $10. group $55. year 8.  contact_Type $55.  
cost_location $55. cost_attributed $55. total_cost 8  visits 8;
set &client._all_final_tier;
run;

/*************04.05 extra*/
data &client._all_final_overall_extra;
length population $55. run $10. group $55. year 8.  contact_Type $55. age_category $55. cost_location $55. 
cost_attributed $55. total_cost 8  visits 8;
set &client._all_final_extra_&year1. &client._all_final_extra_&year2.;
run;


/*final export to excel*/

data &client._all_combine;
length  population $55. run $10. group $55. year 8. contact_type $70.  engage_tier $8. cost_location $55.
cost_attributed $55. total_cost 8  members employees visits 8 age_category $55.;

set &client._all_final_overall
&client._all_final_hier
&client._all_final_tier
&client._all_final_overall_extra;
 
if group ='Outreach' then rank=1;
else if group ='Non-Outreach' then rank=2;
else if group ='Engaged' then rank=3;
else if group = 'Non-Engaged' then rank=4;
else if group ='Total' then rank=5;

if cost_location='Inpatie' then cost_location='Inpatient';
else if cost_location='Outpati' then cost_location='Outpatient';
else if cost_location='Profess' then cost_location='Professional';
else if cost_location ='Pharmac' then cost_location='Pharmacy';

if cost_attributed='Member' then do;
pmpy_cost=total_cost/members;
pmpy_cost_clm=total_cost/members_clm;
end;

else if cost_attributed='Employee' then do;
pmpy_cost=total_cost/employees;
end;

run;

data &client._all_risk;
set &client._risk_&year1. (in=a)
&client._risk_&year2. (in=b);
if a then year=&year1.;
if b then year=&year2.;
run;
data &client._all_risk2;
set &client._risk2_&year1. (in=a)
&client._risk2_&year2. (in=b);
if a then year=&year1.;
if b then year=&year2.;
run;

proc sql; create table &client._all_combine1 as select distinct a.*,b.average as avg_risk
from  &client._all_combine as a left join &client._all_risk as b
on a.year=b.year
and a.group=b.group;
quit;

proc sql; create table &client._all_combine1 as select distinct a.*,b.average as avg_risk2
from  &client._all_combine1 as a left join &client._all_risk2 as b
on a.year=b.year
and a.group=b.group
and a.age_category=b.age;
quit;

data &client._all_combine1;
set &client._all_combine1;

if pmpy_cost ne . then do;
pmpy_cost_risk = pmpy_cost/avg_risk;
end;

if pmpy_cost_clm ne . then do;
pmpy_cost_clm_risk=pmpy_cost_clm/avg_risk;
end;
run;

proc sort data=&client._all_combine1; by rank year descending cost_attributed hierarchy ; run;

data &client._all_combine1;
set &client._all_combine1;
drop rank;
run;


/*****************************
data &client._all_risk2;
set &client._risk2_&year1. (in=a)
&client._risk2_&year2. (in=b);
if a then year=&year1.;
if b then year=&year2.;
run;
proc sql; create table &client._all_combine1_2 as select distinct a.*,b.average as avg_risk
from  &client._all_combine as a left join &client._all_risk2 as b
on a.year=b.year
and a.group=b.group;
quit;
proc sort data=&client._all_combine1_2; by rank year descending cost_attributed hierarchy ; run;
data &client._all_combine1_2;
rename avg_risk=avg_risk2;
set &client._all_combine1_2;
drop rank;
run;
*/



 
/*run 2*/

/*%let client=genesis;*/
%let runtype=run2;
/*%let orgid=1127485;*/
/*%let orgid1=1127485 1135160;*/

%include "C:\Users\ssong\Downloads\mass run\lib\run2.sas";
*%include "C:\Users\ssong\Downloads\mass run\lib\run2hier.sas";
%include "C:\Users\ssong\Downloads\mass run\lib\run2tier.sas";

/*RUN 2*/
%let rstart =&rstart1.;
%let rend=&rend1.;
%let year=&year1.;
%let factor=&factor1.;

%let yearmonth=&yearmonth1.;
%let jobid = &jobid1.;
%let jobida = &jobid1a.;


data &client._member;
length agecat $55.;
set &client._member;
dob=input(compress(memberdob,"/"),anydtdte.);
format dob mmddyy10.;
age = floor((intck('month',dob,&rend) - (day(&rend) < day(dob))) / 12);
if age <18 then agecat='<18';
else if 18<=age<36 then agecat='18-35';
else if 36<=age<51 then agecat='36-50';
else if 51<=age<=65 then agecat='51-65';
else if age>65 then agecat='>65';
run;

%if &truncate.=0 %then   
%ce_eng_t_no_trunc; 
 %else %do;
%truncate(&truncate.);
%ce_eng_t;
%end;

%ce_rs;


%run2;
%run2tier;

%let rstart =&rstart2.;
%let rend=&rend2.;
%let year=&year2.;
%let factor=&factor2.;

%let yearmonth=&yearmonth2.;
%let jobid =&jobid2. ;
%let jobida = &jobid2a.;

data &client._member;
length agecat $55.;
set &client._member;
dob=input(compress(memberdob,"/"),anydtdte.);
format dob mmddyy10.;
age = floor((intck('month',dob,&rend) - (day(&rend) < day(dob))) / 12);
if age <18 then agecat='<18';
else if 18<=age<36 then agecat='18-35';
else if 36<=age<51 then agecat='36-50';
else if 51<=age<=65 then agecat='51-65';
else if age>65 then agecat='>65';
run;

%if &truncate.=0 %then  
%ce_eng_t_no_trunc; 
 %else  %do;
%truncate(&truncate.);
%ce_eng_t;
%end;

%ce_rs;

%run2;
%run2tier;
data &client._all_final_overall;
set &client._all_final_&year1. &client._all_final_&year2.;
run;

data &client._all_final_hier;
set &client._all_final_&year1. &client._all_final_&year2.;

if contact_type='Biometrics' then hierarchy=5;
else if contact_type='HA Case Calls' then hierarchy=4;
else if contact_type='MEP Login' then hierarchy=7; else if contact_type='HCE' then hierarchy=8;
else if contact_type='Wellness Challenges' then hierarchy=2;
else if contact_type='Wellness Workshops' then hierarchy=1;
else if contact_type='Wellness Tracker' then hierarchy=3;
else if contact_type='Wellness Commitments' then hierarchy=6;

if group='Engaged' and hierarchy ne .;

run;

proc sort data=&client._all_final_hier; by run year population hierarchy; run;

data &client._all_final_tier;
length population $55. run $10. group $55. year 8.  contact_Type $55. cost_location $55. cost_attributed $55. total_cost 8  visits 8;
set &client._all_final_&year1._tier (rename=(engage_tier=tier)) &client._all_final_&year2._tier (rename=(engage_tier=tier));

if tier =1 then engage_tier='High';
else if tier=2 then engage_tier='Low';
drop tier;
run;

data &client._all_final_tier;
length population $55. run $10. group $55. year 8.  contact_Type $55. cost_location $55. cost_attributed $55. total_cost 8  visits 8;
set &client._all_final_tier;
run;

data &client._all_combine;
length  population $55. run $10. group $55. year 8. contact_type $70.  engage_tier $8. cost_location $55. cost_attributed $55. total_cost 8  members employees visits 8 age_category $55.;

set &client._all_final_overall
&client._all_final_hier
&client._all_final_tier;
if group ='Outreach' then rank=1;
else if group ='Non-Outreach' then rank=2;
else if group ='Engaged' then rank=3;
else if group = 'Non-Engaged' then rank=4;
else if group ='Total' then rank=5;

if cost_location='Inpatie' then cost_location='Inpatient';
else if cost_location='Outpati' then cost_location='Outpatient';
else if cost_location='Profess' then cost_location='Professional';
else if cost_location ='Pharmac' then cost_location='Pharmacy';

if cost_attributed='Member' then do;
pmpy_cost=total_cost/members;
pmpy_cost_clm=total_cost/members_clm;
end;

else if cost_attributed='Employee' then do;
pmpy_cost=total_cost/employees;
end;


run;


data &client._all_risk;
set &client._risk_&year1. (in=a)
&client._risk_&year2. (in=b);
if a then year=&year1.;
if b then year=&year2.;
run;
data &client._all_risk2;
set &client._risk2_&year1. (in=a)
&client._risk2_&year2. (in=b);
if a then year=&year1.;
if b then year=&year2.;
run;

proc sql; create table &client._all_combine2 as select distinct a.*,b.average as avg_risk
from  &client._all_combine as a left join &client._all_risk as b
on a.year=b.year
and a.group=b.group;
quit;
proc sql; create table &client._all_combine2 as select distinct a.*,b.average as avg_risk2
from  &client._all_combine2 as a left join &client._all_risk2 as b
on a.year=b.year
and a.group=b.group;
quit;

data &client._all_combine2;
set &client._all_combine2;

if pmpy_cost ne . then do;
pmpy_cost_risk = pmpy_cost/avg_risk;
end;

if pmpy_cost_clm ne . then do;
pmpy_cost_clm_risk=pmpy_cost_clm/avg_risk;
end;
run;

proc sort data=&client._all_combine2; by rank year descending cost_attributed hierarchy ; run;


data &client._all_combine2;
set &client._all_combine2;
drop rank;
run;



/**********
data &client._all_risk2;
set &client._risk2_&year1. (in=a)
&client._risk2_&year2. (in=b);
if a then year=&year1.;
if b then year=&year2.;
run;

proc sql; create table &client._all_combine2_2 as select distinct a.*,b.average as avg_risk
from  &client._all_combine as a left join &client._all_risk2 as b
on a.year=b.year
and a.group=b.group;
quit;

proc sort data=&client._all_combine2_2; by rank year descending cost_attributed hierarchy ; run;

data &client._all_combine2_2;
rename avg_risk=avg_risk2;
set &client._all_combine2_2;
drop rank;
run;
*/
/*combine*/

data &client._all;
length population $55. run $5. group $55. year 8. contact_type $55. ;
set 
&client._all_combine1
&client._all_combine2

;

run;

data &client._all;
length population $55. run $5. group $55. year 8. contact_type $55.  cost_location $55.;
format contact_type $55.;
set  &client._all;

if contact_type='Wellness Commitme' then contact_type='Wellness Commitments';
if contact_type='Wellness Challeng' then contact_type='Wellness Challenges';
if contact_type='Automated Outboun' then contact_type='Automated Outbound';

if cost_location='Materni' then cost_location='Maternity';
if cost_location='Office' then cost_location='Office Visit';
/*drop age_catgory;*/
run;

proc sort data=&client._all; by run; run;


*%let dt = %sysfunc(today(), yymmddn8.);
%let dt = %sysfunc(translate(%sysfunc(datetime(),datetime20.3),--,.:)); ;

%if &truncate.=0 %then   
%let name=&path.\&client._&y1s..&m1s.-&y2e..&m2e._&dt..csv;
%else
%let name=&path.\&client._&y1s..&m1s.-&y2e..&m2e._truncated_at_&truncate._&dt..csv;

proc export data=&client._all outfile="&name."  dbms=csv replace;
run;

%mend ;



/* 
%export(client=xtrspace, orgid=1127631, orgid1=1127631 1043255, y1s=2016, y1e=2017, m1s=08, m1e=07, 
d1s=01,d1e=31, y2s=2017, y2e=2018, m2s=08, m2e=07, d2s=01,d2e=31,  jobid1=34433, jobid2=34431, 
truncate=250000,path=c:\users\ssong\downloads);
 



%export(client=xtrspace, orgid=1127631, orgid1=1127631 1043255, y1s=2016, y1e=2017, m1s=08, m1e=07, d1s=01,d1e=31,  
y2s=2017, y2e=2018, m2s=08, m2e=07, d2s=01,d2e=31,  jobid1=34433, jobid2=34431, truncate=0,
path=c:\users\ssong\downloads);
 


*%export(client=genesis, orgid=, orgid1= 1127485, y1s=2016, y1e=2017, 
m1s=08, m1e=07, d1s=01,d1e=31, y2s=2017, y2e=2018, m2s=08, m2e=07, d2s=01,d2e=31,  jobid1=34445, jobid2=34443, truncate=0,path=u:\sas\docs\roi);


*/
 
 %export(client=brock, orgid=, orgid1= 1070827, y1s=2016, y1e=2017, m1s=08, m1e=07, d1s=01,d1e=31,  
y2s=2017, y2e=2018, m2s=08, m2e=07, d2s=01,d2e=31,  jobid1=34448, jobid2=34451, jobid1a=34448, jobid2a=34451, truncate=000,
path=c:\users\ssong\downloads);


 
