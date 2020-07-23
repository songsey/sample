/*================================================================================
/ Organization	: []Health, Inc. 
/ Program		: 
/ Version		: 
/ Date			: 14 Jan 2019
/ Contact:		: 
/ Purpose		: get S&P healthcare index 
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



/*proc sql; create table a as 
select avg(sp_2016_pmpy_tot) from
sp_2016_total;
quit;
proc sql; create table a2 as 
select avg(sp_2017_pmpy_tot) from
sp_2017_total;
quit;

proc sql;create table a as
select count(*) from
sp_2016_total;
 create table a2 as
select count(*) from
sp_2017_total;
quit;

data a3;
set sp_2016_total;
run;
data a4;
set sp_2017_total;
run;
proc export data=a3 outfile="c:\users\ssong\downloads\su16.csv" dbms=csv; run;
proc export data=a4 outfile="c:\users\ssong\downloads\su17.csv" dbms=csv; run;



data a4;
set sp_2017_total;
where c_20171231^=.;
run;


data atest;
set newmont_2016_total_s4;
run;
data atest17;
set newmont_2017_total_s4;
run;
proc export data=atest outfile="c:\users\ssong\downloads\newmont16.csv" dbms=csv; run;
proc export data=atest17 outfile="c:\users\ssong\downloads\newmont17.csv" dbms=csv; run;

*/

 
%macro benchmark(sysparm=,client=, orgid1=,path=);
*libname "c:\users\ssong\downloads\mass run\data";
libname sp odbc dsn="S_P";

libname SASO "O:\SAS\Data\[]";
libname bgdd 	  "O:\SAS\Data\[] standard data";
libname feeds odbc dsn="ENGAGEDB_DATAFEEDS";


options mprint symbolgen  ;
options nomstored;
 

options dlcreatedir;


 /*create member data */
libname ce_in "c:\users\ssong\downloads\mass run\data";
libname ce_out "C:\Users\ssong\Downloads\mass run\data";
 
data &client._member;
set bgdd.[]_member_fixed;
where organizationid in (&orgid1.);
run;

data ce_in.&client._cohort;
set &client._member;
run;

%include "C:\Users\ssong\Downloads\mass run\lib\CE_rpt.sas";
 
%let m1s=%substr(%scan(&sysparm.,1),5,2);
%let d1s=%substr(%scan(&sysparm.,1),7,2);
%let y1s=%substr(%scan(&sysparm.,1),1,4);
%let m1e=%substr(%scan(&sysparm.,12),5,2);
%let d1e=%substr(%scan(&sysparm.,12),7,2);
%let y1e=%substr(%scan(&sysparm.,12),1,4);
%let m2s=%substr(%scan(&sysparm.,13),5,2);
%let d2s=%substr(%scan(&sysparm.,13),7,2);
%let y2s=%substr(%scan(&sysparm.,13),1,4);
%let m2e=%substr(%scan(&sysparm.,24),5,2);
%let d2e=%substr(%scan(&sysparm.,24),7,2);
%let y2e=%substr(%scan(&sysparm.,24),1,4);


%let rstart1=%sysfunc(mdy(&m1s.,01,&y1s.));
%let rend1=%sysfunc(mdy(&m1e.,&d1e.,&y1e.));
  
 
%let rstart2=%sysfunc(mdy(&m2s.,01,&y2s.));
%let rend2=%sysfunc(mdy(&m2e.,&d2e.,&y2e.));
 

/*ce*/
		%ce(client=&client.,
          cohort=cohort,
          rpt=yr1,
          rstart=&rstart1.,
          rend=&rend1.
          );

	      %ce(client=&client.,
          cohort=cohort,
          rpt=yr2,
          rstart=&rstart2.,
          rend=&rend2.
          );


proc sql; create table &client._cohort_ce_yr2 as 
select distinct memid from ce_out.&client._cohort_ce_yr2
;

proc sql; create table &client._cohort_ce_yr1 as 
select distinct memid from ce_out.&client._cohort_ce_yr1
;

quit;
 

 
%let c1=%sysfunc(cats(col_,%scan(&sysparm.,1)) );
%put &c1;
%let c2=%sysfunc(cats(col_,%scan(&sysparm.,2)) );
%put &c2;
%let c3=%sysfunc(cats(col_,%scan(&sysparm.,3)) );
%let c4=%sysfunc(cats(col_,%scan(&sysparm.,4)) );
%let c5=%sysfunc(cats(col_,%scan(&sysparm.,5)) );
%let c6=%sysfunc(cats(col_,%scan(&sysparm.,6)) );
%let c7=%sysfunc(cats(col_,%scan(&sysparm.,7)) );
%let c8=%sysfunc(cats(col_,%scan(&sysparm.,8)) );
%let c9=%sysfunc(cats(col_,%scan(&sysparm.,9)) );
%let c10=%sysfunc(cats(col_,%scan(&sysparm.,10)) );
%let c11=%sysfunc(cats(col_,%scan(&sysparm.,11)) );
%let c12=%sysfunc(cats(col_,%scan(&sysparm.,12)) );
%let d1=%sysfunc(cats(col_,%scan(&sysparm.,13)) );
%let d2=%sysfunc(cats(col_,%scan(&sysparm.,14)) );
%let d3=%sysfunc(cats(col_,%scan(&sysparm.,15)) );
%let d4=%sysfunc(cats(col_,%scan(&sysparm.,16)) );
%let d5=%sysfunc(cats(col_,%scan(&sysparm.,17)) );
%let d6=%sysfunc(cats(col_,%scan(&sysparm.,18)) );
%let d7=%sysfunc(cats(col_,%scan(&sysparm.,19)) );
%let d8=%sysfunc(cats(col_,%scan(&sysparm.,20)) );
%let d9=%sysfunc(cats(col_,%scan(&sysparm.,21)) );
%let d10=%sysfunc(cats(col_,%scan(&sysparm.,22)) );
%let d11=%sysfunc(cats(col_,%scan(&sysparm.,23)) );
%let d12=%sysfunc(cats(col_,%scan(&sysparm.,24)) );


proc sql; create table sp_2016_total as select distinct region, geo_code, measure, inc_pd, lob_code, sector, series, 
&c1, &c2, &c3, &c4 ,
&c5, &c6, &c7, &c8, &c9,
&c10, &c11, &c12
from sp.s_p_indices_auto
where sector in ('HC') and measure='HT' and inc_pd='MF' and lob_code in ('HLG') and series='PMPM' 
and &c12 ne '' and input(&c12,8.)>0  /*  and &c11 ne ''   and &c10 ne ''  and &c9 ne ''   and &c8 ne ''    and &c7 ne ''  
and  &c6 ne ''  and &c5 ne ''   and &c4 ne ''   and &c3 ne ''   and &c2 ne ''    and &c1 ne '' */
/*and source like '%201901%'*/     

order by region,geo_code;
quit;

proc sort data= sp_2016_total nodupkey; by region geo_code; run;

data sp_2016_total;
set sp_2016_total;
c_20160131=input(&c1,8.);
c_20160229=input(&c2,8.);
c_20160331=input(&c3,8.);
c_20160430=input(&c4,8.);
c_20160531=input(&c5,8.);
c_20160630=input(&c6,8.);
c_20160731=input(&c7,8.);
c_20160831=input(&c8,8.);
c_20160930=input(&c9,8.);
c_20161031=input(&c10,8.);
c_20161130=input(&c11,8.);
c_20161231=input(&c12,8.);
drop
&c1
&c2
&c3
&c4
&c5
&c6
&c7
&c8
&c9
&c10
&c11
&c12;
run;

data sp_2016_total;
set sp_2016_total;
sp_2016_pmpy_tot=sum(0, c_20160131, c_20160229, c_20160331, c_20160430, c_20160531, c_20160630, c_20160731, c_20160831, c_20160930,
c_20161031, c_20161130, c_20161231);
run;

/*2016 inpatient*/

proc sql; create table sp_2016_inp as select distinct region, geo_code, measure, inc_pd, lob_code, sector, series, &c1, &c2, &c3, &c4 ,
&c5, &c6, &c7, &c8, &c9,
&c10, &c11, &c12
from sp.s_p_indices_auto
where sector in ('HCMDFAIN') and measure='HT' and inc_pd='MF' and lob_code in ('HLG') and series='PMPM' 
and &c12 ne '' and input(&c12,8.)>0  /*and &c11 ne ''   and &c10 ne ''  and &c9 ne ''   and &c8 ne ''    and &c7 ne ''  
and  &c6 ne ''  and &c5 ne ''   and &c4 ne ''   and &c3 ne ''   and &c2 ne ''    and &c1 ne '' */
/*and source like '%201901%'*/     

order by region,geo_code;
quit;

proc sort data= sp_2016_inp nodupkey; by region geo_code; run;

data sp_2016_inp;
set sp_2016_inp;
c_20160131=input(&c1,8.);
c_20160229=input(&c2,8.);
c_20160331=input(&c3,8.);
c_20160430=input(&c4,8.);
c_20160531=input(&c5,8.);
c_20160630=input(&c6,8.);
c_20160731=input(&c7,8.);
c_20160831=input(&c8,8.);
c_20160930=input(&c9,8.);
c_20161031=input(&c10,8.);
c_20161130=input(&c11,8.);
c_20161231=input(&c12,8.);
drop
&c1
&c2
&c3
&c4
&c5
&c6
&c7
&c8
&c9
&c10
&c11
&c12;
run;

data sp_2016_inp;
set sp_2016_inp;
sp_2016_pmpy_inp=sum(0,c_20160131, c_20160229, c_20160331, c_20160430, c_20160531, c_20160630, c_20160731, c_20160831, c_20160930,
c_20161031, c_20161130, c_20161231);
run;

/*2016 outpatient*/

proc sql; create table sp_2016_out as select distinct region, geo_code, measure, inc_pd, lob_code, sector, series, &c1, &c2, &c3, &c4 ,
&c5, &c6, &c7, &c8, &c9,
&c10, &c11, &c12
from sp.s_p_indices_auto
where sector in ('HCMDFAOU') and measure='HT' and inc_pd='MF' and lob_code in ('HLG') and series='PMPM' 
and &c12 ne '' and input(&c12,8.)>0 /* and &c11 ne ''   and &c10 ne ''  and &c9 ne ''   and &c8 ne ''    and &c7 ne ''  
and  &c6 ne ''  and &c5 ne ''   and &c4 ne ''   and &c3 ne ''   and &c2 ne ''    and &c1 ne '' */
/*and source like '%201901%'*/     

order by region,geo_code;
quit;

proc sort data= sp_2016_out nodupkey; by region geo_code; run;

data sp_2016_out;
set sp_2016_out;
c_20160131=input(&c1,8.);
c_20160229=input(&c2,8.);
c_20160331=input(&c3,8.);
c_20160430=input(&c4,8.);
c_20160531=input(&c5,8.);
c_20160630=input(&c6,8.);
c_20160731=input(&c7,8.);
c_20160831=input(&c8,8.);
c_20160930=input(&c9,8.);
c_20161031=input(&c10,8.);
c_20161130=input(&c11,8.);
c_20161231=input(&c12,8.);
drop
&c1
&c2
&c3
&c4
&c5
&c6
&c7
&c8
&c9
&c10
&c11
&c12;
run;

data sp_2016_out;
set sp_2016_out;
sp_2016_pmpy_out=sum(0,c_20160131, c_20160229, c_20160331, c_20160430, c_20160531, c_20160630, c_20160731, c_20160831, c_20160930,
c_20161031, c_20161130, c_20161231);
run;

/*2016 professional*/

proc sql; create table sp_2016_pf as select distinct region, geo_code, measure, inc_pd, lob_code, sector, series, &c1, &c2, &c3, &c4 ,
&c5, &c6, &c7, &c8, &c9,
&c10, &c11, &c12
from sp.s_p_indices_auto
where sector in ('HCMDPS') and measure='HT' and inc_pd='MF' and lob_code in ('HLG') and series='PMPM'
and &c12 ne '' and input(&c12,8.)>0  /* and &c11 ne ''   and &c10 ne ''  and &c9 ne ''   and &c8 ne ''    and &c7 ne ''  
and  &c6 ne ''  and &c5 ne ''   and &c4 ne ''   and &c3 ne ''   and &c2 ne ''    and &c1 ne '' */
/*and source like '%201901%'*/     

order by region,geo_code;
quit;

proc sort data= sp_2016_pf nodupkey; by region geo_code; run;

data sp_2016_pf;
set sp_2016_pf;
c_20160131=input(&c1,8.);
c_20160229=input(&c2,8.);
c_20160331=input(&c3,8.);
c_20160430=input(&c4,8.);
c_20160531=input(&c5,8.);
c_20160630=input(&c6,8.);
c_20160731=input(&c7,8.);
c_20160831=input(&c8,8.);
c_20160930=input(&c9,8.);
c_20161031=input(&c10,8.);
c_20161130=input(&c11,8.);
c_20161231=input(&c12,8.);
drop
&c1
&c2
&c3
&c4
&c5
&c6
&c7
&c8
&c9
&c10
&c11
&c12;
run;

data sp_2016_pf;
set sp_2016_pf;
sp_2016_pmpy_pf=sum(0,c_20160131, c_20160229, c_20160331, c_20160430, c_20160531, c_20160630, c_20160731, c_20160831, c_20160930,
c_20161031, c_20161130, c_20161231);
run;

/*2016 rx*/

proc sql; create table sp_2016_rx as select distinct region, geo_code, measure, inc_pd, lob_code, sector, series, &c1, &c2, &c3, &c4 ,
&c5, &c6, &c7, &c8, &c9,
&c10, &c11, &c12
from sp.s_p_indices_auto
where sector in ('HCRX') and measure='HT' and inc_pd='MF' and lob_code in ('HLG') and series='PMPM' 
and &c12 ne '' and input(&c12,8.)>0 /* and &c11 ne ''   and &c10 ne ''  and &c9 ne ''   and &c8 ne ''    and &c7 ne ''  
and  &c6 ne ''  and &c5 ne ''   and &c4 ne ''   and &c3 ne ''   and &c2 ne ''    and &c1 ne '' */
/*and source like '%201901%'*/     

order by region,geo_code;
quit;

proc sort data= sp_2016_rx nodupkey; by region geo_code; run;

data sp_2016_rx;
set sp_2016_rx;
c_20160131=input(&c1,8.);
c_20160229=input(&c2,8.);
c_20160331=input(&c3,8.);
c_20160430=input(&c4,8.);
c_20160531=input(&c5,8.);
c_20160630=input(&c6,8.);
c_20160731=input(&c7,8.);
c_20160831=input(&c8,8.);
c_20160930=input(&c9,8.);
c_20161031=input(&c10,8.);
c_20161130=input(&c11,8.);
c_20161231=input(&c12,8.);
drop
&c1
&c2
&c3
&c4
&c5
&c6
&c7
&c8
&c9
&c10
&c11
&c12;
run;

data sp_2016_rx;
set sp_2016_rx;
sp_2016_pmpy_rx=sum(0,c_20160131, c_20160229, c_20160331, c_20160430, c_20160531, c_20160630, c_20160731, c_20160831, c_20160930,
c_20161031, c_20161130, c_20161231);
run;

/*2017 sp*/

proc sql; create table sp_2017_total as select distinct region, geo_code, measure, inc_pd, lob_code, sector, series, &d1, &d2, &d3, &d4,
&d5, &d6, &d7, &d8, &d9,
&d10, &d11, &d12
from sp.s_p_indices_auto
where sector in ('HC') and measure='HT' and inc_pd='MF' and lob_code in ('HLG') and series='PMPM' 
and &d12 ne '' and input(&d12,8.)>0 /*and &d11 ne ''   and &d10 ne ''  and &d9 ne ''   and &d8 ne ''    and &d7 ne ''  
and  &d6 ne ''  and &d5 ne ''   and &d4 ne ''   and &d3 ne ''   and &d2 ne ''    and &d1 ne '' */
/*and source like '%201901%'*/     

order by region,geo_code;
quit;

 

proc sort data= sp_2017_total nodupkey; by region geo_code; run;

data sp_2017_total;
set sp_2017_total;
c_20170131=input(&d1,8.);
c_20170228=input(&d2,8.);
c_20170331=input(&d3,8.);
c_20170430=input(&d4,8.);
c_20170531=input(&d5,8.);
c_20170630=input(&d6,8.);
c_20170731=input(&d7,8.);
c_20170831=input(&d8,8.);
c_20170930=input(&d9,8.);
c_20171031=input(&d10,8.);
c_20171130=input(&d11,8.);
c_20171231=input(&d12,8.);
drop
&d1
&d2
&d3
&d4
&d5
&d6
&d7
&d8
&d9
&d10
&d11
&d12;
run;

data sp_2017_total;
set sp_2017_total;
sp_2017_pmpy_tot=sum(0,c_20170131, c_20170228, c_20170331, c_20170430,
c_20170531, c_20170630, c_20170731, c_20170831, c_20170930, c_20171031, c_20171130, c_20171231);
run;

/*2017 inpatient*/

proc sql; create table sp_2017_inp as select distinct region, geo_code, measure, inc_pd, lob_code, sector, series, &d1, &d2, &d3, &d4,
&d5, &d6, &d7, &d8, &d9,
&d10, &d11, &d12
from sp.s_p_indices_auto
where sector in ('HCMDFAIN') and measure='HT' and inc_pd='MF' and lob_code in ('HLG') and series='PMPM'
and &d12 ne ''  and input(&d12,8.) >0 /* and &d11 ne ''   and &d10 ne ''  and &d9 ne ''   and &d8 ne ''    and &d7 ne ''  
and  &d6 ne ''  and &d5 ne ''   and &d4 ne ''   and &d3 ne ''   and &d2 ne ''    and &d1 ne '' */
/*and source like '%201901%'*/     

order by region,geo_code;
quit;



proc sort data= sp_2017_inp nodupkey; by region geo_code; run;

data sp_2017_inp;
set sp_2017_inp;
c_20170131=input(&d1,8.);
c_20170228=input(&d2,8.);
c_20170331=input(&d3,8.);
c_20170430=input(&d4,8.);
c_20170531=input(&d5,8.);
c_20170630=input(&d6,8.);
c_20170731=input(&d7,8.);
c_20170831=input(&d8,8.);
c_20170930=input(&d9,8.);
c_20171031=input(&d10,8.);
c_20171130=input(&d11,8.);
c_20171231=input(&d12,8.);
drop
&d1
&d2
&d3
&d4
&d5
&d6
&d7
&d8
&d9
&d10
&d11
&d12;
run;

data sp_2017_inp;
set sp_2017_inp;
sp_2017_pmpy_inp=sum(0,c_20170131, c_20170228, c_20170331, c_20170430,
c_20170531, c_20170630, c_20170731, c_20170831, c_20170930, c_20171031, c_20171130, c_20171231);
run;

/*2017 outpatient*/

proc sql; create table sp_2017_out as select distinct region, geo_code, measure, inc_pd, lob_code, sector, series, &d1, &d2, &d3, &d4,
&d5, &d6, &d7, &d8, &d9,
&d10, &d11, &d12
from sp.s_p_indices_auto
where sector in ('HCMDFAOU') and measure='HT' and inc_pd='MF' and lob_code in ('HLG') and series='PMPM' 
and &d12 ne ''  and input(&d12,8.)>0 /* and &d11 ne ''   and &d10 ne ''  and &d9 ne ''   and &d8 ne ''    and &d7 ne ''  
and  &d6 ne ''  and &d5 ne ''   and &d4 ne ''   and &d3 ne ''   and &d2 ne ''    and &d1 ne '' */
/*and source like '%201901%'*/     

order by region,geo_code;
quit;

 


proc sort data= sp_2017_out nodupkey; by region geo_code; run;

data sp_2017_out;
set sp_2017_out;
c_20170131=input(&d1,8.);
c_20170228=input(&d2,8.);
c_20170331=input(&d3,8.);
c_20170430=input(&d4,8.);
c_20170531=input(&d5,8.);
c_20170630=input(&d6,8.);
c_20170731=input(&d7,8.);
c_20170831=input(&d8,8.);
c_20170930=input(&d9,8.);
c_20171031=input(&d10,8.);
c_20171130=input(&d11,8.);
c_20171231=input(&d12,8.);
drop
&d1
&d2
&d3
&d4
&d5
&d6
&d7
&d8
&d9
&d10
&d11
&d12;
run;

data sp_2017_out;
set sp_2017_out;
sp_2017_pmpy_out=sum(0,c_20170131, c_20170228, c_20170331, c_20170430,
c_20170531, c_20170630, c_20170731, c_20170831, c_20170930, c_20171031, c_20171130, c_20171231);
run;

/*2017 professional*/


proc sql; create table sp_2017_pf as select distinct region, geo_code, measure, inc_pd, lob_code, sector, series, &d1, &d2, &d3, &d4,
&d5, &d6, &d7, &d8, &d9,
&d10, &d11, &d12
from sp.s_p_indices_auto
where sector in ('HCMDPS') and measure='HT' and inc_pd='MF' and lob_code in ('HLG') and series='PMPM'
and &d12 ne ''  and input(&d12,8.)>0   /* and &d11 ne ''   and &d10 ne ''  and &d9 ne ''   and &d8 ne ''    and &d7 ne ''  
and  &d6 ne ''  and &d5 ne ''   and &d4 ne ''   and &d3 ne ''   and &d2 ne ''    and &d1 ne '' */
/*and source like '%201901%'*/     

order by region,geo_code;
quit;

 

proc sort data= sp_2017_pf nodupkey; by region geo_code; run;

data sp_2017_pf;
set sp_2017_pf;
c_20170131=input(&d1,8.);
c_20170228=input(&d2,8.);
c_20170331=input(&d3,8.);
c_20170430=input(&d4,8.);
c_20170531=input(&d5,8.);
c_20170630=input(&d6,8.);
c_20170731=input(&d7,8.);
c_20170831=input(&d8,8.);
c_20170930=input(&d9,8.);
c_20171031=input(&d10,8.);
c_20171130=input(&d11,8.);
c_20171231=input(&d12,8.);
drop
&d1
&d2
&d3
&d4
&d5
&d6
&d7
&d8
&d9
&d10
&d11
&d12;
run;

data sp_2017_pf;
set sp_2017_pf;
sp_2017_pmpy_pf=sum(0,c_20170131, c_20170228, c_20170331, c_20170430,
c_20170531, c_20170630, c_20170731, c_20170831, c_20170930, c_20171031, c_20171130, c_20171231);
run;

/*2017 rx*/

proc sql; create table sp_2017_rx as select distinct region, geo_code, measure, inc_pd, lob_code, sector, series, &d1, &d2, &d3, &d4,
&d5, &d6, &d7, &d8, &d9,
&d10, &d11, &d12
from sp.s_p_indices_auto
where sector in ('HCRX') and measure='HT' and inc_pd='MF' and lob_code in ('HLG') and series='PMPM'
and &d12 ne ''  and input(&d12,8.) >0  /* and &d11 ne ''   and &d10 ne ''  and &d9 ne ''   and &d8 ne ''    and &d7 ne ''  
and  &d6 ne ''  and &d5 ne ''   and &d4 ne ''   and &d3 ne ''   and &d2 ne ''    and &d1 ne '' */
/*and source like '%201901%'*/     

order by region,geo_code;
quit;

 

proc sort data= sp_2017_rx nodupkey; by region geo_code; run;

data sp_2017_rx;
set sp_2017_rx;
c_20170131=input(&d1,8.);
c_20170228=input(&d2,8.);
c_20170331=input(&d3,8.);
c_20170430=input(&d4,8.);
c_20170531=input(&d5,8.);
c_20170630=input(&d6,8.);
c_20170731=input(&d7,8.);
c_20170831=input(&d8,8.);
c_20170930=input(&d9,8.);
c_20171031=input(&d10,8.);
c_20171130=input(&d11,8.);
c_20171231=input(&d12,8.);
drop
&d1
&d2
&d3
&d4
&d5
&d6
&d7
&d8
&d9
&d10
&d11
&d12;
run;

data sp_2017_rx;
set sp_2017_rx;
sp_2017_pmpy_rx=sum(0,c_20170131, c_20170228, c_20170331, c_20170430,
c_20170531, c_20170630, c_20170731, c_20170831, c_20170930, c_20171031, c_20171130, c_20171231);
run;


/*run other macro first*/
data &client._total_2016;
set &client._cohort_ce_yr1;
run;

data &client._total_2017;
set &client._cohort_ce_yr2;
run;
 


PROC IMPORT OUT= WORK.s_p_location_codes 
            DATAFILE= "U:\Users\Cathy\s_p_location_codes.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="Sheet1$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

/******************************************************************************************************/
/******************************************************************************************************/
/******************************************************************************************************/
/******************************************************************************************************/
/******************************************************************************************************/
/******************************************************************************************************/
/******************************************************************************************************/
/******************************************************************************************************/
/******************************************************************************************************/
/******************************************************************************************************/
/******************************************************************************************************/
/******************************************************************************************************/


/*2016 total*/

proc sql; create table &client._total_2016a as select distinct a.*,b.city,b.state,b.zipcode
from &client._total_2016 as a inner join feeds.eligibility_current as b
on a.memid=b.memberid;
quit;

proc sql; create table &client._total_2016b as select distinct a.*,b.geography,ind
from &client._total_2016a as a left join s_p_location_codes as b
on upcase(a.city)=upcase(b.city)
and upcase(a.state)=upcase(b.state)
and ind='Metro';
quit;

data &client._total_2016b;
set &client._total_2016b;
if geography='' then do;

if upcase(city)='FORT WORTH' and upcase(state)='TX' then geography='S14';
else if upcase(city)='FORT LAUDERDALE' and upcase(state)='FL' then geography='S20';
else if upcase(city)='DURHAM' and upcase(state)='NC' then geography='S26';
else if upcase(city) IN ('ST PETERSBURG', 'ST. PETERSBURG','CLEARWATER') and upcase(state)='FL' then geography='S29';
else if upcase(city) in ('SAN FRANCISCO',' OAKLAND') and upcase(state) ='CA' then geography='W18';
end;
run;

proc sql; create table &client._total_2016c as select distinct a.*,b.geography as geography2, b.ind as in&d2
from &client._total_2016b as a left join s_p_location_codes as b
on upcase(a.state)=upcase(b.state)
and b.ind='State';
quit;

data &client._total_2016c;
set &client._total_2016c;
if geography='' then geography=geography2;
if ind='' then ind=in&d2;
drop geography2 in&d2;
run;

proc sql; create table &client._total_2016d as select distinct a.*,avg(b.sp_2016_pmpy_tot) as sp_2016_pmpy_tot
from &client._total_2016c as a left join sp_2016_total as b
on a.geography=b.geo_code
group by memid, geography;
quit;


/*proc sort data=&client._total_2016d nodupkey; by memid geography ; run;
*/

proc sql; create table &client._total_2016d as select distinct a.*,b.description from
&client._total_2016d as a left join s_p_location_codes as b
on a.geography=b.geography;
quit;

data  &client._total_2016e;
set &client._total_2016d;
geo1=substr(geography,1,1);
if sp_2016_pmpy_tot=. and substr(description,1,1)='H' then do;
geography=cats(geo1,state);
description='';
end;

else if sp_2016_pmpy_tot=. and substr(description,1,1)='S' then do;
geography=cats(geo1,"--");
description='';
end;

else if geography='' then geography='US-';
keep geography memid state;
run;

/*loop 2*/

proc sql; create table &client._total_2016f as select distinct a.*,avg(b.sp_2016_pmpy_tot) as sp_2016_pmpy_tot
from &client._total_2016e as a left join sp_2016_total as b
on a.geography=b.geo_code
group by memid, geography;
quit;

/*proc sort data=&client._total_2016f nodupkey; by memid geography ; run;*/


proc sql; create table &client._total_2016f as select distinct a.*,b.description from
&client._total_2016f as a left join s_p_location_codes as b
on a.geography=b.geography;
quit;


data  &client._total_2016g;
set &client._total_2016f;
geo1=substr(geography,1,1);
if sp_2016_pmpy_tot=. and substr(description,1,1)='H' then do;
geography=cats(geo1,state);
description='';
end;

else if sp_2016_pmpy_tot=. and substr(description,1,1)='S' then do;
geography=cats(geo1,"--");
description='';
end;

else if geography='' then geography='US-';
keep geography memid state;
run;


/*loop 3*/

proc sql; create table &client._total_2016h as select distinct a.*, avg(b.sp_2016_pmpy_tot) as sp_2016_pmpy_tot
from &client._total_2016g as a left join sp_2016_total as b
on a.geography=b.geo_code
group by memid, geography ;
quit;

/*proc sort data=&client._total_2016h nodupkey; by memid geography ; run;*/

proc sql; create table &client._total_2016h as select distinct a.*,b.description from
&client._total_2016h as a left join s_p_location_codes as b
on a.geography=b.geography;
quit;


proc sql; create table &client._total_2016_sum as select distinct geography, sp_2016_pmpy_tot,count(distinct memid) as members
from &client._total_2016h 
group by geography
order by geography;
quit;

proc sql; create table &client._total_2016_cnt as select count(distinct memid) as tot_members
from &client._total_2016h ;
quit;


/*merge*/
/*total*/
proc sql; create table &client._2016_total_s as select distinct a.*,b.*
from &client._total_2016_sum a, &client._total_2016_cnt b;
quit;

data &client._2016_total_s2;
set &client._2016_total_s;
pct=members/tot_members;
run;

proc sql; create table &client._2016_total_s3 as select distinct a.*,b.description from
&client._2016_total_s2 as a left join s_p_location_codes as b
on a.geography=b.geography;
quit;

data &client._2016_total_s4;
set &client._2016_total_s3;
adj_pmpy=sp_2016_pmpy_tot*pct;
run;

proc sql; create table &client._2016_pmpy_tot as select sum(adj_pmpy) as weight_pmpy_2016_tot
from  &client._2016_total_s4; quit;



/*inp*/

proc sql; create table &client._2016_inp as select distinct a.*, avg(b.sp_2016_pmpy_inp) as sp_2016_pmpy_inp
from &client._2016_total_s2 as a left join sp_2016_inp as b
on a.geography=b.geo_code
group by geography;
quit;

data &client._2016_inp;
set &client._2016_inp;
adj_pmpy=sp_2016_pmpy_inp*pct;
run;

proc sql; create table  &client._2016_inp_s2 as select distinct a.*,b.description from
&client._2016_inp as a left join s_p_location_codes as b
on a.geography=b.geography;
quit;

proc sql; create table &client._2016_pmpy_inp as select sum(adj_pmpy) as weight_pmpy_2016_inp
from  &client._2016_inp_s2; quit;

/*out*/

proc sql; create table &client._2016_out as select distinct a.*,avg(b.sp_2016_pmpy_out) as sp_2016_pmpy_out
from &client._2016_total_s2 as a left join sp_2016_out as b
on a.geography=b.geo_code
group by geography;
quit;

data &client._2016_out;
set &client._2016_out;
adj_pmpy=sp_2016_pmpy_out*pct;
run;

proc sql; create table  &client._2016_out_s2 as select distinct a.*,b.description from
&client._2016_out as a left join s_p_location_codes as b
on a.geography=b.geography;
quit;

proc sql; create table &client._2016_pmpy_out as select sum(adj_pmpy) as weight_pmpy_2016_out
from  &client._2016_out_s2; quit;


/*prof*/

proc sql; create table &client._2016_pf as select distinct a.*, avg(b.sp_2016_pmpy_pf) as sp_2016_pmpy_pf
from &client._2016_total_s2 as a left join sp_2016_pf as b
on a.geography=b.geo_code
group by geography;
quit;

data &client._2016_pf;
set &client._2016_pf;
adj_pmpy=sp_2016_pmpy_pf*pct;
run;

proc sql; create table  &client._2016_pf_s2 as select distinct a.*,b.description from
&client._2016_pf as a left join s_p_location_codes as b
on a.geography=b.geography;
quit;

proc sql; create table &client._2016_pmpy_pf as select sum(adj_pmpy) as weight_pmpy_2016_pf
from  &client._2016_pf_s2; quit;

/*rx*/

proc sql; create table &client._2016_rx as select distinct a.*, avg(b.sp_2016_pmpy_rx) as sp_2016_pmpy_rx
from &client._2016_total_s2 as a left join sp_2016_rx as b
on a.geography=b.geo_code
group by geography;
quit;

data &client._2016_rx;
set &client._2016_rx;
adj_pmpy=sp_2016_pmpy_rx*pct;
run;

proc sql; create table  &client._2016_rx_s2 as select distinct a.*,b.description from
&client._2016_rx as a left join s_p_location_codes as b
on a.geography=b.geography;
quit;

proc sql; create table &client._2016_pmpy_rx as select sum(adj_pmpy) as weight_pmpy_2016_rx
from  &client._2016_rx_s2; quit;


/*2017 total*/

proc sql; create table &client._total_2017a as select distinct a.*,b.city,b.state,b.zipcode
from &client._total_2017 as a inner join feeds.eligibility_current as b
on a.memid=b.memberid;
quit;

proc sql; create table &client._total_2017b as select distinct a.*,b.geography,ind
from &client._total_2017a as a left join s_p_location_codes as b
on upcase(a.city)=upcase(b.city)
and upcase(a.state)=upcase(b.state)
and ind='Metro';
quit;

data &client._total_2017b;
set &client._total_2017b;
if geography='' then do;

if upcase(city)='FORT WORTH' and upcase(state)='TX' then geography='S14';
else if upcase(city)='FORT LAUDERDALE' and upcase(state)='FL' then geography='S20';
else if upcase(city)='DURHAM' and upcase(state)='NC' then geography='S26';
else if upcase(city) IN ('ST PETERSBURG', 'ST. PETERSBURG','CLEARWATER') and upcase(state)='FL' then geography='S29';
else if upcase(city) in ('SAN FRANCISCO',' OAKLAND') and upcase(state) ='CA' then geography='W18';
end;
run;

proc sql; create table &client._total_2017c as select distinct a.*,b.geography as geography2, b.ind as in&d2
from &client._total_2017b as a left join s_p_location_codes as b
on upcase(a.state)=upcase(b.state)
and b.ind='State';
quit;

data &client._total_2017c;
set &client._total_2017c;
if geography='' then geography=geography2;
if ind='' then ind=in&d2;
drop geography2 in&d2;
run;

proc sql; create table &client._total_2017d as select distinct a.*, avg(b.sp_2017_pmpy_tot) as sp_2017_pmpy_tot
from &client._total_2017c as a left join sp_2017_total as b
on a.geography=b.geo_code
group by memid, geography;
quit;

/*proc sort data=&client._total_2017d nodupkey; by memid geography ; run;
*/

proc sql; create table &client._total_2017d as select distinct a.*,b.description from
&client._total_2017d as a left join s_p_location_codes as b
on a.geography=b.geography;
quit;

data  &client._total_2017e;
set &client._total_2017d;
geo1=substr(geography,1,1);
if sp_2017_pmpy_tot=. and substr(description,1,1)='H' then do;
geography=cats(geo1,state);
description='';
end;

else if sp_2017_pmpy_tot=. and substr(description,1,1)='S' then do;
geography=cats(geo1,"--");
description='';
end;

else if geography='' then geography='US-';
keep geography memid state;
run;

/*loop 2*/

proc sql; create table &client._total_2017f as select distinct a.*, avg(b.sp_2017_pmpy_tot) as sp_2017_pmpy_tot
from &client._total_2017e as a left join sp_2017_total as b
on a.geography=b.geo_code
group by memid, geography ;
quit;


/*proc sort data=&client._total_2017f nodupkey; by memid geography ; run;
*/

proc sql; create table &client._total_2017f as select distinct a.*,b.description from
&client._total_2017f as a left join s_p_location_codes as b
on a.geography=b.geography;
quit;


data  &client._total_2017g;
set &client._total_2017f;
geo1=substr(geography,1,1);
if sp_2017_pmpy_tot=. and substr(description,1,1)='H' then do;
geography=cats(geo1,state);
description='';
end;

else if sp_2017_pmpy_tot=. and substr(description,1,1)='S' then do;
geography=cats(geo1,"--");
description='';
end;

else if geography='' then geography='US-';
keep geography memid state;
run;


/*loop 3*/

proc sql; create table &client._total_2017h as select distinct a.*,avg(b.sp_2017_pmpy_tot) as sp_2017_pmpy_tot
from &client._total_2017g as a left join sp_2017_total as b
on a.geography=b.geo_code
group by memid, geography ;
quit;


/*proc sort data=&client._total_2017h nodupkey; by memid geography ; run;
*/

proc sql; create table &client._total_2017h as select distinct a.*,b.description from
&client._total_2017h as a left join s_p_location_codes as b
on a.geography=b.geography;
quit;


proc sql; create table &client._total_2017_sum as select distinct geography, sp_2017_pmpy_tot,count(distinct memid) as members
from &client._total_2017h 
group by geography
order by geography;
quit;

proc sql; create table &client._total_2017_cnt as select count(distinct memid) as tot_members
from &client._total_2017h ;
quit;


/*merge*/
/*total*/
proc sql; create table &client._2017_total_s as select distinct a.*,b.*
from &client._total_2017_sum a, &client._total_2017_cnt b;
quit;

data &client._2017_total_s2;
set &client._2017_total_s;
pct=members/tot_members;
run;

proc sql; create table &client._2017_total_s3 as select distinct a.*,b.description from
&client._2017_total_s2 as a left join s_p_location_codes as b
on a.geography=b.geography;
quit;

data &client._2017_total_s4;
set &client._2017_total_s3;
adj_pmpy=sp_2017_pmpy_tot*pct;
run;

proc sql; create table &client._2017_pmpy_tot as select sum(adj_pmpy) as weight_pmpy_2017_tot
from  &client._2017_total_s4; quit;

/*inp*/

proc sql; create table &client._2017_inp as select distinct a.*, avg(b.sp_2017_pmpy_inp) as sp_2017_pmpy_inp
from &client._2017_total_s2 as a left join sp_2017_inp as b
on a.geography=b.geo_code
group by geography;
quit;

data &client._2017_inp;
set &client._2017_inp;
adj_pmpy=sp_2017_pmpy_inp*pct;
run;

proc sql; create table  &client._2017_inp_s2 as select distinct a.*,b.description from
&client._2017_inp as a left join s_p_location_codes as b
on a.geography=b.geography;
quit;

proc sql; create table &client._2017_pmpy_inp as select sum(adj_pmpy) as weight_pmpy_2017_inp
from  &client._2017_inp_s2; quit;

/*out*/

proc sql; create table &client._2017_out as select distinct a.*,avg(b.sp_2017_pmpy_out) as sp_2017_pmpy_out
from &client._2017_total_s2 as a left join sp_2017_out as b
on a.geography=b.geo_code
group by geography;
quit;

data &client._2017_out;
set &client._2017_out;
adj_pmpy=sp_2017_pmpy_out*pct;
run;

proc sql; create table  &client._2017_out_s2 as select distinct a.*,b.description from
&client._2017_out as a left join s_p_location_codes as b
on a.geography=b.geography;
quit;

proc sql; create table &client._2017_pmpy_out as select sum(adj_pmpy) as weight_pmpy_2017_out
from  &client._2017_out_s2; quit;


/*prof*/

proc sql; create table &client._2017_pf as select distinct a.*, avg(b.sp_2017_pmpy_pf) as sp_2017_pmpy_pf
from &client._2017_total_s2 as a left join sp_2017_pf as b
on a.geography=b.geo_code
group by geography;
quit;

data &client._2017_pf;
set &client._2017_pf;
adj_pmpy=sp_2017_pmpy_pf*pct;
run;

proc sql; create table  &client._2017_pf_s2 as select distinct a.*,b.description from
&client._2017_pf as a left join s_p_location_codes as b
on a.geography=b.geography;
quit;

proc sql; create table &client._2017_pmpy_pf as select sum(adj_pmpy) as weight_pmpy_2017_pf
from  &client._2017_pf_s2; quit;



/*rx*/

proc sql; create table &client._2017_rx as select distinct a.*,avg(b.sp_2017_pmpy_rx) as sp_2017_pmpy_rx
from &client._2017_total_s2 as a left join sp_2017_rx as b
on a.geography=b.geo_code
group by geography;
quit;

data &client._2017_rx;
set &client._2017_rx;
adj_pmpy=sp_2017_pmpy_rx*pct;
run;

proc sql; create table  &client._2017_rx_s2 as select distinct a.*,b.description from
&client._2017_rx as a left join s_p_location_codes as b
on a.geography=b.geography;
quit;

proc sql; create table &client._2017_pmpy_rx as select sum(adj_pmpy) as weight_pmpy_2017_rx
from  &client._2017_rx_s2; quit;



/*2016 combined*/
data &client._2016_all_pmpy;
merge &client._2016_pmpy_tot
&client._2016_pmpy_inp
&client._2016_pmpy_out
&client._2016_pmpy_pf
&client._2016_pmpy_rx;
run;

data &client._2017_all_pmpy;
merge &client._2017_pmpy_tot
&client._2017_pmpy_inp
&client._2017_pmpy_out
&client._2017_pmpy_pf
&client._2017_pmpy_rx;
run;

data &client._all_all_pmpy;
merge &client._2016_all_pmpy
&client._2017_all_pmpy;
run;

 %let dt =%sysfunc(translate(%sysfunc(datetime(),datetime20.3),--,.:)); ;

%let name=&path.\&client._sp_&y1s..&m1s.-&y2e..&m2e._&dt..csv;
proc export data=&client._all_all_pmpy outfile="&name." dbms=csv replace;run;

%mend;
 
/*
%benchmark(sysparm='20160831 20160930 20161031 20161130 20161231 20170131 20170228 20170331 20170430 20170531 20170630 20170731 20170831 20170930 20171031 20171130 20171231 20180131 20180228 20180331 20180430 20180531 20180630 20180731',client=xtrspace,orgid1=1127631,path=c:\users\ssong\downloads);

%benchmark(sysparm='20140131 20140228 20140331 20140430 20140531 20140630 20140731 20140831 20140930 20141031 20141130 20141231 20150131 20150228 20150331 20150430 20150531 20150630 20150731 20150831 20150930 20151031 20151130 20151231',client=pinnacle, orgid1=203405 ,path=c:\users\ssong\downloads);
 
%benchmark(sysparm='20150131 20150228 20150331 20150430 20150531 20150630 20150731 20150831 20150930 20151031 20151130 20151231 20160131 20160229 20160331 20160430 20160531 20160630 20160731 20170831 20160930 20161031 20161130 20161231',client=pinnacle,orgid1=203405 ,path=c:\users\ssong\downloads);


%benchmark(sysparm='20160131 20160229 20160331 20160430 20160531 20160630 20160731 20170831 20160930 20161031 20161130 20161231 20170131 20170228 20170331 20170430 20170531 20170630 20170731 20170831 20170930 20171031 20171130 20171231',client=pinnacle,orgid1=203405 ,path=c:\users\ssong\downloads);
*/  
/*
%benchmark(sysparm='20160131 20160229 20160331 20160430 20160531 20160630 20160731 20160831 20160930 20161031 20161130 20161231 20170131 20170228 20170331 20170430 20170531 20170630 20170731 20170831 20170930 20171031 20171130 20171231', client=brock,orgid1=1070827,path=U:\SAS\Docs\ROI);

%benchmark(sysparm='20160831 20160930 20161031 20161130 20161231 20170131 20170228 20170331 20170430 20170531 20170630 20170731 20170831 20170930 20171031 20171130 20171231 20180131 20180228 20180331 20180430 20180531 20180630 20180731',client=gordon,orgid1=203022,path=U:\SAS\Docs\ROI);
 
*/
%benchmark(sysparm='20160831 20160930 20161031 20161130 20161231 20170131 20170228 20170331 20170430 20170531 20170630 20170731 20170831 20170930 20171031 20171130 20171231 20180131 20180228 20180331 20180430 20180531 20180630 20180731',client=newmont,orgid1=1080148,path=c:\users\ssong\downloads);
 

%benchmark(sysparm='20160131 20160229 20160331 20160430 20160531 20160630 20160731 20170831 20160930 20161031 20161130 20161231 20170131 20170228 20170331 20170430 20170531 20170630 20170731 20170831 20170930 20171031 20171130 20171231',client=pebtf,orgid1=1081225 1171806 1171730 1171731 1171732 1171733 1171734 1171735 1171736 1171737 1171739 1171740 1171742 1171743 1171744 1171745 1171746 1171747 1171748 1171749 1171750 1171751 1171752 1171753 1171754 1171755 1171804 1171830 1172071
,path=c:\users\ssong\downloads)

%benchmark(sysparm='20160831 20160930 20161031 20161130 20161231 20170131 20170228 20170331 20170430 20170531 20170630 20170731 20170831 20170930 20171031 20171130 20171231 20180131 20180228 20180331 20180430 20180531 20180630 20180731',client=pebtf,orgid1=1081225 1171806 1171730 1171731 1171732 1171733 1171734 1171735 1171736 1171737 1171739 1171740 1171742 1171743 1171744 1171745 1171746 1171747 1171748 1171749 1171750 1171751 1171752 1171753 1171754 1171755 1171804 1171830 1172071
,path=c:\users\ssong\downloads)



%benchmark(sysparm='20160831 20160930 20161031 20161130 20161231 20170131 20170228 20170331 20170430 20170531 20170630 20170731 20170831 20170930 20171031 20171130 20171231 20180131 20180228 20180331 20180430 20180531 20180630 20180731',client=gordon,orgid1=203022 1131734,path=U:\SAS\Docs\ROI);
 
