
proc format;
value percentf(notsorted)
low -< -0.28 = '01-下跌28%以上'
-0.28 -< -0.25 =  '02-下跌25%到28%'
-0.25 -< -0.20 =  '03-下跌20%到25%'
-0.20 -< -0.15 =  '04-下跌15%到20%'
-0.15 -< -0.10 =  '05-下跌10%到15%'
-0.10 -< -0.08 =  '06-下跌8%到10%'
-0.08 -< -0.06 =  '07-下跌6%到8%'
-0.06 -< -0.04 =  '08-下跌4%到6%'
-0.04 -< -0.02 =  '09-下跌2%到4%'
-0.02 -< -0.00 =  '10-下跌0%到2%'
0.00 -< 0.02 =  '11-上升0%到2%'
0.02 -< 0.04 =  '12-上升2%到4%'
0.04 -< 0.06 =  '13-上升4%到6%'
0.06 -< 0.08 =  '14-上升6%到8%'
0.08 -< 0.10 =  '15-上升8%到10%'
0.10 -< 0.15 =  '16-上升10%到15%'
0.15 -< 0.20 =  '17-上升15%到20%'
0.20 -< 0.25 =  '18-上升20%到25%'
0.25 -< 0.30 =  '19-上升25%到30%'
0.30 -< 0.34 =  '20-上升30%到34%'
0.34 - high =  '21-上升34%以上'
;
run;
data test;
set smo.daytrd_all_2010_2015;
where Markettype_1 in (1,4,16) and perf_ex = 0;
run;

data test1;
format 	hi_chg_pct lo_chg_pct percentf.;
set smo.daytrd_all_2010_2015(keep=stkcd trddt_1 hi_chg_pct lo_chg_pct Markettype_1 perf_ex);
where Markettype_1 in (1,4,16) and perf_ex = 0;
run;

data test2;
set test1;
where lo_chg_pct >= -0.02;
run;

data test3;
set test1;
where lo_chg_pct < -0.02;
run;


proc sort data=test1;by hi_chg_pct;run;
proc sort data=test2;by hi_chg_pct;run;
proc sort data=test3;by hi_chg_pct;run;

proc freq data=test1;
tables lo_chg_pct hi_chg_pct/missing;
run;
proc freq data=test2;
tables lo_chg_pct hi_chg_pct/missing;
run;
proc freq data=test3;
tables lo_chg_pct hi_chg_pct/missing;
run;


data test_gt333;
set test;
where hi_chg_pct>0.333;
run;

data test_lt25;
set test;
where lo_chg_pct < -0.25;
run;
/**/
data tmp;
set smo.a1_trd_capchg;
/*where stkcd='000540';*/
run;

proc sort data=tmp;by descending Shrchgdt;run;

data test111;
set smo.A1_trd_capchg;
where stkcd='000738';
run;


/*data test11;*/
/*set smo.daytrd_all_2010_2015(keep=stkcd trddt_1 dayno_1 obs=30000);*/
/*run;*/

data test22;
set  test11;
/*if first.stkcd then perf_ex=1; else perf_ex=0;*/
/*if last.stkcd then perf_ex=1; else perf_ex=0;*/
first=first.stkcd;
if first.stkcd then perf_ex=1; else perf_ex=0;
if last.stkcd then  call symput('obs',_n_) ;

by stkcd;

run;

proc sql;
create table test33 as
select t.*,max(t.dayno_1) as sum_trday from
test22  t
group by stkcd;
run;
/**/
/*data test33;*/
/*set test22;*/
/*if last.stkcd then call symput('lstno',dayno_1);*/
/*last_dayno= &lstno.;*/
/*by stkcd;*/
/*run;*/
/**/
/*proc sort data=test33;by stkcd trddt_1;run;*/
/**/
/*data test33;*/
/*set test33;*/
/*if _n_=sum_trday then perf_ex1=1;*/
/*by stkcd;*/
/*run;*/

/**/
*market type:1= 上海A，2= 上海B，4= 深圳A，8= 深圳B, 16=创业板;
data freq_perf;
set  smo.daytrd_all_2010_2015;
run;
proc sort data=	freq_perf out=freq_perf_nodup nodupkey;by stkcd;
run;
options compress=yes;
proc sort data=freq_perf;by Markettype_1 stkcd;run;
proc freq data=freq_perf noprint;
tables stkcd/missing out=stckcd_trddy_no;
by Markettype_1;
/*output out=test;*/
run;

proc means data=stckcd_trddy_no;
class Markettype_1;
var COUNT;
run;

proc freq data=freq_perf;
tables Markettype_1/missing;
run;

proc freq data=freq_perf_nodup;
tables Markettype_1/missing;
run;

/*proc means data=freq_perf;*/
/*by Markettype_1;*/
/*freq  stkcd;*/
/*/*var stkcd;*/*/
/*run;*/


