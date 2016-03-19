options mprint merror symbolgen mlogic compress=yes;

/*一、按交易日期正序排列，做前一日收盘价、前一日日期；*/
data test;
set smo.trd_day_2000_2015;
where trddt >= '01jan2010'd and stkcd in ('000001','000002');
drop Adjprcnd Adjprcwd Dretnd Dretwd;
run;

proc sort data=test;by stkcd trddt;run;

proc contents data=test out=name(keep=name label) noprint;run;
proc sql noprint;
	select cat('array ',' ','p_',TRIM(LEFT(name)),'(50)',' ','p_',TRIM(LEFT(name)),'_1-p_',TRIM(LEFT(name)),'_50;') 
		into:doarray separated by ' ' from name where lowcase(name) ne 'stkcd';
	select cat('p_',TRIM(LEFT(name)),'(1)=',TRIM(LEFT(name)),';') 
		into:firstvar separated by ' ' from name where lowcase(name) ne 'stkcd';
	select cat('p_',TRIM(LEFT(name)),'(&i.)=lag(p_',TRIM(LEFT(name)),'(%eval(','&i.-1)));') 
		into:lagvar separated by ' ' from name where lowcase(name) ne 'stkcd';
	select cats("call symput('f_",name,"',vformat(",name,"));") 
		into:formats separated by ' ' from name where lowcase(name) ne 'stkcd';
	select cats(name,'=','"','&','f_',name,'.";') 
		into:macrofmt separated by ' ' from name where lowcase(name) ne 'stkcd';
quit;
/*做变量格式-----*/
data _null_;
set test(obs=1);
&formats.;
run;

data fmt1;
&macrofmt.;
run;

proc transpose data=fmt1 out=fmt2(rename=(_NAME_=name COL1=formats));
var &names.;
run;

proc sql noprint;
	select cat("format"," ","p_",TRIM(LEFT(name)),"_&i."," ",formats,";") 
		into:doformats separated by " " from fmt2;
quit;

%macro lagvar50;
data test1;
set test;
&doarray.;
%do i = 1 %to 50; &doformats.;%end;
&firstvar.;
%do i=2 %to 50;
&lagvar.;
%end;
run;
%mend;
%lagvar50;

/*3、做一个平均振幅变量*/
data test1;
set test1;
array day_amplitude(50) day_amplitude_1-day_amplitude_50;
;
do i=1 to 50;
day_amplitude(&i.)=(p_Hiprc(&i.)-p_loprc(&i.))/p_Clsprc(&i.+1);
end;
run;


/*2、变量格式*/
data fmt;
set test(obs=1);
run;

proc contents data=fmt out=name(keep=name label) noprint;run;

proc sql noprint;
 	select cats("call symput('f_",name,"',vformat(",name,"));") into:formats
	separated by ' ' from name where lowcase(name) ne 'stkcd';
	select cats(name,'=','"','&','f_',name,'.";') into:macrofmt
	separated by ' ' from name where lowcase(name) ne 'stkcd';
	select cats(name) into:names
	separated by ' ' from name where lowcase(name) ne 'stkcd';
quit;

/*做变量格式-----*/
data fmt;
set fmt;
&formats.;
run;

data fmt1;
&macrofmt.;
run;

proc transpose data=fmt1 out=fmt2(rename=(_NAME_=name COL1=formats));
var &names.;
run;

proc sql noprint;
        select cat('p_',TRIM(LEFT(name)),'(&i.)',' ',formats) 
		into:doformats separated by ' ' from fmt2;
quit;
%put &doformats.;
/*-----以上做变量格式*/

proc sort data=daytrd_all_2010_2015;by stkcd Trddt_1;run;

data daytrd_all_2010_2015;
set daytrd_all_2010_2015;
format &doformats0. ;
&variables0.;
run;


/*3、倒叙排列，做前二日（实际是后二日）变量，变量命名后缀加上_2，_3*/
proc sort data=daytrd_all_2010_2015;by stkcd descending Trddt_1;run;

data daytrd_all_2010_2015;
set daytrd_all_2010_2015;
format &doformats23. ;
&variables23.;
run;

proc sort data=daytrd_all_2010_2015;by stkcd Trddt_1;run;

/*二、做连续交易日期表，连续交易日做一个排序号，从1到1305；*/
/*将日交易表中的每一个日期对应好序号；*/
data trd_cale_2010_2015;
set smo.trd_cale;
where clddt >= '01jan2010'd and State='O';
run;

proc sort data=	trd_cale_2010_2015 nodupkey;by clddt;run;

data trd_cale_2010_2015;
set trd_cale_2010_2015;
day +1;
run;
/*合并*/
proc sql;
create table test0 as select
t.*,s.daywk as daywk_0,s.day as dayno_0 from
daytrd_all_2010_2015 t left join  trd_cale_2010_2015 s
on 	t.Trddt_0=s.Clddt;
create table test1 as select
t.*,s.daywk as daywk_1,s.day as dayno_1 from
test0 t left join  trd_cale_2010_2015 s
on 	t.Trddt_1=s.Clddt;
create table test2 as select
t.*,s.daywk as daywk_2,s.day as dayno_2 from
test1 t left join  trd_cale_2010_2015 s
on 	t.Trddt_2=s.Clddt;
create table daytrd_all_2010_2015 as select
t.*,s.daywk as daywk_3,s.day as dayno_3 from
test2 t left join  trd_cale_2010_2015 s
on 	t.Trddt_3=s.Clddt;
drop table test0;
drop table test1;
drop table test2;
quit;

proc sort data=daytrd_all_2010_2015;by stkcd Trddt_1;run;

/*3、对停牌的处理*/
/*计算三个交易日之间的序号差dayno_intvl。可以规定，最大序号减最小序号差<=3;即3天中最多只隔着一天；如不满足则打上删除标记。*/
data  daytrd_all_2010_2015;
set daytrd_all_2010_2015;
dayno_intvl=dayno_3-dayno_1;
run;

/*4、对股本变动日的处理， 包括对复权的处理*/
/*从day0-day3，做4个股本变动日数据项，如是股本变动日，则给代码，如不是则缺失。*/
/*然后根据复权表，再做4个是否复权的数据项。*/
/*然后判断，day2、3是否需要复权，如需要，则打上删除标记。*/
/*4.1、做股本变动字典表*/
data smo.share_change_type;
/*infile datalines dsd delimiter="";*/
length 	Shrtyp $5. Shrtypc $40. reweight 3;
label Shrtyp='股本变动类型' Shrtypc='解释' reweight='是否需要复权';
input  Shrtyp Shrtypc reweight ;
datalines;
10000	新股首次上市	2
30000	转配股上市	2
07060	股权分置改革3	2
01000	送股/转赠股本	1
00100	配股除权	1
00010	拆细除权(包括拆细和缩股)	1
03000	送/转股立即上市	1
41000	资本公积金转增股本1	1
47000	资本公积金转增股本2	1
07000	股权分置	1
07700	股权分置改革1	1
77000	股权分置改革2	1
60005	60005	0
20000	职工股上市	0
00002	增发新股上市	0
40000	债转股上市	0
50000	基金配售上市（限售）	0
05000	法人股配售上市（限售）	0
00500	战略配售上市（限售）	0
00050	超额配售上市（限售）	0
00005	高管股份变动	0
60000	股权转让	0
06000	股份回购	0
00600	吸收合并	0
00060	非公开增发	0
70000	外资法人股上市	0
00700	股抵债	0
00070	股权激励	0
00008	其他	0
00009	有限售条件上市	0
90000	权证行权	0
00062	00062	0
00065	00065	0
00068	00068	0
00069	00069	0
00660	00660	0
01700	01700	0
05005	05005	0
05009	05009	0
06005	06005	0
25000	25000	0
40005	40005	0
40008	40008	0
46000	46000	0
60002	60002	0
60008	60008	0
60009	60009	0
90009	90009	0
;
run;

/*4.2 如Trddt_1,2,3等于Capchgdt_1,2,3则制作Shrtyp_1,2,3和reweight_1,2,3*/
proc sql noprint;
    create table test1 as
        select t.*, s.Shrtyp as Shrtyp_1
		from daytrd_all_2010_2015 t left join smo.A1_trd_capchg s
		on t.stkcd=s.stkcd and t.Trddt_1=s.Shrchgdt
    ;
	create table test2 as
        select t.*, s.Shrtyp as Shrtyp_2
		from test1 t left join smo.A1_trd_capchg s
		on t.stkcd=s.stkcd and t.Trddt_2=s.Shrchgdt
    ;
	create table daytrd_all_2010_2015 as
        select t.*, s.Shrtyp as Shrtyp_3
		from test2 t left join smo.A1_trd_capchg s
		on t.stkcd=s.stkcd and t.Trddt_3=s.Shrchgdt
    ;
drop table test1;
drop table test2;
quit;

proc sql noprint;
    create table test1 as
        select t.*, s.reweight as reweight_1
		from daytrd_all_2010_2015 t left join smo.share_change_type s
		on t.Shrtyp_1=s.Shrtyp
    ;
    create table test2 as
        select t.*, s.reweight as reweight_2
		from test1 t left join smo.share_change_type s
		on t.Shrtyp_2=s.Shrtyp
    ;
    create table daytrd_all_2010_2015 as
        select t.*, s.reweight as reweight_3
		from test2 t left join smo.share_change_type s
		on t.Shrtyp_3=s.Shrtyp
    ;
drop table test1;
drop table test2;
quit;

proc sort data=daytrd_all_2010_2015;by stkcd Trddt_1;run;

proc sql;
create table daytrd_all_2010_2015 as
select t.*,min(t.dayno_1) as fst_trday,max(t.dayno_1) as lst_trday from
daytrd_all_2010_2015  t
group by stkcd;
run;

/*5、做表现排除标识*/
/*1）数据排除：1-每个证券的trddt_1为第一个交易日和最后两个交易日*/
/*2）停牌排除：2-中间仅可停牌1天，dayno_intvl>=3；4-与观察日只隔1天*/
/*3）复权排除：3-1、2、3个交易日为需要复权的，即reweight_1，2,3为1或2*/
data smo.daytrd_all_2010_2015;
set daytrd_all_2010_2015;
if dayno_1 = fst_trday or dayno_1=lst_trday or dayno_2=lst_trday then perf_ex=1;
else if dayno_intvl> 3 then perf_ex=2;
else if reweight_1 in(1,2) or reweight_2 in(1,2) or reweight_3 in(1,2) then perf_ex=3;
else if dayno_1-dayno_0 >1 then perf_ex=4;
else perf_ex=0;
run;


/*6、做3日最高、3日最低*/
data smo.daytrd_all_2010_2015;
set smo.daytrd_all_2010_2015;
max_prc=max(Hiprc_1,Hiprc_2,Hiprc_3);
min_prc=min(Loprc_1,Loprc_2,Loprc_3);
hi_chg_pct=max_prc/Clsprc_0-1;
lo_chg_pct=min_prc/Clsprc_0-1;
run;







/**/
/*proc freq data=smo.daytrd_all_2010_2015;*/
/*tables perf_ex/missing;*/
/*run;*/
/*proc freq data=smo.daytrd_all_2010_2015;*/
/*tables dayno_intvl/missing;*/
/*run;*/
/*proc freq data=smo.daytrd_all_2010_2015;*/
/*tables lst_trday/missing;*/
/*run;*/
/**/
/*data test;*/
/*set  smo.daytrd_all_2010_2015(keep=stkcd perf_ex);*/
/*where  perf_ex=1;*/
/*run;*/
/*proc sort data=test nodupkey;by stkcd;run;*/
/**/
/*proc sql;*/
/*create table test1 as*/
/*select t.* from  smo.daytrd_all_2010_2015 t*/
/*group by  stkcd*/
/*having sum(case when perf_ex=1 then 1 else . end) <3;*/
/*quit;*/
/**/
/*proc sort data=test1;by stkcd trddt_1;run;*/
/**/
/**/
