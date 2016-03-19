options mprint merror symbolgen mlogic compress=yes;

/*һ�������������������У���ǰһ�����̼ۡ�ǰһ�����ڣ�*/
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
/*��������ʽ-----*/
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

/*3����һ��ƽ���������*/
data test1;
set test1;
array day_amplitude(50) day_amplitude_1-day_amplitude_50;
;
do i=1 to 50;
day_amplitude(&i.)=(p_Hiprc(&i.)-p_loprc(&i.))/p_Clsprc(&i.+1);
end;
run;


/*2��������ʽ*/
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

/*��������ʽ-----*/
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
/*-----������������ʽ*/

proc sort data=daytrd_all_2010_2015;by stkcd Trddt_1;run;

data daytrd_all_2010_2015;
set daytrd_all_2010_2015;
format &doformats0. ;
&variables0.;
run;


/*3���������У���ǰ���գ�ʵ���Ǻ���գ�����������������׺����_2��_3*/
proc sort data=daytrd_all_2010_2015;by stkcd descending Trddt_1;run;

data daytrd_all_2010_2015;
set daytrd_all_2010_2015;
format &doformats23. ;
&variables23.;
run;

proc sort data=daytrd_all_2010_2015;by stkcd Trddt_1;run;

/*�����������������ڱ�������������һ������ţ���1��1305��*/
/*���ս��ױ��е�ÿһ�����ڶ�Ӧ����ţ�*/
data trd_cale_2010_2015;
set smo.trd_cale;
where clddt >= '01jan2010'd and State='O';
run;

proc sort data=	trd_cale_2010_2015 nodupkey;by clddt;run;

data trd_cale_2010_2015;
set trd_cale_2010_2015;
day +1;
run;
/*�ϲ�*/
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

/*3����ͣ�ƵĴ���*/
/*��������������֮�����Ų�dayno_intvl�����Թ涨�������ż���С��Ų�<=3;��3�������ֻ����һ�죻�粻���������ɾ����ǡ�*/
data  daytrd_all_2010_2015;
set daytrd_all_2010_2015;
dayno_intvl=dayno_3-dayno_1;
run;

/*4���Թɱ��䶯�յĴ��� �����Ը�Ȩ�Ĵ���*/
/*��day0-day3����4���ɱ��䶯����������ǹɱ��䶯�գ�������룬�粻����ȱʧ��*/
/*Ȼ����ݸ�Ȩ������4���Ƿ�Ȩ�������*/
/*Ȼ���жϣ�day2��3�Ƿ���Ҫ��Ȩ������Ҫ�������ɾ����ǡ�*/
/*4.1�����ɱ��䶯�ֵ��*/
data smo.share_change_type;
/*infile datalines dsd delimiter="";*/
length 	Shrtyp $5. Shrtypc $40. reweight 3;
label Shrtyp='�ɱ��䶯����' Shrtypc='����' reweight='�Ƿ���Ҫ��Ȩ';
input  Shrtyp Shrtypc reweight ;
datalines;
10000	�¹��״�����	2
30000	ת�������	2
07060	��Ȩ���øĸ�3	2
01000	�͹�/ת���ɱ�	1
00100	��ɳ�Ȩ	1
00010	��ϸ��Ȩ(������ϸ������)	1
03000	��/ת����������	1
41000	�ʱ�������ת���ɱ�1	1
47000	�ʱ�������ת���ɱ�2	1
07000	��Ȩ����	1
07700	��Ȩ���øĸ�1	1
77000	��Ȩ���øĸ�2	1
60005	60005	0
20000	ְ��������	0
00002	�����¹�����	0
40000	ծת������	0
50000	�����������У����ۣ�	0
05000	���˹��������У����ۣ�	0
00500	ս���������У����ۣ�	0
00050	�����������У����ۣ�	0
00005	�߹ܹɷݱ䶯	0
60000	��Ȩת��	0
06000	�ɷݻع�	0
00600	���պϲ�	0
00060	�ǹ�������	0
70000	���ʷ��˹�����	0
00700	�ɵ�ծ	0
00070	��Ȩ����	0
00008	����	0
00009	��������������	0
90000	Ȩ֤��Ȩ	0
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

/*4.2 ��Trddt_1,2,3����Capchgdt_1,2,3������Shrtyp_1,2,3��reweight_1,2,3*/
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

/*5���������ų���ʶ*/
/*1�������ų���1-ÿ��֤ȯ��trddt_1Ϊ��һ�������պ��������������*/
/*2��ͣ���ų���2-�м����ͣ��1�죬dayno_intvl>=3��4-��۲���ֻ��1��*/
/*3����Ȩ�ų���3-1��2��3��������Ϊ��Ҫ��Ȩ�ģ���reweight_1��2,3Ϊ1��2*/
data smo.daytrd_all_2010_2015;
set daytrd_all_2010_2015;
if dayno_1 = fst_trday or dayno_1=lst_trday or dayno_2=lst_trday then perf_ex=1;
else if dayno_intvl> 3 then perf_ex=2;
else if reweight_1 in(1,2) or reweight_2 in(1,2) or reweight_3 in(1,2) then perf_ex=3;
else if dayno_1-dayno_0 >1 then perf_ex=4;
else perf_ex=0;
run;


/*6����3����ߡ�3�����*/
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
