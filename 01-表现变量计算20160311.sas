/*һ�������������������У���ǰһ�����̼ۡ�ǰһ�����ڣ�*/
/*���������ڵ������У���ǰ���գ�ʵ���Ǻ���գ����ּ۸񡢺��������ڡ�*/
/*1������ʱ�����ĳ��Ʊ���ױ�����������׺����_1*/
data daytrd_000001_2010_2015;
set smo.trd_day_2000_2015;
where Stkcd in ('000001','000002') and trddt >= '01jan2010'd;
/*where Stkcd  = '000002' and trddt >= '01jan2010'd;*/
drop Adjprcnd Adjprcwd Dretnd Dretwd;
run;

data fmt;
set daytrd_000001_2010_2015(obs=1);
run;

proc contents data=fmt out=name(keep=name label) noprint;run;
proc sql noprint;
        select compress(name||'='||name||'_1') into:renames separated by ' '
                from name
                where lowcase(name) ne 'stkcd';
quit;
data daytrd_000001_2010_2015;
        set daytrd_000001_2010_2015(rename=(&renames.));
run;

proc contents data=daytrd_000001_2010_2015 out=name1(keep=name label) noprint;run;

/*2���������У���ǰһ�ձ���������������׺����_0*/
proc sql noprint;
    select compress(name||"_0 ="||"lag("||name||"_1"||");") into:variables0 separated by ' '
    from name where lowcase(name) ne 'stkcd';
	select compress(name||"_2 ="||"lag("||name||"_1"||");"||name||"_3 ="||"lag("||name||"_2"||");") into:variables23 separated by ' '
    from name where lowcase(name) ne 'stkcd';
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
        select cat(TRIM(LEFT(name)),'_0',' ',formats) 
		into:doformats0 separated by ' ' from fmt2;
		select cat(TRIM(LEFT(name)),'_2',' ',formats,' ',TRIM(LEFT(name)),'_3',' ',formats) 
		into:doformats23 separated by ' ' from fmt2;
quit;
/*-----������������ʽ*/

proc sort data=daytrd_000001_2010_2015;by stkcd Trddt_1;run;

data daytrd_000001_2010_2015;
set daytrd_000001_2010_2015;
format &doformats0. ;
&variables0.;
run;


/*3���������У���ǰ���գ�ʵ���Ǻ���գ�����������������׺����_2��_3*/
proc sort data=daytrd_000001_2010_2015;by stkcd descending Trddt_1;run;

data daytrd_000001_2010_2015;
set daytrd_000001_2010_2015;
format &doformats23. ;
&variables23.;
run;

proc sort data=daytrd_000001_2010_2015;by stkcd Trddt_1;run;

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
daytrd_000001_2010_2015 t left join  trd_cale_2010_2015 s
on 	t.Trddt_0=s.Clddt;
create table test1 as select
t.*,s.daywk as daywk_1,s.day as dayno_1 from
test0 t left join  trd_cale_2010_2015 s
on 	t.Trddt_1=s.Clddt;
create table test2 as select
t.*,s.daywk as daywk_2,s.day as dayno_2 from
test1 t left join  trd_cale_2010_2015 s
on 	t.Trddt_2=s.Clddt;
create table daytrd_000001_2010_2015 as select
t.*,s.daywk as daywk_3,s.day as dayno_3 from
test2 t left join  trd_cale_2010_2015 s
on 	t.Trddt_3=s.Clddt;
drop table test0;
drop table test1;
drop table test2;
quit;

proc sort data=daytrd_000001_2010_2015;by stkcd Trddt_1;run;

/*3����ͣ�ƵĴ���*/
/*��������������֮�����Ų�dayno_intvl�����Թ涨�������ż���С��Ų�<=3;��3�������ֻ����һ�죻�粻���������ɾ����ǡ�*/
data  daytrd_000001_2010_2015;
set daytrd_000001_2010_2015;
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
		from daytrd_000001_2010_2015 t left join smo.A1_trd_capchg s
		on t.stkcd=s.stkcd and t.Trddt_1=s.Shrchgdt
    ;
	create table test2 as
        select t.*, s.Shrtyp as Shrtyp_2
		from test1 t left join smo.A1_trd_capchg s
		on t.stkcd=s.stkcd and t.Trddt_2=s.Shrchgdt
    ;
	create table daytrd_000001_2010_2015 as
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
		from daytrd_000001_2010_2015 t left join smo.share_change_type s
		on t.Shrtyp_1=s.Shrtyp
    ;
    create table test2 as
        select t.*, s.reweight as reweight_2
		from test1 t left join smo.share_change_type s
		on t.Shrtyp_2=s.Shrtyp
    ;
    create table daytrd_000001_2010_2015 as
        select t.*, s.reweight as reweight_3
		from test2 t left join smo.share_change_type s
		on t.Shrtyp_3=s.Shrtyp
    ;
drop table test1;
drop table test2;
quit;

proc sort data=daytrd_000001_2010_2015;by stkcd Trddt_1;run;

/*5���������ų���ʶ*/
/*1�������ų���ÿ��֤ȯ��trddt_1Ϊ��һ�������պ��������������*/
/*2��ͣ���ų����м����ͣ��1�죬dayno_intvl>=3*/
/*3����Ȩ�ų�����2��3��������Ϊ��Ҫ��Ȩ�ģ���reweight_2,3Ϊ1��2*/
data daytrd_000001_2010_2015;
set daytrd_000001_2010_2015;
if dayno_1 = 1 or dayno_1=1304 or dayno_1=1305 then perf_ex=1;
else if dayno_intvl> 3 then perf_ex=2;
else if reweight_2 in(1,2) or reweight_2 in(1,2) then perf_ex=3;
else perf_ex=0;
run;

proc freq data=daytrd_000001_2010_2015;
tables perf_ex/missing;
run;

data test;
set daytrd_000001_2010_2015(keep=Stkcd Trddt_1 Trddt_2 Trddt_3 dayno_1 dayno_2 dayno_3  perf_ex);
where perf_ex=2;
run;



