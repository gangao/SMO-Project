

/*4�����Ը�Ȩ�������Ը�Ȩ���ϵ�*/


/*1���ϵ���ڹ�Ȩ�䶯�գ���Ϊ��ʼ�㣬֮ǰһ�콻����Ϊ�ս�㣻*/
/*2������ֵ����ģ����ϵ㣻���йɱ��䶯�ģ����и�Ȩ��*/
/*
����Ķ��壺���ɱ��䶯�պ�ǰһ�����գ�����ֵ�䶯��=10%��*/



/*3������ֵ�䶯�ģ������ɱ��䶯�Ͳ��䶯���࣬���ϵ㣻*/



*1������1����ͨ�ɱ��䶯��/δ�䶯�����࣬�䶯����Ҫ�ϵ�;


/*һ��ȷ����Ҫ��Ȩ�Ĺɱ��䶯����*/
*1 �ȶ����й�Ȩ�䶯��������ӽ��з�����ÿ��ȡһ��������Ȩ�䶯�յ��պ�ǰ�յ����;
*1 ����һ�����������յĴ���;
data trd_day_2000;
set smo.trd_day_2000_2015(keep=stkcd trddt Capchgdt Dsmvosd Dsmvtll Opnprc Clsprc);
where trddt >= '01jan2000'd;  
stksh_tt=Dsmvtll/clsprc;
stksh_intrd=Dsmvosd/clsprc;
run;

proc sort data=trd_day_2000;
by stkcd trddt;
run;

proc sort data=trd_day_2000;by Stkcd Trddt;run;
data trd_day_2000_1;
set trd_day_2000;
by stkcd;
retain trd_no;
if first.stkcd  then  trd_no=1;
else trd_no=trd_no+1;
run;


proc sort data=smo.a1_trd_capchg;by  scgtype;run;

proc sort data= smo.a1_trd_capchg;by  Shrchgdt;run;


*2 ȡ�����й�Ȩ�䶯�յļ�¼;
proc sql;
create table d_chg as 
select t.stkcd, t.trddt, t.trd_no, t.Capchgdt, s.scgtype, s.Shrtyp from 
trd_day_2000_1 t, smo.a1_trd_capchg s 
where t.trddt=s.Shrchgdt and t.trddt=t.Capchgdt and t.stkcd=s.stkcd;
quit;

proc sort data=d_chg  nodupkey;by scgtype ;run;

data d_chg_bf;
set d_chg;
trd_no=trd_no-1;
run;

data d_chg_all(keep=stkcd trd_no scgtype Shrtyp);
set d_chg
d_chg_bf ;
run;

proc sort data=d_chg_all nodupkey;by stkcd trd_no;
run;


*3 ȡ�����й�Ȩ�䶯�ռ�ǰ1���¼;

data stockchg_2day;
merge d_chg_all(in=aa)
trd_day_2000_1;
by stkcd trd_no;
if aa;
run;
*4 �ٵ�����һ��07060�����,û�м�¼;

data test;
set smo.a1_trd_capchg;
where scgtype='07060';
run;

proc sql;
create table tmp1 as 
select t.*, s.* from 
test t, smo.trd_day_2000_2015 s
where t.stkcd=s.stkcd and t.Shrchgdt=s.trddt;
quit;
*5 �ٵ�����һ��60005�����;
data test2;
set  smo.trd_day_2000_2015;
where stkcd='000005' and   trddt >= '01jan2006'd and trddt <= '01Sep2006'd ;
run;

data test3;
set smo.a1_trd_capchg;
where stkcd='000005';
run;
