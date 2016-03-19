

/*4、可以复权、不可以复权、断点*/


/*1、断点标在股权变动日，作为起始点，之前一天交易日为终结点；*/
/*2、总市值不变的，不断点；其中股本变动的，进行复权；*/
/*
不变的定义：检查股本变动日和前一交易日，总市值变动《=10%？*/



/*3、总市值变动的，包括股本变动和不变动两类，均断点；*/



*1）规则1：流通股本变动的/未变动的两类，变动的需要断点;


/*一、确定需要复权的股本变动代码*/
*1 先对所有股权变动种类的例子进行分析，每种取一条，看股权变动日当日和前日的情况;
*1 建立一个连续交易日的代码;
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


*2 取出所有股权变动日的记录;
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


*3 取出所有股权变动日及前1天记录;

data stockchg_2day;
merge d_chg_all(in=aa)
trd_day_2000_1;
by stkcd trd_no;
if aa;
run;
*4 再单独看一下07060的情况,没有记录;

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
*5 再单独看一下60005的情况;
data test2;
set  smo.trd_day_2000_2015;
where stkcd='000005' and   trddt >= '01jan2006'd and trddt <= '01Sep2006'd ;
run;

data test3;
set smo.a1_trd_capchg;
where stkcd='000005';
run;
