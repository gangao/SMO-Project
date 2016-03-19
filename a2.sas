/*1、找出所有股票2005.6.6日至今的所有股本变动时间、总股本、流通股本*/
data trd_day_20050606;
set smo.trd_day_2000_2015(keep=stkcd trddt Capchgdt Dsmvosd Dsmvtll Opnprc Clsprc);
where trddt >= '06jun2005'd;  
stksh_tt=Dsmvtll/clsprc;
stksh_intrd=Dsmvosd/clsprc;
run;

proc sort data=trd_day_20050606;
by stkcd trddt;
run;

/*2、以stk,capchgdt为条件，剔除重复项*/

proc sort data=trd_day_20050606 out=trd_stk_change nodupkey;
by stkcd Capchgdt;
run; 

/*3、将股本变动文件与交易文件合并，观察以收盘价计算出来的总股本和流通股本，与官方记录是否有差别*/
proc sort data=smo.trd_capchg;by stkcd shrchgdt;run;

data trd_capchg_merge;
merge trd_stk_change(in=aa)
smo.trd_capchg(keep=stkcd Shrchgdt Shrtyp  scgtype  Nshrttl Nshrh Nshrb Nshra rename=(shrchgdt=capchgdt) );
by stkcd capchgdt;
if aa;
run;

/*结果证明确实是以收盘价计算，结果要乘上1000倍（为什么？)。但有b股、h股的股票则计算出的应是a股总股本*/
data null_merge;
set trd_capchg_merge; 
where Shrtyp='';
run;

proc sort data=null_merge;by Capchgdt;run;

data trd_capchg_merge;
set trd_capchg_merge;
tt_s=round(stksh_tt*1000,1);
td_s=round(stksh_intrd*1000,1);
tta_s=Nshrttl-Nshrh-Nshrb;
tt_minus=tt_s-tta_s;
td_minus=td_s-Nshra;
run;

data un_equal;
set trd_capchg_merge;
where tt_minus>10 or td_minus>10;
run;

/*把股本变动前后两天的记录找出来*/
*1 建立一个连续交易日的代码;
proc sort data=trd_day_20050606;by Stkcd Trddt;run;
data trd_day_20050606_1;
set trd_day_20050606;
by stkcd;
retain trd_no;
if first.stkcd  then  trd_no=1;
else trd_no=trd_no+1;
run;


*2 取出所有股权变动日的记录;
data d_chg;
set trd_day_20050606_1(keep=stkcd Trddt Capchgdt trd_no);
where Trddt=Capchgdt;
run;

data d_chg_bf;
set d_chg;
trd_no=trd_no-1;
run;

data d_chg_af;
set d_chg;
trd_no=trd_no+1;
run;

data d_chg_all(keep=stkcd trd_no);
set d_chg
d_chg_bf
d_chg_af;
run;

proc sort data=d_chg_all nodupkey;by stkcd trd_no;
run;


*3 取出所有股权变动日的前后三天记录;

data stockchg_3day;
merge d_chg_all(in=aa)
trd_day_20050606_1;
by stkcd trd_no;
if aa;
run;

*结果表明，股权变动当天的股权就变动了，前一天是未变动前的
下面就是看股权变动当天的开盘价是否是按变动权重算出来的
即：前一日的流通市值 =？ 当天开盘价*流通股本;
data d_chg_2d(keep=stkcd trd_no);
set d_chg
d_chg_bf;
run;

proc sort data=d_chg_2d nodupkey;by stkcd trd_no;
run;

data stockchg_2day;
merge d_chg_2d(in=aa)
trd_day_20050606_1;
by stkcd trd_no;
if aa;
run;

data stockchg_2day;
set stockchg_2day;
trd_ss=lag(Dsmvosd);
design_open=Opnprc*stksh_intrd;
run;

data stockchg_1day;
set stockchg_2day;
where trddt=Capchgdt;
chg_pct=(design_open-trd_ss)/trd_ss;
run;

proc univariate data=stockchg_1day;
var chg_pct;
histogram /endpoints=-2 to 10 by 0.11;
run;

*看来不是这样;



