/*1���ҳ����й�Ʊ2005.6.6����������йɱ��䶯ʱ�䡢�ܹɱ�����ͨ�ɱ�*/
data trd_day_20050606;
set smo.trd_day_2000_2015(keep=stkcd trddt Capchgdt Dsmvosd Dsmvtll Opnprc Clsprc);
where trddt >= '06jun2005'd;  
stksh_tt=Dsmvtll/clsprc;
stksh_intrd=Dsmvosd/clsprc;
run;

proc sort data=trd_day_20050606;
by stkcd trddt;
run;

/*2����stk,capchgdtΪ�������޳��ظ���*/

proc sort data=trd_day_20050606 out=trd_stk_change nodupkey;
by stkcd Capchgdt;
run; 

/*3�����ɱ��䶯�ļ��뽻���ļ��ϲ����۲������̼ۼ���������ܹɱ�����ͨ�ɱ�����ٷ���¼�Ƿ��в��*/
proc sort data=smo.trd_capchg;by stkcd shrchgdt;run;

data trd_capchg_merge;
merge trd_stk_change(in=aa)
smo.trd_capchg(keep=stkcd Shrchgdt Shrtyp  scgtype  Nshrttl Nshrh Nshrb Nshra rename=(shrchgdt=capchgdt) );
by stkcd capchgdt;
if aa;
run;

/*���֤��ȷʵ�������̼ۼ��㣬���Ҫ����1000����Ϊʲô��)������b�ɡ�h�ɵĹ�Ʊ��������Ӧ��a���ܹɱ�*/
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

/*�ѹɱ��䶯ǰ������ļ�¼�ҳ���*/
*1 ����һ�����������յĴ���;
proc sort data=trd_day_20050606;by Stkcd Trddt;run;
data trd_day_20050606_1;
set trd_day_20050606;
by stkcd;
retain trd_no;
if first.stkcd  then  trd_no=1;
else trd_no=trd_no+1;
run;


*2 ȡ�����й�Ȩ�䶯�յļ�¼;
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


*3 ȡ�����й�Ȩ�䶯�յ�ǰ�������¼;

data stockchg_3day;
merge d_chg_all(in=aa)
trd_day_20050606_1;
by stkcd trd_no;
if aa;
run;

*�����������Ȩ�䶯����Ĺ�Ȩ�ͱ䶯�ˣ�ǰһ����δ�䶯ǰ��
������ǿ���Ȩ�䶯����Ŀ��̼��Ƿ��ǰ��䶯Ȩ���������
����ǰһ�յ���ͨ��ֵ =�� ���쿪�̼�*��ͨ�ɱ�;
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

*������������;



