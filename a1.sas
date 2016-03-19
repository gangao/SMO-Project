data smo.trd_capchg;
set smo.trd_capchg;
format status $8.;
label status="所处周期";
if Shrchgdt <= '16oct2007'd then status = '上升期';
else if  Shrchgdt <= '28oct2008'd then status = '下降期';
else if  Shrchgdt <= '4aug2009'd then status = '上升期';
else if  Shrchgdt <= '20jun2014'd then status = '平稳期';
else status = '上升期';
/**/
format scgtype $30.;
label scgtype="股权变动类型";
if Shrtyp="10000" then do;scgtype="新股首次上市";end;
if Shrtyp="01000" then do;scgtype="送股/转赠股本";end;
if Shrtyp="00100" then do;scgtype="配股除权";end;
if Shrtyp="00010" then do;scgtype="拆细除权(包括拆细和缩股)";end;
if Shrtyp="20000" then do;scgtype="职工股上市";end;
if Shrtyp="00002" then do;scgtype="增发新股上市";end;
if Shrtyp="30000" then do;scgtype="转配股上市";end;
if Shrtyp="03000" then do;scgtype="送/转股立即上市";end;
if Shrtyp="40000" then do;scgtype="债转股上市";end;
if Shrtyp="50000" then do;scgtype="基金配售上市（限售）";end;
if Shrtyp="05000" then do;scgtype="法人股配售上市（限售）";end;
if Shrtyp="00500" then do;scgtype="战略配售上市（限售）";end;
if Shrtyp="00050" then do;scgtype="超额配售上市（限售）";end;
if Shrtyp="00005" then do;scgtype="高管股份变动";end;
if Shrtyp="60000" then do;scgtype="股权转让";end;
if Shrtyp="06000" then do;scgtype="股份回购";end;
if Shrtyp="00600" then do;scgtype="吸收合并";end;
if Shrtyp="00060" then do;scgtype="非公开增发";end;
if Shrtyp="70000" then do;scgtype="外资法人股上市";end;
if Shrtyp="00700" then do;scgtype="股抵债";end;
if Shrtyp="07000" then do;scgtype="股权分置";end;
if Shrtyp="00070" then do;scgtype="股权激励";end;
if Shrtyp="00008" then do;scgtype="其他";end;
if Shrtyp="00009" then do;scgtype="有限售条件上市";end;
if Shrtyp="90000" then do;scgtype="权证行权";end;
if scgtype="" then do;scgtype=Shrtyp;end;
run;


proc sort data=smo.trd_capchg;by Shrchgdt;run;

data smo.a1_trd_capchg;
set smo.trd_capchg;
where Shrchgdt >= '6jun2005'd;
run;

data smo.a1_trd_capchg;
set smo.a1_trd_capchg;
format status $8.;
label status="所处周期";
if Shrchgdt <= '16oct2007'd then status = '上升期';
else if  Shrchgdt <= '28oct2008'd then status = '下降期';
else if  Shrchgdt <= '4aug2009'd then status = '上升期';
else if  Shrchgdt <= '20jun2014'd then status = '平稳期';
else status = '上升期';
/**/
format scgtype $30.;
label scgtype="股权变动类型";
if Shrtyp="10000" then do;scgtype="新股首次上市";end;
if Shrtyp="01000" then do;scgtype="送股/转赠股本";end;
if Shrtyp="00100" then do;scgtype="配股除权";end;
if Shrtyp="00010" then do;scgtype="拆细除权(包括拆细和缩股)";end;
if Shrtyp="20000" then do;scgtype="职工股上市";end;
if Shrtyp="00002" then do;scgtype="增发新股上市";end;
if Shrtyp="30000" then do;scgtype="转配股上市";end;
if Shrtyp="03000" then do;scgtype="送/转股立即上市";end;
if Shrtyp="40000" then do;scgtype="债转股上市";end;
if Shrtyp="50000" then do;scgtype="基金配售上市（限售）";end;
if Shrtyp="05000" then do;scgtype="法人股配售上市（限售）";end;
if Shrtyp="00500" then do;scgtype="战略配售上市（限售）";end;
if Shrtyp="00050" then do;scgtype="超额配售上市（限售）";end;
if Shrtyp="00005" then do;scgtype="高管股份变动";end;
if Shrtyp="60000" then do;scgtype="股权转让";end;
if Shrtyp="06000" then do;scgtype="股份回购";end;
if Shrtyp="00600" then do;scgtype="吸收合并";end;
if Shrtyp="00060" then do;scgtype="非公开增发";end;
if Shrtyp="70000" then do;scgtype="外资法人股上市";end;
if Shrtyp="00700" then do;scgtype="股抵债";end;
if Shrtyp="07000" then do;scgtype="股权分置";end;
if Shrtyp="00070" then do;scgtype="股权激励";end;
if Shrtyp="00008" then do;scgtype="其他";end;
if Shrtyp="00009" then do;scgtype="有限售条件上市";end;
if Shrtyp="90000" then do;scgtype="权证行权";end;
if scgtype="" then do;scgtype=Shrtyp;end;
run;


proc freq data=smo.a1_trd_capchg;
tables Shrtyp/missing out=aaa OUTCUM;
run;

proc freq data=smo.a1_trd_capchg;
tables scgtype/missing out=bbb OUTCUM;
run;


proc sort data=smo.a1_trd_capchg;by Shrtyp Stkcd Shrchgdt;run;

/*新股首次上市 是否是该股票第一条股权变动记录*/
*1、找所有有新股首次上市变动的股票，取其所有变动记录，然后取第一条，看是否是新股首次上市变动;
data test;
set smo.a1_trd_capchg(keep=Stkcd Shrchgdt Shrtyp scgtype);
where scgtype="新股首次上市";
run;

proc sql;
create table test2 as
select t.* from smo.a1_trd_capchg t, test e
where t.stkcd=e.stkcd;
quit;
run;

proc sort data=test2;by Stkcd Shrchgdt;run;

data test3;
set test2;
by Stkcd Shrchgdt;
if first.Stkcd;
run;

proc freq data=test3;
tables scgtype/missing;
run;
*2、99%的是第一条，但还有11支股票不是，说明新股首次上市不都是股票第一次上市的股权变动记录;

data test4;
merge test3(in=aa)
smo.trd_co;
by stkcd;
if aa;
run;
*3、跟公司文件对比，发现99%的是首次上市时间;

data test5;
set test4(keep=Stkcd Shrchgdt scgtype Listdt Ipodt);
if Shrchgdt not eq Listdt then match1=0  ;
else match1=1;
if Shrchgdt not eq Ipodt then match2=0  ;
else match2=1;
run;

proc freq data=test5;
tables match1/missing;
run;


/*二、对于网络资料完善scgtype字典*/
data scgtype_dd;
set smo.a1_trd_capchg;
where scgtype in ('00062','00065','00068','00069','00660','01700','05005','05009',
'06005','07060','07700','25000','40005','40008','41000','46000',
'47000','60002','60005','60008','60009','77000','90009');
run;

proc sort data=scgtype_dd;by scgtype Stkcd;run;

data temp;
set scgtype_dd;
if first.scgtype then output;
by scgtype;
run;
