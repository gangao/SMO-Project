data smo.trd_capchg;
set smo.trd_capchg;
format status $8.;
label status="��������";
if Shrchgdt <= '16oct2007'd then status = '������';
else if  Shrchgdt <= '28oct2008'd then status = '�½���';
else if  Shrchgdt <= '4aug2009'd then status = '������';
else if  Shrchgdt <= '20jun2014'd then status = 'ƽ����';
else status = '������';
/**/
format scgtype $30.;
label scgtype="��Ȩ�䶯����";
if Shrtyp="10000" then do;scgtype="�¹��״�����";end;
if Shrtyp="01000" then do;scgtype="�͹�/ת���ɱ�";end;
if Shrtyp="00100" then do;scgtype="��ɳ�Ȩ";end;
if Shrtyp="00010" then do;scgtype="��ϸ��Ȩ(������ϸ������)";end;
if Shrtyp="20000" then do;scgtype="ְ��������";end;
if Shrtyp="00002" then do;scgtype="�����¹�����";end;
if Shrtyp="30000" then do;scgtype="ת�������";end;
if Shrtyp="03000" then do;scgtype="��/ת����������";end;
if Shrtyp="40000" then do;scgtype="ծת������";end;
if Shrtyp="50000" then do;scgtype="�����������У����ۣ�";end;
if Shrtyp="05000" then do;scgtype="���˹��������У����ۣ�";end;
if Shrtyp="00500" then do;scgtype="ս���������У����ۣ�";end;
if Shrtyp="00050" then do;scgtype="�����������У����ۣ�";end;
if Shrtyp="00005" then do;scgtype="�߹ܹɷݱ䶯";end;
if Shrtyp="60000" then do;scgtype="��Ȩת��";end;
if Shrtyp="06000" then do;scgtype="�ɷݻع�";end;
if Shrtyp="00600" then do;scgtype="���պϲ�";end;
if Shrtyp="00060" then do;scgtype="�ǹ�������";end;
if Shrtyp="70000" then do;scgtype="���ʷ��˹�����";end;
if Shrtyp="00700" then do;scgtype="�ɵ�ծ";end;
if Shrtyp="07000" then do;scgtype="��Ȩ����";end;
if Shrtyp="00070" then do;scgtype="��Ȩ����";end;
if Shrtyp="00008" then do;scgtype="����";end;
if Shrtyp="00009" then do;scgtype="��������������";end;
if Shrtyp="90000" then do;scgtype="Ȩ֤��Ȩ";end;
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
label status="��������";
if Shrchgdt <= '16oct2007'd then status = '������';
else if  Shrchgdt <= '28oct2008'd then status = '�½���';
else if  Shrchgdt <= '4aug2009'd then status = '������';
else if  Shrchgdt <= '20jun2014'd then status = 'ƽ����';
else status = '������';
/**/
format scgtype $30.;
label scgtype="��Ȩ�䶯����";
if Shrtyp="10000" then do;scgtype="�¹��״�����";end;
if Shrtyp="01000" then do;scgtype="�͹�/ת���ɱ�";end;
if Shrtyp="00100" then do;scgtype="��ɳ�Ȩ";end;
if Shrtyp="00010" then do;scgtype="��ϸ��Ȩ(������ϸ������)";end;
if Shrtyp="20000" then do;scgtype="ְ��������";end;
if Shrtyp="00002" then do;scgtype="�����¹�����";end;
if Shrtyp="30000" then do;scgtype="ת�������";end;
if Shrtyp="03000" then do;scgtype="��/ת����������";end;
if Shrtyp="40000" then do;scgtype="ծת������";end;
if Shrtyp="50000" then do;scgtype="�����������У����ۣ�";end;
if Shrtyp="05000" then do;scgtype="���˹��������У����ۣ�";end;
if Shrtyp="00500" then do;scgtype="ս���������У����ۣ�";end;
if Shrtyp="00050" then do;scgtype="�����������У����ۣ�";end;
if Shrtyp="00005" then do;scgtype="�߹ܹɷݱ䶯";end;
if Shrtyp="60000" then do;scgtype="��Ȩת��";end;
if Shrtyp="06000" then do;scgtype="�ɷݻع�";end;
if Shrtyp="00600" then do;scgtype="���պϲ�";end;
if Shrtyp="00060" then do;scgtype="�ǹ�������";end;
if Shrtyp="70000" then do;scgtype="���ʷ��˹�����";end;
if Shrtyp="00700" then do;scgtype="�ɵ�ծ";end;
if Shrtyp="07000" then do;scgtype="��Ȩ����";end;
if Shrtyp="00070" then do;scgtype="��Ȩ����";end;
if Shrtyp="00008" then do;scgtype="����";end;
if Shrtyp="00009" then do;scgtype="��������������";end;
if Shrtyp="90000" then do;scgtype="Ȩ֤��Ȩ";end;
if scgtype="" then do;scgtype=Shrtyp;end;
run;


proc freq data=smo.a1_trd_capchg;
tables Shrtyp/missing out=aaa OUTCUM;
run;

proc freq data=smo.a1_trd_capchg;
tables scgtype/missing out=bbb OUTCUM;
run;


proc sort data=smo.a1_trd_capchg;by Shrtyp Stkcd Shrchgdt;run;

/*�¹��״����� �Ƿ��Ǹù�Ʊ��һ����Ȩ�䶯��¼*/
*1�����������¹��״����б䶯�Ĺ�Ʊ��ȡ�����б䶯��¼��Ȼ��ȡ��һ�������Ƿ����¹��״����б䶯;
data test;
set smo.a1_trd_capchg(keep=Stkcd Shrchgdt Shrtyp scgtype);
where scgtype="�¹��״�����";
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
*2��99%���ǵ�һ����������11֧��Ʊ���ǣ�˵���¹��״����в����ǹ�Ʊ��һ�����еĹ�Ȩ�䶯��¼;

data test4;
merge test3(in=aa)
smo.trd_co;
by stkcd;
if aa;
run;
*3������˾�ļ��Աȣ�����99%�����״�����ʱ��;

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


/*��������������������scgtype�ֵ�*/
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
