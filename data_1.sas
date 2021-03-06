DATA SMO.trd_day_2010_2015;
SET STOCK.Trd_day_2000_2010_all
smo.TRD_Day_2011_15;
run;

DATA smo.TRD_Co (Label="公司文件");
Infile 'D:\DATA\RAW\stock\基本数据\公司文件\TRD_Co.txt' encoding="utf-8" delimiter = '09'x Missover Dsd lrecl=32767 firstobs=2;
Format Cuntrycd 5.;
Format Stkcd $6.;
Format Stknme $20.;
Format Conme $100.;
Format Conme_en $100.;
Format Indcd $4.;
Format Indnme $50.;
Format Nindcd $10.;
Format Nindnme $50.;
Format Nnindcd $10.;
Format Nnindnme $50.;
Format Estbdt yymmdd10.;
Format Listdt yymmdd10.;
Format Favaldt yymmdd10.;
Format Curtrd $3.;
Format Ipoprm 10.;
Format Ipoprc 10.;
Format Ipocur $3.;
Format Nshripo 14.;
Format Parvcur $3.;
Format Ipodt yymmdd10.;
Format Parval 8.;
Format Sctcd 1.;
Format Statco $1.;
Format Crcd $6.;
Format Statdt yymmdd10.;
Format Commnt $200.;
Format Markettype 10.;
Informat Cuntrycd 5.;
Informat Stkcd $6.;
Informat Stknme $20.;
Informat Conme $100.;
Informat Conme_en $100.;
Informat Indcd $4.;
Informat Indnme $50.;
Informat Nindcd $10.;
Informat Nindnme $50.;
Informat Nnindcd $10.;
Informat Nnindnme $50.;
Informat Estbdt yymmdd10.;
Informat Listdt yymmdd10.;
Informat Favaldt yymmdd10.;
Informat Curtrd $3.;
Informat Ipoprm 10.;
Informat Ipoprc 10.;
Informat Ipocur $3.;
Informat Nshripo 14.;
Informat Parvcur $3.;
Informat Ipodt yymmdd10.;
Informat Parval 8.;
Informat Sctcd 1.;
Informat Statco $1.;
Informat Crcd $6.;
Informat Statdt yymmdd10.;
Informat Commnt $200.;
Informat Markettype 10.;
Label Cuntrycd="国家代码";
Label Stkcd="证券代码";
Label Stknme="证券简称";
Label Conme="公司全称";
Label Conme_en="公司英文全称";
Label Indcd="行业代码A";
Label Indnme="行业名称A";
Label Nindcd="行业代码B";
Label Nindnme="行业名称B";
Label Nnindcd="行业代码C";
Label Nnindnme="行业名称C";
Label Estbdt="公司成立日期";
Label Listdt="上市日期";
Label Favaldt="数据库最早交易记录的日期";
Label Curtrd="数据库中交易数据的计量货币";
Label Ipoprm="股票发行溢价";
Label Ipoprc="发行价格";
Label Ipocur="发行价格的计量货币";
Label Nshripo="发行数量";
Label Parvcur="股票面值的计量货币";
Label Ipodt="发行日期";
Label Parval="股票面值";
Label Sctcd="区域码";
Label Statco="公司活动情况";
Label Crcd="AB股交叉码";
Label Statdt="情况变动日";
Label Commnt="H股交叉码";
Label Markettype="市场类型";
Input Cuntrycd Stkcd $ Stknme $ Conme $ Conme_en $ Indcd $ Indnme $ Nindcd $ Nindnme $ Nnindcd $ Nnindnme $ Estbdt $ Listdt $ Favaldt $ Curtrd $ Ipoprm Ipoprc Ipocur $ Nshripo Parvcur $ Ipodt $ Parval Sctcd Statco $ Crcd $ Statdt $ Commnt $ Markettype ;
Run;
