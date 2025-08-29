INSERT  INTO T_2_1  (
B010001 , -- 01 '客户ID'
B010002 , -- 02 '机构ID'
B010003 , -- 03 '对公客户名称'
B010004 , -- 04 '统一社会信用代码'
B010005 , -- 05 '组织机构登记/年检/更新日期'
B010006 , -- 06 '登记注册代码'
B010007 , -- 07 '登记注册/年检/更新日期'
B010008 , -- 08 '全球法人识别编码'
B010019 , -- 09 '注册资本'
B010020 , -- 10 '注册资本币种'
B010021 , -- 11 '实收资本'
B010022 , -- 12 '实收资本币种'
B010023 , -- 13 '成立日期'
B010024 , -- 14 '经营范围'
B010025 , -- 15 '行业类型'
B010026 , -- 16 '对公客户类型'
B010027 , -- 17 '控股类型'
B010028 , -- 18 '注册地国家地区'
B010029 , -- 19 '注册地址'
B010030 , -- 20 '注册地行政区划'
B010031 , -- 21 '电话号码'
B010032 , -- 22 '法定代表人姓名'
B010033 , -- 23 '法定代表人证件类型'
B010034 , -- 24 '法定代表人证件号码'
B010035 , -- 25 '财务人员姓名'
B010036 , -- 26 '财务人员证件类型'
B010037 , -- 27 '财务人员证件号码'
B010038 , -- 28 '基本存款账号'
B010039 , -- 29 '基本存款账户开户行行号'
B010040 , -- 30 '基本存款账户开户行名称'
B010041 , -- 31 '员工人数'
B010042 , -- 32 '上市情况'
B010043 , -- 33 '新型农业经营主体标识'
B010044 , -- 34 '外部评级结果'
B010045 , -- 35 '信用评级机构'
B010046 , -- 36 '内部评级结果'
B010047 , -- 37 '环境和社会风险分类'
B010048 , -- 38 '首次建立信贷关系年月'
B010049 , -- 39 '风险预警信号'
B010050 , -- 40 '关注事件代码'
B010053 , -- 41 '关停企业标识'
B010057 , -- 42 '母公司名称'
B010058 , -- 43 '违约概率'
B010061 , -- 44 '科技企业类型'
B010060,  -- 45 '采集日期'
DIS_DATA_DATE, -- 装入数据日期
DIS_BANK_ID,   -- 机构号
DEPARTMENT_ID  -- 业务条线
)
SELECT
T1.CUST_ID                                                          , -- 01 '客户ID'
T2.ORG_ID                                                           , -- 02 '机构ID'
COALESCE(T1.CUST_NAM, T1.CUST_NAM_EN, T3.FINA_ORG_NAME_LEGAL,'')    , -- 03 '对公客户名称'
T3.TYSHXYDM                                                         , -- 04 '统一社会信用代码'
NVL(TO_CHAR(TO_DATE(T3.ORG_UPD_DT,'YYYYMMDD'),'YYYY-MM-DD'),'9999-12-31')             , -- 05 '组织机构登记/年检/更新日期'
T1.ID_NO                                                            , -- 06 '登记注册代码'
NVL(TO_CHAR(TO_DATE(T3.REGISTER_INFO_UPD_DT,'YYYYMMDD'),'YYYY-MM-DD'),'9999-12-31')   , -- 07 '登记注册/年检/更新日期'
T3.LEI_CODE                                                         , -- 08 '全球法人识别编码'
T3.CAPITAL_AMT                                                      , -- 09 '注册资本'
NVL(T3.CAPITAL_CURRENCY,'CNY')                                      , -- 10 '注册资本币种' 20240613 与EAST逻辑同步,EAST部分币种为空默认为CNY,此处与EAST逻辑同步
NVL(T3.PAICL_UP_CAPITAL,'0')                                        , -- 11 '实收资本'  -- 0620_LHY 将NULL转为0
NVL(T3.PAICL_UP_CAP_CURR,'CNY')                                     , -- 12 '实收资本币种' 20240617 与EAST逻辑同步,原逻辑为:T3.PAICL_UP_CAP_CURR
NVL(TO_CHAR(TO_DATE(T3.BORROWER_BULID_YEAR,'YYYYMMDD'),'YYYY-MM-DD'),'9999-12-31')    , -- 13 '成立日期'
case when T3.CORP_BUSINSESS_TYPE is not null and SUBSTR(T3.BORROWER_PRODUCT_DESC, 1, 300) = '无' then (CASE WHEN T1.INLANDORRSHORE_FLG = 'Y' THEN TRIM(T3.CORP_BUSINSESS_TYPE)
WHEN T1.INLANDORRSHORE_FLG = 'N' THEN '境外' END) -- 20240704 LDP
when T3.CORP_BUSINSESS_TYPE is null and SUBSTR(T3.BORROWER_PRODUCT_DESC, 1, 300) = '无' then '承受招标人的委托,可承担工程总投资一亿元人民币以下的工程招标代理业务；建设项目可行性投资估算,项目经济评价、工程项目概算、预算、结算、决算编制及审核、工程量清算编制及审核、工程量清算编制、计价、标底编制.'
when SUBSTR(T3.BORROWER_PRODUCT_DESC, 1, 300) is null or SUBSTR(T3.BORROWER_PRODUCT_DESC, 1, 300) = '' then '承受招标人的委托,可承担工程总投资一亿元人民币以下的工程招标代理业务；建设项目可行性投资估算,项目经济评价、工程项目概算、预算、结算、决算编制及审核、工程量清算编制及审核、工程量清算编制、计价、标底编制.'
when SUBSTR(T3.BORROWER_PRODUCT_DESC, 1, 300) <> '无' then SUBSTR(T3.BORROWER_PRODUCT_DESC, 1, 300)
else '承受招标人的委托,可承担工程总投资一亿元人民币以下的工程招标代理业务；建设项目可行性投资估算,项目经济评价、工程项目概算、预算、结算、决算编制及审核、工程量清算编制及审核、工程量清算编制、计价、标底编制.'
end     , -- 14 '经营范围'   0625_LHY同步east逻辑
case when T1.INLANDORRSHORE_FLG = 'N' then '99999'
else T3.CORP_BUSINSESS_TYPE
end                                                                 , -- 15 '行业类型' 20240617 与EAST逻辑同步,原逻辑为:T3.CORP_BUSINSESS_TYPE
CASE
WHEN T1.INLANDORRSHORE_FLG = 'N' THEN
'9' -- '境外机构'
WHEN SUBSTR(T3.CUST_TYP, 1, 1) IN ('1','0') AND nvl(T3.CORP_SCALE,T3.CORP_SCALE_GL) = 'B' THEN  -- 与east校验修改农民专业合作社应映射为企业
'1' -- '大型企业'
WHEN SUBSTR(T3.CUST_TYP, 1, 1) IN ('1','0') AND nvl(T3.CORP_SCALE,T3.CORP_SCALE_GL) = 'M' THEN  -- 与east校验修改农民专业合作社应映射为企业
'2' -- '中型企业'
WHEN SUBSTR(T3.CUST_TYP, 1, 1) IN ('1','0') AND nvl(T3.CORP_SCALE,T3.CORP_SCALE_GL) = 'S' THEN  -- 与east校验修改农民专业合作社应映射为企业
'3' -- '小型企业'
WHEN SUBSTR(T3.CUST_TYP, 1, 1) IN ('1','0') AND nvl(T3.CORP_SCALE,T3.CORP_SCALE_GL) = 'T' THEN  -- 与east校验修改农民专业合作社应映射为企业
'4' -- '微型企业'
WHEN SUBSTR(T3.CUST_TYP, 1, 1) = '2' THEN
'5' -- '政府机关'
WHEN SUBSTR(T3.CUST_TYP, 1, 1) = '5' THEN
'6' -- '事业单位'
WHEN SUBSTR(T3.CUST_TYP, 1, 1) = '4' THEN
'7' -- '社会团体'
ELSE
'8' -- '其他组织机构'
END    AS DGKHLX                       , -- 16 '对公客户类型'20241015
CASE
WHEN SUBSTR(T3.CORP_HOLD_TYPE,1,1) = 'A' THEN
'01' -- 国有控股
WHEN SUBSTR(T3.CORP_HOLD_TYPE,1,1) = 'B' THEN
'02' -- 集体控股
WHEN SUBSTR(T3.CORP_HOLD_TYPE,1,1) = 'C' THEN
'03' -- 私人控股
WHEN SUBSTR(T3.CORP_HOLD_TYPE,1,1) = 'D' THEN
'04' -- 港澳台商控股
WHEN SUBSTR(T3.CORP_HOLD_TYPE,1,1) = 'E' THEN
'05' -- 外商控股
ELSE
'06' -- 其他
END                                                             , -- 17 '控股类型'
T3.NATION_CD                                                 	, -- 18 '注册地国家地区'
NVL(T4.BORROWER_REGISTER_ADDR,'无')                             , -- 19 '注册地址' 20240607 与EAST同步逻辑 原取值字段为 T4.BORROWER_REGISTER_ADDR
T1.REGION_CD                                                    , -- 20 '注册地行政区划'
-- T4.CUST_TELEPHONE_NO                                         , -- 21 '电话号码'
NVL(T4.CUST_TELEPHONE_NO,T4.HAND_PHONE_NO)                      , -- 21 '电话号码' 一表通2.0升级修改
T3.LEGAL_NAME                                                   , -- 22 '法定代表人姓名'
NVL(M.GB_CODE,M2.GB_CODE)                                       , -- 23 '法定代表人证件类型'
T3.LEGAL_CARD_NO                                  				, -- 24 '法定代表人证件号码'
T3.FINANCIAL_NAME                                 				, -- 25 '财务人员姓名'
M1.GB_CODE                                        				, -- 26 '财务人员证件类型'
T3.FINANCIAL_CARD_TYPE                            				, -- 27 '财务人员证件号码'
CASE WHEN NVL(T3.BASE_ACCT, T8.O_ACCT_NUM) IS NOT NULL
AND NVL(T3.BASE_ACCT_OP_ID, T12.BANK_CD) IS NOT NULL
AND COALESCE(T3.BASE_ACCT_OP_NAME,T2.ORG_NAM,CASE WHEN T1.ORG_NUM IS NULL THEN '金融市场部' END) IS NOT NULL
THEN REPLACE(NVL(T3.BASE_ACCT, T8.O_ACCT_NUM), ' ', '')
ELSE NULL
END                                      				        , -- 28 '基本存款账号'  20240605 与EAST同步逻辑 原取值字段为 T3.BASE_ACCT
CASE WHEN NVL(T3.BASE_ACCT, T8.O_ACCT_NUM) IS NOT NULL
AND NVL(T3.BASE_ACCT_OP_ID, T12.BANK_CD) IS NOT NULL
AND COALESCE(T3.BASE_ACCT_OP_NAME,T2.ORG_NAM,CASE WHEN T1.ORG_NUM IS NULL THEN '金融市场部' END) IS NOT NULL
THEN NVL(T3.BASE_ACCT_OP_ID, T12.BANK_CD)
ELSE NULL
END AS JBCKZHKHHH                               				, -- 29 '基本存款账户开户行行号'  20240605 与EAST同步逻辑 原取值字段为 T3.BASE_ACCT_OP_ID
CASE WHEN NVL(T3.BASE_ACCT, T8.O_ACCT_NUM) IS NOT NULL
AND NVL(T3.BASE_ACCT_OP_ID, T12.BANK_CD) IS NOT NULL
AND COALESCE(T3.BASE_ACCT_OP_NAME,T2.ORG_NAM,CASE WHEN T1.ORG_NUM IS NULL THEN '金融市场部' END) IS NOT NULL
THEN COALESCE(REPLACE(T3.BASE_ACCT_OP_NAME, ' ', ''),REPLACE(T2.ORG_NAM, ' ', ''),CASE WHEN T1.ORG_NUM IS NULL THEN '金融市场部' END)
ELSE NULL
END AS JBCKZHKHXMC                              				, -- 30 '基本存款账户开户行名称'  20240605 与EAST同步逻辑 原取值字段为 T3.BASE_ACCT_OP_NAME
CASE WHEN T13.CUST_ID IS NOT NULL AND NVL(T3.STAFF_NUM, 0) = 0 THEN 1
ELSE NVL(T3.STAFF_NUM, 0)
END                                                           	, -- 31 '员工人数' 20240625 LDP V2.4 与EAST同步逻辑,原为 T3.STAFF_NUM
/* CASE WHEN T3.STOCK_FLG = 'N' THEN NULL -- 发文：若无上市相关情况可以允许为空
ELSE T3.STOCK_FLG
END                             			                 	, -- 32 '上市情况'
*/
CASE WHEN T3.SSGSBZ = '01' THEN 'A' -- A股市场上市
WHEN T3.SSGSBZ = '02' THEN 'B' -- B股市场上市
WHEN T3.SSGSBZ = '03' THEN 'C' -- H股香港上市
WHEN T3.SSGSBZ = '04' THEN 'D' -- F股海外上市
END || '-' ||
CASE WHEN T3.SSD IN ('01','02') THEN 'CHN'
WHEN T3.SSD IN ('01','02') THEN 'CHN'
WHEN T3.SSD IN ('03') THEN 'HKG'
WHEN T3.SSD IN ('04') THEN 'SGP'
WHEN T3.SSD IN ('05') THEN 'LON'
WHEN T3.SSD IN ('06') THEN 'USA'
WHEN T3.SSD IN ('07') THEN 'TYO'
END || '-' ||
T3.SSGSDM                                         , -- 32 '上市情况'  一表通2.0升级 20240719 王金保修改
CASE WHEN T3.AGRICLTURAL_MANAGE_TYPE IN ('A','B','C','D','E','F','G') THEN '1' -- 是
WHEN T3.AGRICLTURAL_MANAGE_TYPE IS NULL THEN NULL
ELSE '0' -- 否
END                                               , -- 33 '新型农业经营主体标识'
T3.CUS_RISK_LEV_EXT                               , -- 34 '外部评级结果'
T3.RATING_ORGNAME                                 , -- 35 '信用评级机构'
T3.CUS_RISK_LV_DE                                 , -- 36 '内部评级结果'
T3.SAFETY_RISK_TYPE                               , -- 37 '环境和社会风险分类'
NVL(TO_CHAR(TO_DATE(T3.FIRST_CREDIT_DATE,'YYYYMMDD'),'YYYY-MM'),'9999-12') , -- 38 '首次建立信贷关系年月'
T6.RISK_SGN                                       , -- 39 '风险预警信号'
F.RISK_SGN_B                                      , -- 40 '关注事件代码'   0628 WWK
CASE WHEN T3.CORP_CLOSE_FLG ='N' THEN '0' -- 0否
WHEN T3.CORP_CLOSE_FLG ='Y' THEN '1' -- 1是
END                                               , -- 41 '关停企业标识' -- 0否,1.是
T9.CUST_GROUP_NAM                                 , -- 42 '母公司名称'
T3.CUST_PD                                        , -- 43 '违约概率'
CASE WHEN T3.TECH_CORP_TYPE IN ('C01', 'C02') AND T3.CORP_SCALE IN ('S', 'M', 'T') THEN '1' ELSE '0' END || -- 首位是科技型中小企业标识
CASE WHEN T3.TECH_CORP_TYPE ='E' THEN '1' ELSE '0' END ||                     -- 第二位是“专精特新”中小企业标识
CASE WHEN T3.TECH_CORP_TYPE ='F' THEN '1' ELSE '0' END ||                     -- 第三位是专精特新“小巨人”企业标识
CASE WHEN T3.TECH_CORP_TYPE ='H' THEN '1' ELSE '0' END ||                     -- 第四位是国家技术创新示范企业标识
CASE WHEN T3.TECH_CORP_TYPE ='J' THEN '1' ELSE '0' END ||                     -- 第五位是制造业单项冠军企业标识
CASE WHEN T3.IF_HIGH_SALA_CORP = 'Y' THEN '1' ELSE '0' END                 -- 第六位是高新技术企业标识
, -- ALTER BY WJB 20240624 一表通2.0升级 由原来的八位改为六位
-- 44 '科技企业类型'
TO_CHAR(TO_DATE(T1.DATA_DATE,'YYYYMMDD'),'YYYY-MM-DD'),           -- 45 '采集日期'
TO_CHAR(TO_DATE(I_DATE,'YYYYMMDD'),'YYYY-MM-DD'),                 -- 装入数据日期
T1.ORG_NUM,												          -- 机构号
'0098SJ'                                                          -- 业务条线  默认数据管理部
FROM SMTMODS.L_CUST_ALL T1 -- 全量客户信息表
INNER JOIN SMTMODS.L_CUST_C T3 -- 对公客户信息表
ON T1.CUST_ID = T3.CUST_ID
AND T3.DATA_DATE = I_DATE
LEFT JOIN VIEW_L_PUBL_ORG_BRA T2 -- 机构表
ON T1.ORG_NUM = T2.ORG_NUM
AND T2.DATA_DATE = I_DATE
LEFT JOIN SMTMODS.L_CUST_CONTACT T4 -- 客户联系信息表
ON T3.CUST_ID = T4.CUST_ID -- 20240704 LDP
AND T4.DATA_DATE = I_DATE
LEFT JOIN (SELECT CUST_ID,RISK_SGN_A,RISK_SGN_B
from (
SELECT F.CUST_ID,
CASE
WHEN F.SGN_TYP = 'A' THEN
GROUP_CONCAT(distinct F.RISK_SGN separator ';')
end AS RISK_SGN_A ,-- 拼接字段
CASE
WHEN F.SGN_TYP = 'B' THEN
GROUP_CONCAT(distinct F.RISK_SGN separator ';')
end  AS RISK_SGN_B -- 拼接字段
,ROW_NUMBER() over (partition by  F.CUST_ID order by F.SGN_TYP DESC) as NR
FROM SMTMODS.L_CUST_C_RISK_SGN F -- 客户风险预警及关注信号表
WHERE F.DATA_DATE = '20240331'
AND F.SGN_TYP IN ('A', 'B')
GROUP BY F.CUST_ID, F.SGN_TYP ) SS
where SS.NR=1
order by SS.CUST_ID,SS.RISK_SGN_B) F
ON T3.CUST_ID = F.CUST_ID
LEFT JOIN
(
SELECT BB.* FROM (SELECT AA.*, ROW_NUMBER() OVER(PARTITION BY CUST_ID ORDER BY RISK_SGN) RN
FROM SMTMODS.L_CUST_C_RISK_SGN AA -- 客户风险预警及关注信号表
WHERE AA.DATA_DATE = I_DATE) BB
WHERE BB.RN = 1
) T6 -- -- 客户风险预警及关注信号表
ON T3.CUST_ID = T6.CUST_ID
LEFT JOIN SMTMODS.L_CUST_C_GROUP_INFO T9 -- 集团客户信息表
ON T3.CUST_GROUP_NO = T9.CUST_GROUP_NO
AND T9.DATA_DATE = I_DATE
LEFT JOIN (SELECT *
FROM (SELECT CUST_ID, O_ACCT_NUM, ORG_NUM, ROW_NUMBER() OVER(PARTITION BY CUST_ID ORDER BY O_ACCT_NUM) RN
FROM SMTMODS.L_ACCT_DEPOSIT
WHERE DATA_DATE = I_DATE
AND DEPARTMENTD = 'CH') N
WHERE RN = 1) T8 -- 20240605 与EAST同步逻辑新增关联表
ON T1.CUST_ID = T8.CUST_ID
LEFT JOIN SMTMODS.L_PUBL_ORG_BRA T12 -- 20240605 与EAST同步逻辑新增关联表
ON T8.ORG_NUM = T12.ORG_NUM
AND T12.DATA_DATE = I_DATE
LEFT JOIN (SELECT DISTINCT CUST_ID FROM SMTMODS.L_AGRE_CREDITLINE WHERE DATA_DATE = I_DATE) T13 -- 20240625 与EAST同步逻辑新增关联表(授信额度表)
ON T1.CUST_ID = T13.CUST_ID
LEFT JOIN M_DICT_CODETABLE M -- 码值表
ON T3.LEGAL_CARD_TYPE = M.L_CODE
AND M.L_CODE_TABLE_CODE = 'C0001'
LEFT JOIN M_DICT_CODETABLE M1 -- 码值表
ON T3.FINANCIAL_TYPE = M1.L_CODE
AND M1.L_CODE_TABLE_CODE = 'C0001'
LEFT JOIN M_DICT_CODETABLE M2 -- 码值表
ON T3.LEGAL_CARD_TYPE2 = M2.L_CODE
AND M2.L_CODE_TABLE_CODE = 'C0001'
left join smtmods.L_CUST_BILL_TY ty -- 20240724 zjk update  修改单一法人客户排查同业客户在 2.3同业客户表中填报
on t3.cust_id = ty.ECIF_CUST_ID
AND ty.DATA_DATE = I_DATE
WHERE T1.DATA_DATE = I_DATE
-- AND T3.CUST_GROUP_NO IS NULL -- 集团号 BA:集团成员属于单一法人的 也要报送
-- AND (NVL(T3.CUST_TYP,0) NOT IN ('3') OR NVL(T3.DEPOSIT_CUSTTYPE,0) NOT IN ('13','14')) -- 不取个体工商户
AND NVL(T3.CUST_TYP,0) NOT IN ('3') -- 不取个体工商户
AND (
T3.CUST_TYP IS NOT NULL
OR
(T3.CUST_TYP IS NULL AND NVL(T3.DEPOSIT_CUSTTYPE,0) NOT IN ('13','14')) ) -- 不取个体工商户  修改 优先取贷款类个体工商户客户类型贷款标识如果为空用柜面补充202409
-- AND NVL(T1.CUST_TYPE,0) NOT IN ('12') -- 不取同业客户  update 20240731 丢失数据
AND
(
EXISTS (SELECT 1 FROM ybt_datacore.t_4_3 A WHERE A.D030015 = TO_CHAR(TO_DATE(I_DATE,'YYYYMMDD'),'YYYY-MM-DD')  AND A.D030003 = T1.CUST_ID)
OR
EXISTS (SELECT 1 FROM ybt_datacore.t_8_13 B WHERE B.H130023 = TO_CHAR(TO_DATE(I_DATE,'YYYYMMDD'),'YYYY-MM-DD') AND B.H130002 = T1.CUST_ID)
) -- 只报送在分户账和授信情况表中的客户
and ty.ECIF_CUST_ID is null -- 20240724 zjk update  修改单一法人客户排查同业客户在 2.3同业客户表中填报
;