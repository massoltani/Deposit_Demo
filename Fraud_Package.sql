---------------------------------------------------
-- Export file for user FRAUD2DM_IRANZAMIN       --
-- Created by msoltani on 10/8/2019, 10:10:46 AM --
---------------------------------------------------

set define off
spool Fraud_Package.log

prompt
prompt Creating package PKG_CARD_VIOLATION
prompt ===================================
prompt
CREATE OR REPLACE PACKAGE FRAUD2DM_IRANZAMIN."PKG_CARD_VIOLATION" is
  --procedure ins_v1(currdate date);
  procedure ins_v2(currdate date);
  procedure ins_v3(currdate date);
  procedure ins_v5(currdate date);
  procedure ins_v6(currdate date);
  --procedure ins_v7(currdate date);
  procedure ins_v8;
  procedure ins_v10(currdate date);
  procedure ins_v11(currdate date);
  procedure ins_v12(currdate date);
  procedure ins_v13(currdate date);
  procedure sole_procedure;
  procedure main_violation;
  procedure v1v10v7_violation;
  procedure ins_v1(currdate date);
  procedure ins_v30;
  procedure ins_v7;
  procedure ins_v31;
  procedure ins_v32;
  procedure ins_v33;

end pkg_card_violation;
/

prompt
prompt Creating package PKG_DEPOSIT_VIOLATION
prompt ======================================
prompt
CREATE OR REPLACE PACKAGE FRAUD2DM_IRANZAMIN."PKG_DEPOSIT_VIOLATION" is
  procedure ins_main;
  procedure ins_dep_v5(currdate date);
  procedure ins_dep_v50(currdate date);
  procedure ins_dep_v1(currdate date);
  procedure ins_dep_v8(currdate date);
  procedure ins_dep_v11(currdate date);
  procedure ins_dep_v13;
  procedure ins_dep_v14(currdate date);
  procedure ins_dep_v19(currdate date);
  procedure ins_dep_v20(currdate date);
  procedure ins_dep_v21;
  procedure ins_dep_v4;
  procedure ins_dep_v23 /*(currdate date)*/
  ;
  procedure ins_dep_v38(currdate date);
  procedure ins_dep_v22;
  procedure ins_dep_v30;
  procedure ins_dep_v40;
  procedure ins_dep_v6;
  procedure ins_dep_v7;
  procedure ins_dep_v42;
  procedure ins_dep_v12(currdate date);
end pkg_deposit_violation;
/

prompt
prompt Creating package PKG_FINANCIAL_VIOLATION
prompt ========================================
prompt
CREATE OR REPLACE PACKAGE FRAUD2DM_IRANZAMIN."PKG_FINANCIAL_VIOLATION" is

  procedure main_violation;
  procedure ins_fin_v6(currdate date);
  procedure ins_fin_v14(currdate date);
  procedure ins_fin_v1(currdate date);
  procedure ins_fin_v10(currdate date);
  procedure ins_fin_v4(currdate date);
  procedure ins_fin_v5(currdate date);
  procedure ins_fin_v8(currdate date);
  procedure ins_fin_v16(currdate date);
  procedure ins_fin_v20(currdate date);
  procedure ins_fin_v21(currdate date);
  procedure ins_fin_v13(currdate date);
end pkg_financial_violation;
/

prompt
prompt Creating package PKG_LOAN_VIOLATION
prompt ===================================
prompt
CREATE OR REPLACE PACKAGE FRAUD2DM_IRANZAMIN."PKG_LOAN_VIOLATION" is
  /*--
  procedure sole_procedure;
  procedure main_violation;
  procedure v1v10v7_violation;*/
  procedure ins_Loan_v1;
  procedure ins_Loan_v2;
  procedure ins_Loan_v3;
  procedure ins_Loan_v4;
  procedure ins_Loan_v6;
  procedure ins_Loan_v9;
  procedure ins_Loan_v19;
  procedure main_violation;
  procedure ins_Loan_v23(fromdate date);
  procedure ins_Loan_v22;
  procedure ins_Loan_v21;
  procedure ins_Loan_v24;
  procedure ins_Loan_v28(currdate date);
end PKG_LOAN_VIOLATION;
/

prompt
prompt Creating package body PKG_CARD_VIOLATION
prompt ========================================
prompt
CREATE OR REPLACE PACKAGE BODY FRAUD2DM_IRANZAMIN."PKG_CARD_VIOLATION" is
  t              number;
  currdate       date;
  rowcnt         number;
  OperationStart Date;
  OperationEnd   Date;

  --  max_date date;
  --------------------------proc v1
  procedure ins_v1(currdate date) is
  begin
    OperationStart := sysdate;
    insert /*+noappend*/
    into card_violation_1
      select /*+Parallel(4)*/
       seq_v1.nextval   as violation_id,
       Card_Num,
       CUSTOMER_NUM,
       Deposit_Num,
       DEPOSIT_TITLE,
       CARD_ACNT_NUM,
       ACNT_TITLE,
       card_Branch_Cod,
       VOUCHER_NUM,
       branch_cod,
       Amount           as voucher_amount,
       VOUCHER_DESC,
       process_name     as VOUCHER_TYPE,
       currdate         effective_date,
       MAJOR_TYPE_DESC,
       Violation_Reason,
       null,
       null,
       1,
       ISSUE_DATE,
       CARD_STAT,
       null
        from (select /*+Parallel(6)*/
              
               card_src.CARD_NUM,
               --in ezafe shode-----------------------------
               card_src.MAJOR_TYPE_DESC,
               ---------------------------------------------
               card_src.CUSTOMER_NUM,
               dp_src.DEPOSIT_NUM,
               dp_src.DEPOSIT_TITLE,
               card_src.CARD_ACNT_NUM,
               card_src.ACNT_TITLE,
               card_src.BRANCH_COD    as card_branch_cod,
               card_src.ISSUE_DATE    as ISSUE_DATE,
               card_src.CARD_STAT     as CARD_STAT,
               f.VOUCHER_NUM          as Voucher_Num,
               f.VOUCHER_DATE         as Voucher_date,
               f.BRANCH_COD           as Branch_cod,
               f.AMOUNT               as Amount,
               vch.VOUCHER_DESC       as voucher_desc,
               p.PROCESS_NAME         as process_name,
               
               --
               -- sharhe gheyre mojaz boodan-----------------------------
               case
                 when card_src.MAJOR_TYPE_COD not in (3) then
                  '„‘ —Ì ‰«„— »ÿ'
                 when card_src.MAJOR_TYPE_COD in (3) then
                  'Õ”«» ‰«„— »ÿ'
               end Violation_Reason
              --------------------------------------------------
              
                from factcardtransaction f
                inner join factcard2depositfls fc
                on f.SRC_CARD_KEY = fc.CARD_KEY
               inner join dimdeposit_mapped dp_src
                  on fc.DPST_KEY = dp_src.DEPOSITKEY_OLD
               inner join dimcard card_src
                  on f.SRC_CARD_KEY = card_src.CARD_KEY
                left join factdep2custfls dc
                  on dc.BRANCH_COD = dp_src.BRNCH_COD
                 and dc.DEPOSIT_TYPE = dp_src.DEPOSIT_TYPE
                 and dc.DPST_CUSTOMER_NUM = dp_src.CUSTOMER_NUM
                 and dc.DESPOSIT_SERIAL = dp_src.DEPOSIT_SERIAL
                 and (card_src.CUSTOMER_NUM = dc.CUSTOMER_NUM)
                 and dc.REL_IRANZAMIND_DATE >= currdate
               inner join dimprocess p
                  on p.process_cod = f.ACTION_TYPE
               inner join dimaccount acc_src
                  on acc_src.ACCOUNT_KEY = f.SRC_ACNT_KEY
              
               left outer join dimvoucher vch
                 on f.VOUCHER_Num = vch.VOUCHER_Num
                 and f.voucher_DATE = vch.voucher_DATE
                 and f.BRANCH_COD = vch.BRANCH_COD
                 and vch.voucher_DATE >= currdate
                 and vch.voucher_DATE < currdate + 1
              
               where f.EFFECTIVE_DATE >= currdate
                 and f.EFFECTIVE_DATE < currdate + 1
                 and fc.ISCURRENT = 1
                 and fc.REL_TYPE = 'M'
                 and card_src.CUSTOMER_NUM is not null
                 and dc.BRANCH_COD is null
                 and card_src.CUSTOMER_NUM <> dp_src.CUSTOMER_NUM
                 and (card_src.MAJOR_TYPE_COD not in (3) or
                     (card_src.MAJOR_TYPE_COD = 3 and
                     acc_src.ACCOUNT_TYPE_COD not in
                     (5480, 4992, 8374, 5525, 5479, 4458, 963))));
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_v1',
       'card_violation_1',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'ÃœÊ·  Œ·›«  1');
    commit;
  end;
  ------------------------------proc v2
  procedure ins_v2(currdate date) is
  begin
    OperationStart := sysdate;
    insert /*+ noappend*/
    into card_violation_2
      select seq_v2.nextval violation_id,
             Card_Num,
             Customer_Num,
             Voucher_Aggregate_Amount,
             Voucher_Count,
             EFFECTIVE_DATE,
             Unique_Terminal_Count,
             Branch_Cod,
             TL_INT_CURR,
             null,
             null,
             1,
             ISSUE_DATE,
             CARD_STAT,
             null
        from (select /*+parallel(10)*/
               card.CARD_NUM as Card_Num,
               card.CUSTOMER_NUM as Customer_Num,
               sum(f.AMOUNT) as Voucher_Aggregate_Amount,
               count(*) as Voucher_Count,
               f.EFFECTIVE_DATE as EFFECTIVE_DATE,
               count(distinct f.TERMINAL_KEY) as Unique_Terminal_Count,
               card.BRANCH_COD as Branch_Cod,
               card.TL_INT_CURR,
               MAX(CARD.ISSUE_DATE) AS ISSUE_DATE,
               MAX(CARD.CARD_STAT) AS CARD_STAT,
               null,
               null,
               null
                from factcardtransaction f
               inner join dimcard card
                  on f.SRC_CARD_KEY = card.CARD_KEY
                left outer join dimcard dstc
                  on dstc.CARD_KEY = f.DST_CARD_KEY
               inner join dimterminal dt
                  on f.terminal_key = dt.terminal_key
              /*left outer join dimvoucher vch
               on f.VOUCHER_Num = vch.voucher_num
              and f.voucher_date = vch.voucher_date
              and f.BRANCH_COD = vch.BRANCH_COD*/
               where f.ACTION_TYPE in (11)
                 and f.EFFECTIVE_DATE >= currdate
                 and f.EFFECTIVE_DATE < currdate + 1
                 and f.dst_card_num <> -1
                 and f.src_card_num <> -1
                 and f.IS_SHETABI = 0
                 and f.TERM_TYPE = 2
                 and dt.TERMINAL_ID <> -1
                 and f.REVERSE_TYPE = 0
                 and card.MAJOR_TYPE_COD not in (4)
                 and f.RESPONSE_COD = 0
                 and ((dstc.CUSTOMER_NUM <> card.CUSTOMER_NUM or
                     dstc.CUSTOMER_NUM is null) or
                     (dstc.CUSTOMER_NUM <> card.CUSTOMER_NUM and
                     card.CARD_ACNT_NUM <> dstc.CARD_ACNT_NUM))
              /* and vch.voucher_date >= currdate
              and vch.voucher_date < currdate + 1*/
               group by card.CARD_NUM,
                        card.CUSTOMER_NUM,
                        f.EFFECTIVE_DATE,
                        card.BRANCH_COD,
                        card.TL_INT_CURR
              having sum(f.AMOUNT) > card.TL_INT_CURR);
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_v2',
       'card_violation_2',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'ÃœÊ·  Œ·›«  2');
    commit;
  end;

  ------------------------------------proc v3
  procedure ins_v3(currdate date) is
  begin
  
    OperationStart := sysdate;
    insert /*+noappend*/
    into card_violation_3
      select seq_v3.nextval violation_id,
             Card_NUM,
             Customer_NUM,
             Voucher_Aggregate_Amount,
             Voucher_Count,
             EFFECTIVE_DATE,
             Branch_Cod,
             WL_INT_CURR,
             null,
             null,
             1,
             ISSUE_DATE,
             CARD_STAT,
             null
        from (select /*+parallel(10)*/
               0, --violation_id
               card.CARD_NUM as Card_NUM,
               card.CUSTOMER_NUM as Customer_NUM,
               sum(f.AMOUNT) as Voucher_Aggregate_Amount,
               count(*) as Voucher_Count,
               f.EFFECTIVE_DATE as EFFECTIVE_DATE,
               card.BRANCH_COD as Branch_Cod,
               card.WL_INT_CURR,
               max(CARD.ISSUE_DATE) AS ISSUE_DATE,
               max(CARD.CARD_STAT) AS CARD_STAT,
               null,
               null,
               null
                from factcardtransaction f
               inner join dimcard card
                  on f.src_card_key = card.CARD_KEY
                left outer join dimterminal dt
                  on f.terminal_key = dt.terminal_key
               where f.ACTION_TYPE in (2)
                 and f.EFFECTIVE_DATE >= currdate
                 and f.EFFECTIVE_DATE < currdate + 1
                 and dt.terminal_type = 2
                 and f.RESPONSE_COD = 0
                 and f.REVERSE_TYPE = 0
               group by card.CARD_NUM,
                        card.CUSTOMER_NUM,
                        f.EFFECTIVE_DATE,
                        card.BRANCH_COD,
                        card.WL_INT_CURR
              having sum(f.AMOUNT) > card.WL_INT_CURR);
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_v3',
       'card_violation_3',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'Ãœ«Ê·  Œ·›«  3');
    commit;
  end;

  ---------------------------proc v5
  procedure ins_v5(currdate date) is
  begin
  
    OperationStart := sysdate;
    insert /*+noappend*/
    into card_violation_5
      select seq_v5.nextval violation_id,
             Card_Num,
             card_Account_Num,
             Action_Type,
             Voucher_Amount,
             Voucher_Num,
             Voucher_Date,
             Voucher_DESC,
             branch_cod,
             effective_date,
             null,
             null,
             1,
             ISSUE_DATE,
             CARD_STAT,
             null
        from (select /*+parallel(10)*/
               0, --violation_id
               card.CARD_NUM      as Card_Num,
               card.CARD_ACNT_NUM as card_Account_Num,
               p.PROCESS_NAME     as Action_Type,
               f.AMOUNT           as Voucher_Amount,
               vch.VOUCHER_Num    as Voucher_Num,
               vch.voucher_date   as Voucher_Date,
               vch.VOUCHER_DESC   as Voucher_DESC,
               card.BRANCH_COD    as branch_cod,
               CARD.ISSUE_DATE    AS ISSUE_DATE,
               CARD.CARD_STAT     AS CARD_STAT,
               currdate           effective_date,
               null,
               null,
               null
                from factcardtransaction f
                left join dimcard card
                  on f.dst_card_key = card.card_key
                left outer join dimvoucher vch
                  on f.VOUCHER_Num = vch.voucher_num
                 and f.voucher_date = vch.voucher_date
                 and f.BRANCH_COD = vch.BRANCH_COD
               inner join dimprocess p
                  on p.PROCESS_COD = f.ACTION_TYPE
               inner join dimterminal dt
                  on dt.TERMINAL_KEY = f.TERMINAL_KEY
               where f.ACTION_TYPE not in (1, 2, 3, 24, 28, 14)
                 and card.MAJOR_TYPE_COD = 3
                 and f.EFFECTIVE_DATE >= currdate
                 and f.EFFECTIVE_DATE < currdate + 1
                 and vch.voucher_date >= currdate
                 and vch.voucher_date < currdate + 1
                 and f.dst_card_key <> -1
                 and f.src_card_key <> -1
                 and f.RESPONSE_COD = 0
                 and f.REVERSE_TYPE = 0
              -- and dt.TERMINAL_BANK_IIN <> 936450
              );
    rowcnt := sql%rowcount;
    commit;
    delete from card_violation_5 c5
     where c5.voucher_desc like '%—›⁄ „€«Ì— %';
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_v5',
       'card_violation_5',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'Ãœ«Ê·  Œ·›«  5');
    commit;
  end;

  ---------------------------------proc v6
  procedure ins_v6(currdate date) is
  begin
  
    OperationStart := sysdate;
    insert /*+noappend*/
    into card_violation_6
      select seq_v6.nextval, --violation_id
             SRC_CARD_NUM,
             CUSTOMER_NUM,
             CARD_BRANCH,
             CARD_BALANCE,
             effective_date,
             --PreviousRow_BAL,
             null,
             null,
             1,
             ISSUE_DATE,
             CARD_STAT,
             null
        from (select /*+parallel(4)*/
               card.CARD_NUM      as SRC_CARD_NUM,
               card.CUSTOMER_NUM  as CUSTOMER_NUM,
               card.BRANCH_COD    as CARD_BRANCH,
               fct.CARD_BAL       as CARD_BALANCE,
               CARD.ISSUE_DATE    AS ISSUE_DATE,
               CARD.CARD_STAT     AS CARD_STAT,
               fct.effective_date,
               /* nvl(LAG(fct.CARD_BAL, 1) OVER(partition by fct.CARD_KEY ORDER BY
                    fct.EFFECTIVE_DATE),
               0) PreviousRow_BAL,*/
               null,
               null,
               1
              
                from factcarddaily fct
               inner join dimcard card
                  on card.CARD_KEY = fct.CARD_KEY
                left outer join card_violation_6 v6
                  on v6.src_card_num = card.CARD_NUM
               where fct.CARD_BAL < 0
                 and fct.EFFECTIVE_DATE >= currdate
                 and fct.EFFECTIVE_DATE < currdate + 1
                 and fct.CARD_BAL <> -1
                 and card.MAJOR_TYPE_COD <> 5
                 and v6.src_card_num is null) T
       where T.CARD_BALANCE < 0;
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_v6',
       'card_violation_6',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'Ãœ«Ê·  Œ·›«  6');
    commit;
  end;

  ----------------------proc v7

  procedure ins_v7 /*(currdate date)*/
   is
  begin
  
    OperationStart := sysdate;
    insert /*+noappend*/
    into card_violation_7
      select seq_v7.nextval violation_id,
             CARD_NUM,
             CUSTOMER_NUM,
             Illegal_Customer_num,
             Illegal_DEPOSIT_NUM,
             Illegal_DEPOSIT_DESC,
             Change_DATE,
             effective_date,
             branch_cod,
             null,
             null,
             1,
             ISSUE_DATE,
             CARD_STAT,
             null
        from (
              
              select /*+parallel(10)*/
              distinct card.CARD_NUM         as CARD_NUM,
                        card.CUSTOMER_NUM     as CUSTOMER_NUM,
                        deposit.CUSTOMER_NUM  as Illegal_customer_num,
                        deposit.DEPOSIT_NUM   as Illegal_DEPOSIT_NUM,
                        deposit.DEPOSIT_TITLE as Illegal_DEPOSIT_DESC,
                        --fc2d.effective_date   as Change_DATE,
                        trunc(sysdate) as Change_DATE,
                        trunc(sysdate) -1 as effective_date,
                        card.BRANCH_COD as branch_cod,
                        CARD.ISSUE_DATE AS ISSUE_DATE,
                        CARD.CARD_STAT AS CARD_STAT,
                        null,
                        null,
                        null
                from factcard2depositfls fc2d
               inner join dimcard card
                  on card.CARD_KEY = fc2d.card_key
               inner join dimdeposit_mapped deposit
                  on fc2d.dpst_key = deposit.DEPOSITKEY_OLD
                left join factdep2custfls dc
                  on dc.BRANCH_COD = deposit.BRNCH_COD
                 and dc.DEPOSIT_TYPE = deposit.DEPOSIT_TYPE
                 and dc.DPST_CUSTOMER_NUM = deposit.CUSTOMER_NUM
                 and dc.DESPOSIT_SERIAL = deposit.DEPOSIT_SERIAL
                 and dc.CUSTOMER_NUM = card.CUSTOMER_NUM
                 and dc.REL_IRANZAMIND_DATE >= trunc(sysdate)
                 left join card_violation_7 v7
                 on card.CARD_NUM = v7.card_num
                 and card.CUSTOMER_NUM = v7.customer_num
               where dc.BRANCH_COD is null
                 and card.MAJOR_TYPE_COD not in (3)
                    --  and fc2d.START_DATE <= currdate
                 and fc2d.REL_TYPE = 'M'
                 and card.CARD_STAT = 'O'
                 and card.IS_CURRENT = 1
                 and fc2d.ISCURRENT = 1
                 and deposit.ISCURRENT = 1
                 and v7.card_num is null
              --  and fc2d.END_DATE > currdate
              
              /*select \*+parallel(10)*\
              0,
              card.CARD_NUM         as CARD_NUM,
              card.CUSTOMER_NUM     as CUSTOMER_NUM,
              deposit.CUSTOMER_NUM  as Illegal_customer_num,
              deposit.DEPOSIT_NUM   as Illegal_DEPOSIT_NUM,
              deposit.DEPOSIT_TITLE as Illegal_DEPOSIT_DESC,
              --fc2d.effective_date   as Change_DATE,
              currdate        as Change_DATE,
              currdate        as effective_date,
              card.BRANCH_COD as branch_cod,
              CARD.ISSUE_DATE AS ISSUE_DATE,
              CARD.CARD_STAT  AS CARD_STAT,
              null,
              null,
              null
               from factcard2depositfls fc2d
              inner join dimcard card
                 on card.CARD_KEY = fc2d.card_key
              inner join dimdepositcard deposit
                 on fc2d.dpst_key = deposit.DEPOSI
              inner join factdep2custfls dc
                 on dc.BRANCH_COD = deposit.BRNCH_COD
                and dc.DEPOSIT_TYPE = deposit.DEPOSIT_TYPE
                and dc.DPST_CUSTOMER_NUM = deposit.CUSTOMER_NUM
                and dc.DESPOSIT_SERIAL = deposit.DEPOSIT_SERIAL
              where card.CUSTOMER_NUM not in
                    (dc.CUSTOMER_NUM, dc.DPST_CUSTOMER_NUM)
                and card.MAJOR_TYPE_COD not in (3)
                and dc.REL_IRANZAMIND_DATE >=
                    to_date('01012100', 'ddmmyyyy')
                and fc2d.START_DATE <= currdate
                and fc2d.REL_TYPE = 'M'
                and fc2d.END_DATE > currdate*/
              );
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_v7',
       'card_violation_7',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       trunc(sysdate),
       'Ãœ«Ê·  Œ·›«  7');
    commit;
  end;

  --------------------------proc v8

  procedure ins_v8 /*(currdate date)*/
   is
  begin
  
    OperationStart := sysdate;
    insert /*noappend*/
    into card_violation_8
      select seq_v8.nextval violation_id,
             CARD_NUM,
             CARD_BRANCH_COD,
             CARD_ACCOUNT_Num,
             CARD_ACCOUNT_TITLE,
             ACCOUNT_Minor_Num,
             Change_DATE,
             effective_date,
             MAJOR_TYPE_COD,
             EXPIRE_DATE,
             null,
             null,
             1,
             ISSUE_DATE,
             CARD_STAT,
             null
        from (select /*+parallel(10)*/
               0,
               card.CARD_NUM,
               card.BRANCH_COD as CARD_BRANCH_COD,
               card.CARD_ACNT_NUM as CARD_ACCOUNT_Num,
               card.ACNT_TITLE as CARD_ACCOUNT_TITLE,
               CARD.ISSUE_DATE AS ISSUE_DATE,
               CARD.CARD_STAT AS CARD_STAT,
               case
                 when fc2d.rel_type != 'M' then
                 --nvl(dp.DEPOSIT_NUM, acc.account_num)
                  acc.account_num
                 else
                  'Õ”«» €Ì—„Ã«“'
               end ACCOUNT_Minor_Num,
               --fc2d.effective_date 
               currdate            as Change_DATE,
               currdate            effective_date,
               card.MAJOR_TYPE_COD,
               card.EXPIRE_DATE,
               null,
               null,
               null
                from factcard2depositfls fc2d
               inner join dimcard card
                  on card.CARD_KEY = fc2d.card_key
               inner join dimaccount acc
                  on acc.account_key = fc2d.acnt_key
              /*inner join dimdepositcard dp
              on dp.DEPOSIT_KEY = fc2d.dpst_key*/
                left outer join card_violation_8 cv8
                  on cv8.card_num = card.CARD_NUM
               where card.MAJOR_TYPE_COD in ('3', '4')
                 and ( /*dp.DEPOSIT_NUM is null and*/
                      acc.ACCOUNT_TYPE_COD not in
                      (825, 4991, 5479, 5480, 5959) or fc2d.rel_type <> 'M'
                     /*   or  (dp.DEPOSIT_NUM is not null and dp.DEPOSIT_TYPE not in ('750', '850', '824', '825'))*/
                     /*and dp.BRNCH_COD not in (1,197)*/
                     )
                    /* and fc2d.START_DATE <= currdate
                    and fc2d.END_DATE > currdate*/
                 and fc2d.ISCURRENT = 1
                 and card.IS_CURRENT = 1
                    /*and dp.iscurrent = 1*/
                 and cv8.card_num is null);
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_v8',
       'card_violation_8',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'Ãœ«Ê·  Œ·›«  8');
    commit;
  end;

  ----------------------------proc v10
  procedure ins_v10(currdate date) is
  begin
  
    OperationStart := sysdate;
    insert /*noappend*/
    into card_violation_10
      select seq_v10.nextval VIOLATION_ID,
             CARD_NUM,
             deposit_num,
             Account_num,
             branch_cod,
             prev_dep_num,
             Prev_Acc_Num,
             deposit_title,
             Account_Title,
             prev_dep_title,
             Prev_Acc_Title,
             Change_Date,
             currdate        effective_date,
             null,
             null,
             1,
             ISSUE_DATE,
             CARD_STAT,
             null
        from (select /*+parallel(10)*/
               0, --seq_v10.nextval VIOLATION_ID,
               dd.CARD_NUM,
               ddp.DEPOSIT_NUM as deposit_num,
               da.account_num as Account_num,
               dd.BRANCH_COD as branch_cod,
               max(DD.ISSUE_DATE) AS ISSUE_DATE,
               max(DD.CARD_STAT) AS CARD_STAT,
               nvl(LAG(ddp.DEPOSIT_NUM, 1)
                   OVER(ORDER BY min(fls.START_DATE)),
                   -1) as prev_dep_num,
               
               LAG(da.account_num, 1) OVER(partition by dd.CARD_num ORDER BY min(fls.START_DATE)) as Prev_Acc_Num,
               
               nvl(ddp.DEPOSIT_TITLE, 'ò«—  »Ì ‰«„') as deposit_title,
               da.ACCOUNT_TITLE as Account_Title,
               
               nvl(lag(ddp.DEPOSIT_TITLE, 1)
                   over(order by min(fls.START_DATE)),
                   '»Ì ‰«„') as prev_dep_title,
               lag(da.ACCOUNT_TITLE, 1) over(partition by dd.CARD_num order by min(fls.START_DATE)) as Prev_Acc_Title,
               
               --min(fls.effective_date) 
               currdate as Change_Date,
               null,
               null,
               null
              
                from dimcard dd
               inner join (select fls.card_key
                            from factcard2depositfls fls
                           inner join dimcard card
                              on card.CARD_KEY = fls.card_key
                           where fls.rel_type = 'M'
                             and card.MAJOR_TYPE_COD in ('8', '3')
                             and fls.START_DATE <= currdate
                             and fls.END_DATE > currdate
                           group by fls.card_key
                          having count(distinct fls.acnt_key) > 1) cnt
                  on dd.CARD_KEY = cnt.card_key
               inner join factcard2depositfls fls
                  on fls.card_key = cnt.card_key
               inner join dimaccount da
                  on fls.acnt_key = da.ACCOUNT_KEY
               inner join dimdepositcard ddp
                  on fls.dpst_key = ddp.DEPOSIT_KEY
               where dd.MAJOR_TYPE_COD in ('8', '3')
                 and fls.rel_type = 'M'
                    --    and fls.EFFECTIVE_DATE >= currdate
                    --    and fls.EFFECTIVE_DATE < currdate + 1
                    
                 and fls.START_DATE <= currdate
                 and fls.END_DATE > currdate
               group by dd.CARD_NUM,
                        ddp.DEPOSIT_NUM,
                        da.account_num,
                        da.ACCOUNT_TITLE,
                        ddp.DEPOSIT_TITLE,
                        dd.BRANCH_COD
              -- order by min(fls.effective_date)
              );
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_v10',
       'card_violation_10',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'Ãœ«Ê·  Œ·›«  10');
    commit;
  end;

  ----------------------------proc v11
  procedure ins_v11(currdate date) is
  begin
  
    OperationStart := sysdate;
    insert /*noappend*/
    into card_violation_11
      select seq_v11.nextval violation_id,
             CARD_NUM,
             branch_cod,
             CURRENTDEP_NUM,
             CURRENTACNT_Num,
             CHANGE_DATE,
             currdate        effective_date,
             null,
             null,
             1,
             ISSUE_DATE,
             CARD_STAT,
             null
        from (Select /*+parallel(10)*/
               0, --seq_v11.nextval violation_id,
               card.CARD_NUM as CARD_NUM,
               card.BRANCH_COD as branch_cod,
               max(CARD.ISSUE_DATE) AS ISSUE_DATE,
               max(CARD.CARD_STAT) AS CARD_STAT,
               nvl(listagg(dp.DEPOSIT_NUM, ',') WITHIN
                   GROUP(order by fls.card_key),
                   -1) as CURRENTDEP_Num,
               
               nvl(listagg(acc.account_num, ',') WITHIN
                   GROUP(order by fls.card_key),
                   -1) as CURRENTACNT_NUM,
               
               --               min(fls.EFFECTIVE_DATE)
               currdate as CHANGE_DATE,
               null,
               null,
               null
              
                from factcard2depositfls fls
               inner join (select fls.card_key
                            from factcard2depositfls fls
                           where fls.rel_type = 'M'
                                --and fls.EFFECTIVE_DATE >= currdate
                                -- and fls.EFFECTIVE_DATE < currdate + 1
                             and fls.ISCURRENT = 1
                           group by fls.card_key --, fls.effective_date
                          having count(distinct fls.dpst_key) > 1) cd
                  on fls.card_key = cd.card_key
               inner join dimcard card
                  on card.CARD_KEY = fls.CARD_KEY
               inner join dimaccount acc
                  on acc.account_key = fls.ACNT_KEY
               inner join dimdepositcard dp
                  on dp.DEPOSIT_KEY = fls.dpst_key
                left outer join card_violation_11 v11
                  on card.CARD_NUM = v11.card_num
               where fls.rel_type = 'M'
                 and fls.START_DATE <= currdate
                 and fls.END_DATE > currdate + 1
                 and v11.card_num is null
                 and fls.ISCURRENT = 1
               group by card.CARD_NUM, card.BRANCH_COD);
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_v11',
       'card_violation_11',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'Ãœ«Ê·  Œ·›«  11');
    commit;
  end;

  ----------------------------proc v12
  procedure ins_v12(currdate date) is
  begin
  
    OperationStart := sysdate;
    insert /*noappend*/
    into card_violation_12
      select seq_v12.nextval violation_id, --violation_id
             ff.Card_NUM,
             card.BANK_NAME,
             Voucher_Aggregate_Amount,
             Voucher_Count,
             EFFECTIVE_DATE,
             User_Changed_Stat,
             User_Changed_date,
             User_Cod,
             null
        from (select f.SRC_CARD_NUM as Card_NUM,
                      -- max(card.BANK_NAME) as BANK_NAME,
                      sum(f.AMOUNT) as Voucher_Aggregate_Amount,
                      count(*) as Voucher_Count,
                      f.EFFECTIVE_DATE as EFFECTIVE_DATE,
                      1 as User_Changed_Stat,
                      null User_Changed_date,
                      null as User_Cod
                 from factcardtransaction f
               /*left join dimallcard card
               on f.SRC_CARD_NUM = card.CARD_NUM*/
               /*inner join dimterminal dt
               on f.terminal_key = dt.terminal_key*/
                where /*card.BANK_IIN <> 505785
                                                                                                                                         and*/
                f.ACTION_TYPE in (2)
             and f.EFFECTIVE_DATE >= currdate
             and f.EFFECTIVE_DATE < currdate + 1
             and f.TERM_TYPE = 2
             and f.RESPONSE_COD = 0
             and f.REVERSE_TYPE = 0
             and f.IS_SHETABI = 1
               --   and dt.TERMINAL_ID <> -1
                group by f.SRC_CARD_NUM, f.EFFECTIVE_DATE
               
               having sum(f.AMOUNT) > 2000000) ff
        left join dimallcard card
          on ff.Card_NUM = card.CARD_NUM;
  
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_v12',
       'card_violation_12',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'Ãœ«Ê·  Œ·›«  12');
    commit;
  end;
  ---------------------------------
  procedure ins_v13(currdate date) is
  begin
    OperationStart := sysdate;
    insert /*+noappend*/
    into card_violation_13
      select seq_v13.nextval violation_id,
             Card_Num,
             Customer_Num,
             Voucher_Aggregate_Amount,
             Voucher_Count,
             EFFECTIVE_DATE,
             Unique_Terminal_Count,
             Branch_Cod,
             TL_INT_CURR,
             null,
             null,
             1,
             ISSUE_DATE,
             CARD_STAT,
             null
        from (select /*+parallel(6)*/
               0, -- violation_id
               f.SRC_CARD_NUM as Card_Num,
               max(card.CUSTOMER_NUM) as Customer_Num,
               sum(f.AMOUNT) as Voucher_Aggregate_Amount,
               count(*) as Voucher_Count,
               f.EFFECTIVE_DATE as EFFECTIVE_DATE,
               count(distinct f.TERMINAL_KEY) as Unique_Terminal_Count,
               max(card.BRANCH_COD) as Branch_Cod,
               max(card.TL_INT_CURR) as TL_INT_CURR,
               MAX(CARD.ISSUE_DATE) AS ISSUE_DATE,
               MAX(CARD.CARD_STAT) AS CARD_STAT
                from factcardtransaction f
               inner join dimcard card
                  on f.SRC_CARD_KEY = card.CARD_KEY
                left outer join dimcard dstc
                  on dstc.CARD_KEY = f.DST_CARD_KEY
              /*inner join dimterminal dt
              on f.terminal_key = dt.terminal_key*/
               where f.ACTION_TYPE in (11)
                 and f.EFFECTIVE_DATE = currdate
                    -- and f.EFFECTIVE_DATE < currdate + 1
                 and f.IS_SHETABI = 1
                 and f.TERM_TYPE = 2
                    -- and dt.TERMINAL_ID <> -1
                    --  and f.dst_card_num <> -1
                    --  and f.src_card_num <> -1
                 and f.REVERSE_TYPE = 0
                 and f.RESPONSE_COD = 0
                 and dstc.CARD_KEY is null
              /* and ((dstc.CUSTOMER_NUM <> card.CUSTOMER_NUM or
                  dstc.CUSTOMER_NUM is null) or
                  (dstc.CUSTOMER_NUM <> card.CUSTOMER_NUM and
                  card.CARD_ACNT_NUM <> dstc.CARD_ACNT_NUM))
              and ((dt.TERMINAL_ID <> -1 and dt.terminal_type = 2) or
                  (dt.TERMINAL_TYPE <> f.TERM_TYPE and
                  dt.TERMINAL_ID = -1 and f.TERM_TYPE = 2))*/
               group by f.SRC_CARD_NUM, f.EFFECTIVE_DATE
              having sum(f.AMOUNT) > 30000000);
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_v13',
       'card_violation_13',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'ÃœÊ·  Œ·›«  13');
    commit;
  end;
  ---------------------------------
  --- gozareshe card_violation_30 vojud nadasht va ijad gardid  --soltani
  procedure ins_v30 is
  begin
  --  pkg_utilities.Truncate_Table('card_violation_30');
    OperationStart := sysdate;
    insert /*+noappend*/
    into card_violation_30
      select seq_v30.nextval as VIOLATION_ID,
             tt.CARD_NUM,
             tt.MINOR_TYPE_DESC,
             tt.CUSTOMER_NUM_card,
             tt.CUSTOMER_NAME,
             tt.CARD_STAT,
             tt.CARD_STAT_DESC,
             tt.EFFECTIVE_DATE,
             tt.BRANCH_COD,
             tt.CARD_ACNT_NUM,
             tt.DEPNUM,
             tt.DPST_KEY,
             tt.CUSTOMER_NUM_dep,
             tt.DEPCUSTNAME,
             tt.DEPOSIT_TITLE,
             tt.ACNT_TITLE,
             tt.BRNCH_COD,
             tt.DEPOSIT_TYPE,
             tt.DEPOSIT_SERIAL,
             null,
             null,
             1,
             null
        from (select distinct t.CARD_NUM,
                              t.MINOR_TYPE_DESC,
                              t.CUSTOMER_NUM as CUSTOMER_NUM_card,
                              t.CUSTOMER_NAME,
                              t.CARD_STAT,
                              t.CARD_STAT_DESC,
                              t.ISSUE_DATE as EFFECTIVE_DATE,
                              t.BRANCH_COD,
                              t.CARD_ACNT_NUM,
                              d.BRNCH_COD || '-' || d.DEPOSIT_TYPE || '-' ||
                              d.CUSTOMER_NUM || '-' || d.DEPOSIT_SERIAL as DEPNUM,
                              f.DPST_KEY,
                              d.CUSTOMER_NUM as CUSTOMER_NUM_dep,
                              dc.CUST_LAST_NAME || '*' || dc.CUST_FIRST_NAME || '*' ||
                              dc.CUST_FATHER_NAME as DEPCUSTNAME,
                              d.DEPOSIT_TITLE,
                              t.ACNT_TITLE,
                              d.BRNCH_COD,
                              d.DEPOSIT_TYPE,
                              d.DEPOSIT_SERIAL
                from dimcard t
               inner join factcard2depositfls f
                  on t.CARD_KEY = f.CARD_KEY
               inner join dimdeposit_mapped d
                  on f.DPST_KEY = d.DEPOSITKEY_OLD
               inner join dimcustomer dc
                  on d.CUSTOMER_NUM = dc.CUSTOMER_NUM
               where t.IS_DEPOSIT_BASE = 1
                 and t.IS_CURRENT = 1
                 and f.ISCURRENT = 1
                 and f.REL_TYPE = 'M'
                 and t.CUSTOMER_NUM <> d.CUSTOMER_NUM) tt
      
        left join factdep2custfls fd
          on tt.BRNCH_COD = fd.BRANCH_COD
         and tt.DEPOSIT_TYPE = fd.DEPOSIT_TYPE
         and tt.CUSTOMER_NUM_dep = fd.DPST_CUSTOMER_NUM
         and tt.DEPOSIT_SERIAL = fd.DESPOSIT_SERIAL
         and fd.CUSTOMER_NUM = tt.CUSTOMER_NUM_card
         left join card_violation_30 v30
         on tt.card_num = v30.card_num
       where fd.BRANCH_COD is null
       and v30.card_num is null;
  
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_v30',
       'card_violation_30',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       trunc(sysdate),
       'ÃœÊ·  Œ·›«  30');
    commit;
  end;
  --------------------------------------------

  procedure ins_v31 is
  begin
    OperationStart := sysdate;
    insert /*+noappend*/
    into card_violation_31
      select seq_v31.nextval as VIOLATION_ID,
             ttt.DPST_KEY,
             ttt.NumOfCards,
             ttt.NumOfCusts,
             ttt.deposit_title,
             ttt.brnch_cod,
             ttt.deposit_type,
             ttt.customer_num,
             ttt.deposit_serial,
             ttt.deposit_open_date,
             ttt.deposit_stat_cod,
             ttt.deposit_stat_desc,
             ttt.draw_access_code,
             ttt.draw_access_desc,
             null,
             null,
             1,
             null,
             ttt.EFFECTIVE_DATE,
             ttt.FIRST_CARD_NUM
        from (select f.DPST_KEY,
                     count(*) as NumOfCards,
                     case
                       when count(distinct c.CUSTOMER_NUM) = 1 then
                        1
                       else
                        2
                     end NumOfCusts,
                     max(t.deposit_title) as deposit_title,
                     max(t.brnch_cod) as brnch_cod,
                     max(t.deposit_type) as deposit_type,
                     max(t.customer_num) as customer_num,
                     max(t.deposit_serial) as deposit_serial,
                     max(t.deposit_open_date) as deposit_open_date,
                     max(t.deposit_stat_cod) as deposit_stat_cod,
                     max(t.deposit_stat_desc) as deposit_stat_desc,
                     max(t.draw_access_code) as draw_access_code,
                     max(t.draw_access_desc) as draw_access_desc,
                     min(c.ISSUE_DATE) as EFFECTIVE_DATE,
                     max(c.CARD_NUM) keep(dense_rank first order by c.ISSUE_DATE) as FIRST_CARD_NUM
                from factcard2depositfls f
               inner join DIMDEPOSIT_MAPPED t
                  on f.DPST_KEY = t.DEPOSITKEY_OLD
               inner join dimcard c
                  on f.CARD_KEY = c.CARD_KEY
               inner join (select tt.BRANCH_COD,
                                 tt.DEPOSIT_TYPE,
                                 tt.DPST_CUSTOMER_NUM,
                                 tt.DESPOSIT_SERIAL
                            from FACTDEP2CUSTFLS tt
                           where tt.REL_IRANZAMIND_DATE > trunc(sysdate)
                             and tt.DPST2CUST_REL_COD in (0, 1)
                           group by tt.BRANCH_COD,
                                    tt.DEPOSIT_TYPE,
                                    tt.DPST_CUSTOMER_NUM,
                                    tt.DESPOSIT_SERIAL
                          having count(*) > 1) fd
                  on t.BRNCH_COD = fd.BRANCH_COD
                 and t.DEPOSIT_TYPE = fd.DEPOSIT_TYPE
                 and t.CUSTOMER_NUM = fd.DPST_CUSTOMER_NUM
                 and t.DEPOSIT_SERIAL = fd.DESPOSIT_SERIAL
                left outer join card_violation_31 v
                  on t.BRNCH_COD = v.Brnch_Cod
                 and t.DEPOSIT_TYPE = v.deposit_type
                 and t.CUSTOMER_NUM = v.customer_num
                 and t.DEPOSIT_SERIAL = v.deposit_serial
              
               where c.IS_CURRENT = 1
                 and t.draw_access_code <> 'A'
                 and c.CARD_STAT = 'O'
                 and c.MAJOR_TYPE_COD <> 7
                 and f.ISCURRENT = 1
                 and f.REL_TYPE = 'M'
                 and t.ISCURRENT = 1
                 and v.violation_id is null
               group by f.DPST_KEY
              having count(*) >= 1) ttt;
  
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_v31',
       'card_violation_31',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       trunc(sysdate),
       'ÃœÊ·  Œ·›«  31');
    commit;
  end;
  ----------------------------------------------------------

  procedure ins_v32 is
  begin
    OperationStart := sysdate;
    insert /*+noappend*/
    into card_violation_32
      select seq_v32.nextval as VIOLATION_ID,
             ttt.DPST_KEY,
             ttt.CARD_CUSTOMER_NUM,
             ttt.deposit_title,
             ttt.brnch_cod,
             ttt.deposit_type,
             ttt.customer_num,
             ttt.deposit_serial,
             ttt.deposit_open_date,
             ttt.deposit_stat_cod,
             ttt.deposit_stat_desc,
             ttt.draw_access_code,
             ttt.draw_access_desc,
             null,
             null,
             1,
             null,
             ttt.EFFECTIVE_DATE,
             ttt.FIRST_CARD_NUM,
             ttt.NumOfCards
        from (select f.DPST_KEY,
                     count(*) as NumOfCards,
                     c.CUSTOMER_NUM as CARD_CUSTOMER_NUM,
                     max(t.deposit_title) as deposit_title,
                     max(t.brnch_cod) as brnch_cod,
                     max(t.deposit_type) as deposit_type,
                     max(t.customer_num) as customer_num,
                     max(t.deposit_serial) as deposit_serial,
                     max(t.deposit_open_date) as deposit_open_date,
                     max(t.deposit_stat_cod) as deposit_stat_cod,
                     max(t.deposit_stat_desc) as deposit_stat_desc,
                     max(t.draw_access_code) as draw_access_code,
                     max(t.draw_access_desc) as draw_access_desc,
                     min(c.ISSUE_DATE) as EFFECTIVE_DATE,
                     max(c.CARD_NUM) keep(dense_rank first order by c.ISSUE_DATE) as FIRST_CARD_NUM
                from factcard2depositfls f
               inner join DIMDEPOSIT_MAPPED t
                  on f.DPST_KEY = t.DEPOSITKEY_OLD
               inner join dimcard c
                  on f.CARD_KEY = c.CARD_KEY
                left outer join card_violation_32 v
                  on t.BRNCH_COD = v.Brnch_Cod
                 and t.DEPOSIT_TYPE = v.deposit_type
                 and t.CUSTOMER_NUM = v.customer_num
                 and t.DEPOSIT_SERIAL = v.deposit_serial
              
               where c.IS_CURRENT = 1
                 and c.CARD_STAT = 'O'
                 and f.ISCURRENT = 1
                 and f.REL_TYPE = 'M'
                 and t.ISCURRENT = 1
                 and v.violation_id is null
               group by f.DPST_KEY, c.CUSTOMER_NUM
              having count(*) > 1) ttt;
  
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_v32',
       'card_violation_32',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       trunc(sysdate),
       'ÃœÊ·  Œ·›«  32');
    commit;
  end;

  ----------------------------------------------------

  procedure ins_v33 is
  begin
    OperationStart := sysdate;
    insert /*+noappend*/
    into card_violation_33
      select seq_v32.nextval      as VIOLATION_ID,
             dd.BRNCH_COD,
             dd.DEPOSIT_TYPE,
             dd.CUSTOMER_NUM,
             dd.DEPOSIT_SERIAL,
             dd.DEPOSITKEY_OLD,
             dd.DEPOSIT_TITLE,
             dd.DEPOSIT_OPEN_DATE,
             dd.DEPOSIT_STAT_COD,
             dd.DEPOSIT_STAT_DESC,
             dd.DRAW_ACCESS_CODE,
             dd.DRAW_ACCESS_DESC,
             d.CARD_NUM,
             d.MINOR_TYPE_DESC,
             d.ISSUE_DATE,
             d.EXPIRE_DATE,
             d.CARD_STAT,
             d.CARD_STAT_DESC,
             d.CUSTOMER_NUM       as Card_Customer_Code,
             d.CUSTOMER_NAME      as Card_Customer_Name,
             d.BRANCH_COD         as Card_Branch,
             null                 as user_cod,
             null                 as user_changed_date,
             1                    as user_changed_stat,
             null                 as comment_f,
             d.CARD_KEY
        from dimcard d
       inner join factcard2depositfls f
          on d.CARD_KEY = f.CARD_KEY
       inner join dimdeposit_mapped dd
          on f.DPST_KEY = dd.DEPOSITKEY_OLD
       inner join dimcustomer c
          on dd.CUSTOMER_NUM = c.CUSTOMER_NUM
        left join card_violation_33 v33
          on v33.card_key = d.CARD_KEY
       where d.IS_CURRENT = 1
         and dd.ISCURRENT = 1
         and f.ISCURRENT = 1
         and c.CUST_TYPE_COD = 1
            -- and dd.DEPOSIT_STAT_COD = 1
         and d.CARD_STAT = 'O'
         and v33.violation_id is null
         and d.MINOR_TYPE_COD not in (700, 199,198);
  
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_v33',
       'card_violation_33',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       trunc(sysdate),
       'ÃœÊ·  Œ·›«  33');
    commit;
  end;

  ---------------------------------
  procedure v1v10v7_violation is
  begin
    --select max(v.effective_date) into max_date from violation_1 v;
    currdate := to_date('09/22/2016', 'mm/dd/yyyy');
    while (currdate <= to_date('03/01/2017', 'mm/dd/yyyy')) loop
    
      ins_v12(currdate);
      currdate := currdate + 1;
    end loop;
  end;
  ---------------------------procedure sole run
  procedure sole_procedure is
  begin
    currdate := to_date('2/18/2014', 'mm/dd/yyyy');
    while (currdate <= to_date('3/16/2014', 'mm/dd/yyyy')) loop
      --ins_v8(currdate);
      currdate := currdate + 1;
    end loop;
  
    --select max(v.effective_date) into max_date from violation_1 v;
    currdate := to_date('09/22/2016', 'mm/dd/yyyy');
    while (currdate <= to_date('03/01/2017', 'mm/dd/yyyy')) loop
      ins_v2(currdate);
      ins_v3(currdate);
      --ins_v5(currdate);
      ins_v6(currdate);
      --ins_v7(currdate);
      --ins_v8(currdate);
      ins_v10(currdate);
      ins_v11(currdate);
      --ins_v1(currdate);
      ins_v12(currdate);
      currdate := currdate + 1;
    end loop;
  end;
  ----------------------------main_violation
  procedure main_violation is
    currdate date;
    lastDate date;
  begin
    select max(t.EFFECTIVE_DATE) into lastdate from factcardtransaction t;
    select max(a.effective_date)
      into currdate
      from card_violation_all a
     where a.report_id not in ('card_violation_30',
                               'card_violation_31',
                               'card_violation_32',
                               'card_violation_33',
                               'card_violation_7',
                               'card_violation_8');
    currdate := currdate + 1;
  
    if (currdate <= lastdate) then
      while (currdate <= lastdate) loop
        ins_v1(currdate);
        ins_v10(currdate);
        ins_v11(currdate);
        ins_v12(currdate);
        ins_v13(currdate);
        ins_v2(currdate);
        -- ins_v3(currdate);
        ins_v5(currdate);
        ins_v6(currdate);
        currdate := currdate + 1;
      end loop;
      ins_v30;
      ins_v31;
      ins_v32;
      ins_v33;
      ins_v7;
      ins_v8;
    end if;
  end;
begin
  currdate := trunc(sysdate) - 1;
  t        := 1;
end pkg_card_violation;
/

prompt
prompt Creating package body PKG_DEPOSIT_VIOLATION
prompt ===========================================
prompt
CREATE OR REPLACE PACKAGE BODY FRAUD2DM_IRANZAMIN."PKG_DEPOSIT_VIOLATION" is

  t              number;
  currdate       date;
  rowcnt         number;
  OperationStart Date;
  OperationEnd   Date;
  fromdate       date;
  todate         date;

  -------------- deposit_violation_1
  procedure ins_dep_v1(currdate date) is
  begin
    OperationStart := sysdate;
    insert /*+noappend*/
    into deposit_violation_1
    
      select /*+parallel(4)*/
       seq_dep_v1.nextval as violation_id,
       f.DEPOSIT_BRANCH_COD || '-' || f.DEPOSIT_TYPE || '-' ||
       f.CUSTOMER_NUM || '-' || f.DEPOSIT_SERIAL as deposit_num,
       f.CUSTOMER_NUM,
       f.VOUCHER_DATE,
       f.VOUCHER_NUM,
       null USER_COD,
       dv.VOUCHER_DESC,
       abs(f.AMOUNT) as AMOUNT,
       currdate as effective_date,
       null,
       1,
       f.DEPOSIT_BRANCH_COD,
       f.USER_COD FRUD_USER_COD,
       f.USER_BRANCH_COD FRUD_USER_BRNCH,
       null as comment_f,
       f.DEPOSIT_TYPE,
       f.DEPOSIT_SERIAL
        from factdeposittrns f
        left join dimtrnsstat dt
          on dt.REVERSE_COD = f.RVRS_TYPE_COD
        left join factcheqtrns fc
          on fc.VOUCHER_BRNCH = f.branch_cod
         and fc.VOUCHER_NUM = f.VOUCHER_NUM
         and fc.VOUCHER_DATE = f.VOUCHER_DATE
       inner join dimvoucher dv
          on dv.BRANCH_COD = f.branch_cod
         and dv.VOUCHER_DATE = f.voucher_date
         and dv.VOUCHER_NUM = f.voucher_num
       where f.voucher_date >= currdate
         and f.voucher_date < currdate + 1
         and f.DEPOSIT_TYPE in (10, 11)
         and f.trns_type_cod in (12, 11, 15)
            --   and f.DEPOSIT_TYPE <> 3
            --    and f.DEPOSIT_BRANCH_COD not in (198, 199)
         and fc.BRANCH_COD is null
         and dt.REVERSE_COD = 0
         
         and dv.VOUCHER_DESC not like '%ò«—„“œ%'
         and dv.VOUCHER_DESC not like ('% „»—%')
         and dv.VOUCHER_DESC not like '% „—%'
         and dv.VOUCHER_DESC not like ('%çò%')
         and dv.VOUCHER_DESC not like ('%.2111.%')
         and dv.VOUCHER_DESC not like ('%.4500.%')
         and dv.VOUCHER_DESC not like ('%.4454.%')
         and dv.VOUCHER_DESC not like ('%.4459.%')
         and dv.VOUCHER_DESC not like ('%.4466.%')
         and dv.VOUCHER_DESC not like ('%.4467.%')
         and dv.VOUCHER_DESC not like ('%.4468.%')
         and dv.VOUCHER_DESC not like ('%.5551.%')
         and dv.VOUCHER_DESC not like ('%.4470.%')
         and dv.VOUCHER_DESC not like ('%.5538.%')
         and dv.VOUCHER_DESC not like ('%.5550.%')
         and dv.VOUCHER_DESC not like ('%.5536.%')
         
         and dv.VOUCHER_DESC not like ('%.5649.%')
         and dv.VOUCHER_DESC not like ('%.4501.%')
         and dv.VOUCHER_DESC not like ('%.4482.%')
         and dv.VOUCHER_DESC not like ('%.4499.%')
         and dv.VOUCHER_DESC not like ('%.4502.%')
         and dv.VOUCHER_DESC not like ('%.5556.%')
         and dv.VOUCHER_DESC not like ('%.5419.%')
         and dv.VOUCHER_DESC not like ('%.4464.%')
         and dv.VOUCHER_DESC not like ('%.5583.%')
         and dv.VOUCHER_DESC not like ('%.4498.%')
         and dv.VOUCHER_DESC not like ('%.4486.%')
         and dv.VOUCHER_DESC not like ('%.4465.%');
         
    --    and f.CUSTOMER_NUM not in (6274330, 10001, 8888002);
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert into operation_Log
    values
      ('ins_deposit_v1',
       'violation_1',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       'ins_dep_violation_1');
    commit;
  end;

  --------------------deposit_violation_4
  procedure ins_dep_v4 is
  begin
    OperationStart := sysdate;
    insert /*+noappend*/
    into deposit_violation_4
      select /*+parallel(4)*/
       seq_dep_v4.nextval as violation_id,
       d.CUSTOMER_NUM,
       d.DEPOSIT_NUM as deposit_num,
       null USER_COD,
       d.DEPOSIT_OPEN_DATE,
       trunc(sysdate) - 1 as effective_date,
       null as user_changed_date,
       1 as user_changed_stat,
       d.BRNCH_COD,
       d.USER_COD FRUD_USER_COD,
       d.USER_BRNCH_COD FRUD_USER_BRNCH,
       null as comment_f,
       1 as SOLVED_FLAG,
       d.DEPOSIT_TYPE,
       d.DEPOSIT_SERIAL,
       c.CUST_TYPE_COD,
       c.CUST_TYPE_DESC,
       c.CUST_GROUP_CODE,
       c.CUST_GROUP_DESC,
       d.DEPOSIT_TITLE,
       d.DPST_GROUP_COD,
       d.DPST_GROUP_DESC,
       du.USER_NAME,
       du.USER_POST,
       du.USER_POST_DESC
        from dimdeposit d
       inner join dimcustomer c
          on d.CUSTOMER_NUM = c.CUSTOMER_NUM
       inner join dimuser du
          on d.USER_COD = du.USER_COD
         and d.USER_BRNCH_COD = du.BRNCH_COD
        left join deposit_violation_4 v4
          on v4.deposit_num = d.DEPOSIT_NUM
       where d.SIGNATURE_VALID = 0
            --  and d.DPST_GROUP_COD = 0   -- BE DARKHASTE BANK KE KOLE SEPORDEHA RO MIKHAST VA BARAYE HAMEYE MOSHTARIHA
            --  and c.CUST_TYPE_COD = 0    
         and d.DEPOSIT_OPEN_DATE < trunc(sysdate)
         and d.ISCURRENT = 1
         and d.DEPOSIT_STAT_COD <> 0
         and v4.deposit_num is null;
  
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v4',
       'violation_4',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       'INSERT DEPOSIT VIOLATION 4');
    commit;
  end;

  --------------------deposit_violation_5
  procedure ins_dep_v5(currdate date) is
  begin
    OperationStart := sysdate;
  
    /*pkg_utilities.Truncate_Table('tmp_chq_cnt');
    insert \*+noappend*\
    into tmp_chq_cnt
      select \*+parallel(4)*\
       f.CUSTOMER_NUM,
       (sum(f.PASS_CHQ_CNT) + sum(f.BLOCKED_CHQ_CNT) +
       sum(f.REVERSED_CHQ_CNT) + sum(f.SEMIRVRS_CHQ_CNT) +
       sum(f.DISCARD_CHQ_CNT)) as soorat,
       (sum(f.ACTIVE_CHQ_CNT) + sum(f.PASS_CHQ_CNT) +
       sum(f.BLOCKED_CHQ_CNT) + sum(f.REVERSED_CHQ_CNT) +
       sum(f.SEMIRVRS_CHQ_CNT) + sum(f.DISCARD_CHQ_CNT)) as makhraj,
       f.BRANCH_COD
        from factcheqbookdaily f
       inner join (select f1.*
                     from factcheqbookdaily f1
                    inner join dimcheqbook dcb
                       on f1.BRANCH_COD = dcb.DEPOSIT_BRNCH_COD
                      and f1.DEPOSIT_TYPE = dcb.DEPOSIT_TYPE
                      and f1.CUSTOMER_NUM = dcb.CUSTOMER_NUM
                      and f1.DEPOSIT_SERIAL = dcb.DEPOSIT_SERIAL
                      and f1.CHQBOOK_NUM = dcb.CHQBOOK_NUM
                    where dcb.ISSUE_DATE >= currdate
                      and dcb.ISSUE_DATE < currdate + 1
                      and f1.EFFECTIVE_DATE >= currdate
                      and f1.EFFECTIVE_DATE < currdate + 1) f1
          on f.CUSTOMER_NUM = f1.CUSTOMER_NUM
         and f.BRANCH_COD = f1.BRANCH_COD
         and f.EFFECTIVE_DATE >= currdate - 1
         and f.EFFECTIVE_DATE < currdate
      --and f.BRANCH_COD in (brnch)
       group by f.CUSTOMER_NUM, f.BRANCH_COD;
    commit;
    pkg_utilities.Truncate_Table('tmp_cust_new_chq');
    insert into tmp_cust_new_chq
      select t.customer_num,
             t.soorat / t.makhraj,
             t.soorat,
             t.makhraj,
             t.BRANCH_COD
        from tmp_chq_cnt t
       where t.soorat / t.makhraj < 4 / 5
         and t.makhraj <> 0;
    commit;
    
    pkg_utilities.Truncate_Table('tmp_fact_issued_cheq');
    insert into tmp_fact_issued_cheq
      select f.EFFECTIVE_DATE,
             f.BRANCH_COD,
             f.DEPOSIT_TYPE,
             f.CUSTOMER_NUM,
             f.DEPOSIT_SERIAL,
             f.CHQBOOK_NUM,
             dcb.ISSUE_DATE,
             f.PASS_CHQ_CNT,
             f.BLOCKED_CHQ_CNT,
             f.REVERSED_CHQ_CNT,
             f.SEMIRVRS_CHQ_CNT,
             f.ACTIVE_CHQ_CNT,
             f.DISCARD_CHQ_CNT
        from factcheqbookdaily f
       inner join dimcheqbook dcb
          on f.BRANCH_COD = dcb.DEPOSIT_BRNCH_COD
         and f.DEPOSIT_TYPE = dcb.DEPOSIT_TYPE
         and f.CUSTOMER_NUM = dcb.CUSTOMER_NUM
         and f.DEPOSIT_SERIAL = dcb.DEPOSIT_SERIAL
         and f.CHQBOOK_NUM = dcb.CHQBOOK_NUM
       where dcb.ISSUE_DATE >= currdate
         and dcb.ISSUE_DATE < currdate + 1
         and f.EFFECTIVE_DATE >= currdate
         and f.EFFECTIVE_DATE < currdate + 1
         and dcb.CHQ_SHEET_CNT <> 1;
    commit;*/
  
    /*  pkg_utilities.Truncate_Table('tmp_curr_prev_chq');
    insert into tmp_curr_prev_chq
      select *
        from (select f.CUSTOMER_NUM,
                     f.PASS_CHQ_CNT,
                     lag(f.PASS_CHQ_CNT, 1) over(order by f.ISSUE_DATE) prev_PASS_CHQ_CNT,
                     f.CHQBOOK_NUM,
                     lag(f.CHQBOOK_NUM, 1) over(order by f.ISSUE_DATE) prev_chqnum,
                     f.ISSUE_DATE,
                     dc.CHQ_SHEET_CNT,
                     lag(dc.CHQ_SHEET_CNT, 1) over(order by f.ISSUE_DATE) prev_CHQ_SHEET_CNT
                from factcheqbookdaily f
               inner join dimcheqbook dc
                  on f.BRANCH_COD = dc.DEPOSIT_BRNCH_COD
                 and f.DEPOSIT_TYPE = dc.DEPOSIT_TYPE
                 and f.CUSTOMER_NUM = dc.CUSTOMER_NUM
                 and f.DEPOSIT_SERIAL = dc.DEPOSIT_SERIAL
                 and f.CHQBOOK_NUM = dc.CHQBOOK_NUM
               where f.EFFECTIVE_DATE >= currdate
                 and f.EFFECTIVE_DATE < currdate + 1
                 and f.CUSTOMER_NUM in
                     (select t.customer_num
                        from tmp_fact_issued_cheq t
                       group by t.customer_num)) f
       where f.ISSUE_DATE >= currdate
         and f.ISSUE_DATE < currdate + 1;
    commit;*/
  
    /*    pkg_utilities.DRP_IX_FROM_TABLENAME('DEPOSIT_VIOLATION_5');
    
    insert into deposit_violation_5
      select \*+parallel(4)*\
       seq_dep_v5.nextval as violation_id,
       fc.CUSTOMER_NUM,
       fc.BRANCH_COD || '-' || fc.DEPOSIT_TYPE || '-' || fc.CUSTOMER_NUM || '-' ||
       fc.DEPOSIT_SERIAL as deposit_num,
       null USER_COD,
       dc.CHQ_SHEET_CNT,
       fc.chqbook_num as cheqbook_num,
       -- prev.prev_chqnum AS prev_cheqbook_num,
       -- prev.prev_chq_sheet_cnt as prev_cheqsheet_cnt,
       -- prev.prev_pass_chq_cnt as prev_PASS_CHQ_CNT,
       fc.ISSUE_DATE,
       currdate          as effective_date,
       null              as user_changed_date,
       1                 as user_changed_stat,
       fc.BRANCH_COD,
       dc.USER_COD       FRUD_USER_COD,
       t.soorat          as Cnt_Give_chq,
       t.makhraj         as Cnt_Total_chq,
       dc.USER_BRNCH_COD as frud_user_brnch,
       fc.deposit_type,
       fc.deposit_serial,
       null              as comment_f
        from tmp_fact_issued_cheq fc
       inner join tmp_cust_new_chq t
          on fc.CUSTOMER_NUM = t.customer_num
         and fc.branch_cod = t.BRANCH_COD
      \* inner join tmp_curr_prev_chq prev
      on fc.customer_num = prev.customer_num*\
       inner join dimcheqbook dc
          on fc.BRANCH_COD = dc.DEPOSIT_BRNCH_COD
         and fc.DEPOSIT_TYPE = dc.DEPOSIT_TYPE
         and fc.CUSTOMER_NUM = dc.CUSTOMER_NUM
         and fc.DEPOSIT_SERIAL = dc.DEPOSIT_SERIAL
         and fc.CHQBOOK_NUM = dc.CHQBOOK_NUM
        left join deposit_violation_5 dv
          on fc.BRANCH_COD = dv.DEPOSIT_BRANCH_COD
         and fc.DEPOSIT_TYPE = dv.DEPOSIT_TYPE
         and fc.CUSTOMER_NUM = dv.CUSTOMER_NUM
         and fc.DEPOSIT_SERIAL = dv.DEPOSIT_SERIAL
         and fc.CHQBOOK_NUM = dv.CHEQBOOK_NUM
       where dv.customer_num is null
         and fc.effective_date >= currdate
         and fc.effective_date < currdate + 1;*/
  
    pkg_utilities.DRP_IX_FROM_TABLENAME('DEPOSIT_VIOLATION_5');
  
    insert into deposit_violation_5
      select /*+parallel(4)*/
       seq_dep_v5.nextval as violation_id,
       t.CUSTOMER_NUM,
       t.deposit_num,
       null               USER_COD,
       t.CHQ_SHEET_CNT,
       t.cheqbook_num,
       t.ISSUE_DATE,
       currdate           as effective_date,
       null               as user_changed_date,
       1                  as user_changed_stat,
       t.BRANCH_COD,
       t.FRUD_USER_COD,
       t.Cnt_Give_chq,
       t.Cnt_Total_chq,
       t.frud_user_brnch,
       t.DEPOSIT_TYPE,
       t.DEPOSIT_SERIAL,
       null               as comment_f
        from (select d.CUSTOMER_NUM,
                     d.DEPOSIT_BRNCH_COD || '-' || d.DEPOSIT_TYPE || '-' ||
                     d.CUSTOMER_NUM || '-' || d.DEPOSIT_SERIAL as deposit_num,
                     max(d.CHQ_SHEET_CNT) as CHQ_SHEET_CNT,
                     d.CHQBOOK_NUM as cheqbook_num,
                     max(d.ISSUE_DATE) as ISSUE_DATE,
                     d.DEPOSIT_BRNCH_COD as BRANCH_COD,
                     max(d.USER_COD) FRUD_USER_COD,
                     sum(f.ACTIVE_CHQ_CNT) as Cnt_Give_chq,
                     sum(dd.CHQ_SHEET_CNT) as Cnt_Total_chq,
                     max(d.USER_BRNCH_COD) as frud_user_brnch,
                     d.DEPOSIT_TYPE,
                     d.DEPOSIT_SERIAL,
                     sum(case
                           when f.ACTIVE_CHQ_CNT > 0 then
                            1
                           else
                            0
                         end) Active_CheqBook_Count,
                     max(f.ACTIVE_CHQ_CNT) keep(dense_rank last order by dd.issue_date) as LastCheqBook_ActiveSheetCnt,
                     max(dd.chq_sheet_cnt) keep(dense_rank last order by dd.issue_date) as LastCheqBook_Cnt
                from dimcheqbook d
               inner join factcheqbookdaily f
                  on d.DEPOSIT_BRNCH_COD = f.BRANCH_COD
                 and d.DEPOSIT_TYPE = f.DEPOSIT_TYPE
                 and d.CUSTOMER_NUM = f.CUSTOMER_NUM
                 and d.DEPOSIT_SERIAL = f.DEPOSIT_SERIAL
               inner join dimcheqbook dd
                  on dd.DEPOSIT_BRNCH_COD = f.BRANCH_COD
                 and dd.DEPOSIT_TYPE = f.DEPOSIT_TYPE
                 and dd.CUSTOMER_NUM = f.CUSTOMER_NUM
                 and dd.DEPOSIT_SERIAL = f.DEPOSIT_SERIAL
                 and dd.CHQBOOK_NUM = f.CHQBOOK_NUM
                left join deposit_violation_5 dv
                  on d.DEPOSIT_BRNCH_COD = dv.DEPOSIT_BRANCH_COD
                 and d.DEPOSIT_TYPE = dv.DEPOSIT_TYPE
                 and d.CUSTOMER_NUM = dv.CUSTOMER_NUM
                 and d.DEPOSIT_SERIAL = dv.DEPOSIT_SERIAL
                 and d.CHQBOOK_NUM = dv.CHEQBOOK_NUM
               where d.ISSUE_DATE >= currdate
                 and d.ISSUE_DATE < currdate + 1
                 and d.CHQ_SHEET_CNT > 1
                 and dd.CHQ_SHEET_CNT > 1
                 and f.EFFECTIVE_DATE = currdate - 1
                 and dd.ISSUE_DATE < currdate
                 and dv.violation_id is null
               group by d.DEPOSIT_BRNCH_COD,
                        d.DEPOSIT_TYPE,
                        d.CUSTOMER_NUM,
                        d.DEPOSIT_SERIAL,
                        d.CHQBOOK_NUM
              -- having sum(f.ACTIVE_CHQ_CNT) / sum(dd.CHQ_SHEET_CNT) > 0.2
              ) t
       where (t.LastCheqBook_ActiveSheetCnt / t.LastCheqBook_Cnt > 0.2)
          or (t.Active_CheqBook_Count >= 2)
          or (t.Active_CheqBook_Count = 1 and
             t.LastCheqBook_ActiveSheetCnt = 0);
    rowcnt := sql%rowcount;
    commit;
  
    pkg_utilities.CRT_IX_FOR_TABLENAME('DEPOSIT_VIOLATION_5');
    pkg_utilities.Analyze_Table('FRAUD2DM_iranzamin',
                                'DEPOSIT_VIOLATION_5');
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v5',
       'violation_5',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       'INSERT DEPOSIT VIOLATION 5');
    commit;
  end;

  --------------------deposit_violation_6
  procedure ins_dep_v6 /*(currdate date)*/
   is
  begin
    OperationStart := sysdate;
  
    insert /*+noappend*/
    into deposit_violation_6
      select DEPOSIT_NUM,
             DEPOSIT_BRNCH_COD,
             DEPOSIT_TYPE,
             CUSTOMER_NUM,
             DEPOSIT_SERIAL,
             CHQBOOK_NUM,
             USER_BRNCH_COD,
             USER_COD,
             USER_NAM,
             CHQ_SHEET_CNT,
             CHQBOOK_TYPE,
             ISSUE_DATE,
             NUMOFCHQ,
             seq_dep_v6.nextval as violation_id,
             FRAUD_USER_COD,
             user_changed_date,
             user_changed_stat,
             comment_f,
             effective_date
        from (select d.DEPOSIT_BRNCH_COD || '-' || d.DEPOSIT_TYPE || '-' ||
                     d.CUSTOMER_NUM || '-' || d.DEPOSIT_SERIAL as DEPOSIT_NUM,
                     d.DEPOSIT_BRNCH_COD,
                     d.DEPOSIT_TYPE,
                     d.CUSTOMER_NUM,
                     d.DEPOSIT_SERIAL,
                     d.CHQBOOK_NUM,
                     d.USER_BRNCH_COD,
                     d.USER_COD as FRAUD_USER_COD,
                     d.USER_NAM,
                     d.CHQ_SHEET_CNT,
                     d.CHQBOOK_TYPE,
                     d.ISSUE_DATE,
                     count(*) as NUMOFCHQ,
                     null as USER_COD,
                     null as user_changed_date,
                     1 as user_changed_stat,
                     null as comment_f,
                     trunc(d.ISSUE_DATE) as effective_date
                from dimcheqbook d
               inner join factretchq f
                  on d.CUSTOMER_NUM = f.CUSTOMER_NUM
                left join deposit_violation_6 v6
                  on d.DEPOSIT_BRNCH_COD = v6.deposit_brnch_cod
                 and d.DEPOSIT_TYPE = v6.deposit_type
                 and d.CUSTOMER_NUM = v6.customer_num
                 and d.DEPOSIT_SERIAL = v6.deposit_serial
                 and d.CHQBOOK_NUM = v6.chqbook_num
               where f.REJDATE < trunc(d.ISSUE_DATE)
                 and (trunc(d.ISSUE_DATE) + 1 < f.DELDATE or
                     f.DELDATE is null)
                 and d.CHQ_SHEET_CNT > 1
                 and d.CUSTOMER_NUM <> 10001
                 and v6.deposit_num is null
                 and f.CHQREASON = '0'
                 and d.ISSUE_DATE >
                     to_date('01011390', 'ddmmyyyy', 'nls_calendar=persian')
                 and f.REJDATE >
                     to_date('01011390', 'ddmmyyyy', 'nls_calendar=persian')
               group by d.DEPOSIT_BRNCH_COD,
                        d.DEPOSIT_TYPE,
                        d.CUSTOMER_NUM,
                        d.DEPOSIT_SERIAL,
                        d.CHQBOOK_NUM,
                        d.USER_BRNCH_COD,
                        d.USER_COD,
                        d.USER_NAM,
                        d.CHQ_SHEET_CNT,
                        d.CHQBOOK_TYPE,
                        d.ISSUE_DATE
              
              /*
              insert \*+noappend*\
              into deposit_violation_6
              
                select seq_dep_v6.nextval as violation_id,
                       CUSTOMER_NUM,
                       deposit_num,
                       illgl_chqbook_num,
                       illgl_chqsht_cnt,
                       ISSUE_DATE,
                       USER_COD,
                       REVERSED_CHQ_CNT,
                       effective_date,
                       user_changed_date,
                       user_changed_stat,
                       BRANCH_COD,
                       FRUD_USER_COD,
                       FRUD_USER_BRNCH,
                       DEPOSIT_TYPE,
                       DEPOSIT_SERIAL,
                       CHQBOOK_NUM,
                       comment_f
                  from (select \*+parallel(4)*\
                         f.CUSTOMER_NUM,
                         f.BRANCH_COD || '-' || f.DEPOSIT_TYPE || '-' ||
                         f.CUSTOMER_NUM || '-' || f.DEPOSIT_SERIAL as deposit_num,
                         f.BRANCH_COD || '-' || f.DEPOSIT_TYPE || '-' ||
                         f.CUSTOMER_NUM || '-' || f.DEPOSIT_SERIAL || '-' ||
                         f.CHQBOOK_NUM as illgl_chqbook_num,
                         d.CHQ_SHEET_CNT illgl_chqsht_cnt,
                         d.ISSUE_DATE,
                         null USER_COD,
                         f.REVERSED_CHQ_CNT,
                         trunc(d.ISSUE_DATE) as effective_date,
                         null as user_changed_date,
                         1 as user_changed_stat,
                         f.BRANCH_COD,
                         d.USER_COD FRUD_USER_COD,
                         d.USER_BRNCH_COD FRUD_USER_BRNCH,
                         f.DEPOSIT_TYPE,
                         f.DEPOSIT_SERIAL,
                         f.CHQBOOK_NUM,
                         null as comment_f
                          from factcheqbookdaily f
                         inner join dimcheqbook d
                            on d.DEPOSIT_BRNCH_COD = f.BRANCH_COD
                           and d.DEPOSIT_TYPE = f.DEPOSIT_TYPE
                           and d.CUSTOMER_NUM = f.CUSTOMER_NUM
                           and d.DEPOSIT_SERIAL = f.DEPOSIT_SERIAL
                           and d.CHQBOOK_NUM = f.CHQBOOK_NUM
                         inner join (select f2.BRANCH_COD,
                                           f2.DEPOSIT_TYPE,
                                           f2.CUSTOMER_NUM,
                                           f2.DEPOSIT_SERIAL,
                                           f2.CHQBOOK_NUM,
                                           f2.CHQSHEET_NUM
                                      from factcheqtrns f2
                                     where f2.CHQ_TRNS_TYPE in (8, 9)
                                       and f2.VOUCHER_DATE < currdate
                                     group by f2.BRANCH_COD,
                                              f2.DEPOSIT_TYPE,
                                              f2.CUSTOMER_NUM,
                                              f2.DEPOSIT_SERIAL,
                                              f2.CHQBOOK_NUM,
                                              f2.CHQSHEET_NUM
                                    having max(f2.CHQ_TRNS_TYPE) <> 9) f1
                            on f1.CUSTOMER_NUM = f.CUSTOMER_NUM
                          LEFT JOIN deposit_violation_6 v6
                            on v6.deposit_branch_cod = f.BRANCH_COD
                           and v6.customer_num = f.CUSTOMER_NUM
                           and v6.deposit_type = f.DEPOSIT_TYPE
                           and v6.deposit_serial = f.DEPOSIT_SERIAL
                           and f.CHQBOOK_NUM = v6.chqbook_num
                         where d.ISSUE_DATE >= currdate
                           and d.ISSUE_DATE < currdate + 1
                           and f.EFFECTIVE_DATE >= currdate
                           and f.EFFECTIVE_DATE < currdate + 1
                           and v6.customer_num is null
                           and f.CUSTOMER_NUM <> 10001
                           and d.CHQ_SHEET_CNT <> 1
                         group by f.BRANCH_COD,
                                  f.DEPOSIT_TYPE,
                                  f.CUSTOMER_NUM,
                                  f.DEPOSIT_SERIAL,
                                  f.CHQBOOK_NUM,
                                  d.CHQ_SHEET_CNT,
                                  f.CUSTOMER_NUM,
                                  d.ISSUE_DATE,
                                  f.REVERSED_CHQ_CNT,
                                  d.USER_COD,
                                  d.USER_BRNCH_COD*/
              
              /*select \*+parallel(4)*\
              f.CUSTOMER_NUM,
              f.BRANCH_COD || '-' || f.DEPOSIT_TYPE || '-' ||
              f.CUSTOMER_NUM || '-' || f.DEPOSIT_SERIAL as deposit_num,
              f.BRANCH_COD || '-' || f.DEPOSIT_TYPE || '-' ||
              f.CUSTOMER_NUM || '-' || f.DEPOSIT_SERIAL || '-' ||
              f.CHQBOOK_NUM as illgl_chqbook_num,
              d.CHQ_SHEET_CNT illgl_chqsht_cnt,
              d.ISSUE_DATE,
              null USER_COD,
              f.REVERSED_CHQ_CNT,
              trunc(d.ISSUE_DATE) as effective_date,
              null as user_changed_date,
              1 as user_changed_stat,
              f.BRANCH_COD,
              d.USER_COD FRUD_USER_COD,
              d.USER_BRNCH_COD FRUD_USER_BRNCH,
              f.DEPOSIT_TYPE,
              f.DEPOSIT_SERIAL,
              f.CHQBOOK_NUM
               from factcheqbookdaily f
              inner join dimcheqbook d
                 on d.DEPOSIT_BRNCH_COD = f.BRANCH_COD
                and d.DEPOSIT_TYPE = f.DEPOSIT_TYPE
                and d.CUSTOMER_NUM = f.CUSTOMER_NUM
                and d.DEPOSIT_SERIAL = f.DEPOSIT_SERIAL
                and d.CHQBOOK_NUM = f.CHQBOOK_NUM
              inner join (select f1.*
                           from factcheqtrns f1
                           left join (select *
                                       from factcheqtrns f2
                                      where f2.CHQ_TRNS_TYPE = 9
                                        and f2.VOUCHER_DATE < currdate + 1) f2
                             on f1.BRANCH_COD = f2.BRANCH_COD
                            and f1.DEPOSIT_TYPE = f2.DEPOSIT_TYPE
                            and f1.CUSTOMER_NUM = f2.CUSTOMER_NUM
                            and f1.DEPOSIT_SERIAL = f2.DEPOSIT_SERIAL
                            and f1.CHQBOOK_NUM = f2.CHQBOOK_NUM
                            and f1.CHQSHEET_NUM = f2.CHQSHEET_NUM
                          where f1.CHQ_TRNS_TYPE = 8
                            and f2.BRANCH_COD is null) f1
                 on f1.CUSTOMER_NUM = f.CUSTOMER_NUM
               LEFT JOIN deposit_violation_6 v6
                 on v6.deposit_branch_cod = f.BRANCH_COD
                and v6.customer_num = f.CUSTOMER_NUM
                and v6.deposit_type = f.DEPOSIT_TYPE
                and v6.deposit_serial = f.DEPOSIT_SERIAL
                and f.CHQBOOK_NUM = v6.chqbook_num
              where d.ISSUE_DATE >= currdate
                and d.ISSUE_DATE < currdate + 1
                and d.ISSUE_DATE > f1.VOUCHER_DATE
                and v6.customer_num is null
                and f.CUSTOMER_NUM <> 10001
              group by f.BRANCH_COD,
                       f.DEPOSIT_TYPE,
                       f.CUSTOMER_NUM,
                       f.DEPOSIT_SERIAL,
                       f.CHQBOOK_NUM,
                       d.CHQ_SHEET_CNT,
                       f.CUSTOMER_NUM,
                       d.ISSUE_DATE,
                       f.REVERSED_CHQ_CNT,
                       d.USER_COD,
                       d.USER_BRNCH_COD*/
              );
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v6',
       'violation_6',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       'INSERT DEPOSIT VIOLATION 6');
    commit;
  end;

  --------------------deposit_violation_7
  procedure ins_dep_v7 is
  begin
  
    OperationStart := sysdate;
    /*insert \*+noappend*\
    into deposit_violation_7
      select \*+parallel(4)*\
       seq_dep_v7.nextval as violation_id,
       f.CUSTOMER_NUM,
       f.DEPOSIT_NUM as deposit_num,
       null USER_COD,
       fc.rvs_chq_amnt,
       fc.rvs_chq_cnt,
       f.DEPOSIT_OPEN_DATE,
       trunc(f.DEPOSIT_OPEN_DATE) as effective_date,
       null as user_changed_date,
       1 as user_changed_stat,
       f.BRNCH_COD,
       f.USER_COD FRUD_USER_COD,
       f.USER_BRNCH_COD FRUD_USER_BRNCH,
       f.DEPOSIT_TYPE,
       f.DEPOSIT_SERIAL
        from dimdeposit f
       inner join (select fc.CUSTOMER_NUM,
                          count(*) rvs_chq_cnt,
                          sum(fc.REVERSED_AMNT) rvs_chq_amnt
                     from factcheqsheetacc fc
                    where fc.CHQSHEET_STATE_COD = 'R'
                      and fc.ISSUE_DATE < currdate
                    group by fc.CUSTOMER_NUM) fc
          on f.CUSTOMER_NUM = fc.CUSTOMER_NUM
        left join deposit_violation_7 v7
          on v7.deposit_branch_cod = f.BRNCH_COD
         and v7.deposit_type = f.DEPOSIT_TYPE
         and v7.deposit_serial = f.DEPOSIT_SERIAL
         and v7.customer_num = f.CUSTOMER_NUM
       where f.DEPOSIT_TYPE in (2,1)
         and f.CURRENCY_COD = 'IRR'
         and f.DEPOSIT_OPEN_DATE >= currdate
         and f.DEPOSIT_OPEN_DATE < currdate + 1
         and f.CUSTOMER_NUM <> 10001
         and v7.customer_num is null;*/
    /*insert \*+noappend*\
    into deposit_violation_7
      select \*+parallel(4)*\
       seq_dep_v7.nextval as violation_id,
       f.CUSTOMER_NUM,
       f.DEPOSIT_NUM as deposit_num,
       null USER_COD,
       fc.rvs_chq_amnt,
       fc.rvs_chq_cnt,
       f.DEPOSIT_OPEN_DATE,
       trunc(f.DEPOSIT_OPEN_DATE) as effective_date,
       null as user_changed_date,
       1 as user_changed_stat,
       f.BRNCH_COD,
      null, -- f.USER_COD FRUD_USER_COD,
       null , --f.USER_BRNCH_COD FRUD_USER_BRNCH,
       f.DEPOSIT_TYPE,
       f.DEPOSIT_SERIAL,
       fc.rvs_chq_min_date,
       null as comment_f
        from dimdeposit f
       inner join (select fc.CUSTOMER_NUM,
                          count(*) rvs_chq_cnt,
                          sum(fc.REVERSED_AMNT) rvs_chq_amnt,
                          min(t.VOUCHER_DATE) rvs_chq_min_date,
                          min(nvl(t2.VOUCHER_DATE,
                                  to_date('01012100', 'ddmmyyyy'))) naghd_chq_min_date
                     from factcheqsheetacc fc
                    inner join factcheqtrns t
                       on fc.BRANCH_COD = t.BRANCH_COD
                      and fc.DEPOSIT_TYPE = t.DEPOSIT_TYPE
                      and fc.CUSTOMER_NUM = t.CUSTOMER_NUM
                      and fc.DEPOSIT_SERIAL = t.DEPOSIT_SERIAL
                      and fc.CHQBOOK_NUM = t.CHQBOOK_NUM
                      and fc.CHQSHEET_NUM = t.CHQSHEET_NUM
    
                     left join factcheqtrns t2
                       on t.BRANCH_COD = t2.BRANCH_COD
                      and t.DEPOSIT_TYPE = t2.DEPOSIT_TYPE
                      and t.CUSTOMER_NUM = t2.CUSTOMER_NUM
                      and t.DEPOSIT_SERIAL = t2.DEPOSIT_SERIAL
                      and t.CHQBOOK_NUM = t2.CHQBOOK_NUM
                      and t.CHQSHEET_NUM = t2.CHQSHEET_NUM
                      and t2.CHQ_TRNS_TYPE = 1
    
                    where fc.CHQSHEET_STATE_COD = 'R'
                      and t.VOUCHER_DATE < trunc(sysdate)
                      and t.CHQ_TRNS_TYPE = 2
                      and fc.ISSUE_DATE < trunc(sysdate)
                    group by fc.CUSTOMER_NUM) fc
          on f.CUSTOMER_NUM = fc.CUSTOMER_NUM
        left join deposit_violation_7 v7
          on v7.deposit_branch_cod = f.BRNCH_COD
         and v7.deposit_type = f.DEPOSIT_TYPE
         and v7.deposit_serial = f.DEPOSIT_SERIAL
         and v7.customer_num = f.CUSTOMER_NUM
       where f.DEPOSIT_TYPE in (2, 1)
         and f.CURRENCY_COD = 'IRR'
         and f.DEPOSIT_OPEN_DATE > fc.rvs_chq_min_date
         and f.DEPOSIT_OPEN_DATE < fc.naghd_chq_min_date
         and f.CUSTOMER_NUM <> 10001
         and v7.customer_num is null;*/
  
    insert /*+noappend*/
    into deposit_violation_7
      select seq_dep_v7.nextval as violation_id,
             tt.CUSTOMER_NUM,
             tt.DEPOSIT_NUM,
             tt.USER_COD,
             tt.rvs_chq_amnt,
             tt.rvs_chq_cnt,
             tt.VOUCHER_DATE,
             tt.effective_date,
             tt.user_changed_date,
             tt.user_changed_stat,
             tt.DEPOSIT_BRANCH_COD,
             tt.FRUD_USER_COD,
             tt.FRUD_USER_BRNCH,
             tt.DEPOSIT_TYPE,
             tt.DEPOSIT_SERIAL,
             tt.DEPOSIT_TITLE,
             null
        from (select /*+parallel(4)*/
               d.DEPOSIT_NUM,
               max(d.DEPOSIT_OPEN_DATE) as EFFECTIVE_DATE,
               min(r.REJDATE) as VOUCHER_DATE,
               max(d.BRNCH_COD) as DEPOSIT_BRANCH_COD,
               max(d.CUSTOMER_NUM) as CUSTOMER_NUM,
               sum(r.CHQAMNT) as RVS_CHQ_AMNT,
               max(d.USER_BRNCH_COD) as FRUD_USER_BRNCH,
               max(d.USER_COD) as FRUD_USER_COD,
               count(*) as RVS_CHQ_CNT,
               max(d.DEPOSIT_TYPE) as DEPOSIT_TYPE,
               max(d.DEPOSIT_SERIAL) as DEPOSIT_SERIAL,
               max(d.DEPOSIT_TITLE) as DEPOSIT_TITLE,
               null as user_changed_date,
               1 as user_changed_stat,
               null USER_COD
                from dimdeposit d
               inner join factdep2custfls f
                  on d.CUSTOMER_NUM = f.CUSTOMER_NUM
               inner join factretchq r
                  on r.BRNCH_COD = f.BRANCH_COD
                 and r.DEPOSIT_TYPE = f.DEPOSIT_TYPE
                 and r.CUSTOMER_NUM = f.DPST_CUSTOMER_NUM
                 and r.DEPOSIT_SERIAL = f.DESPOSIT_SERIAL
                left join deposit_violation_7 v7
                  on d.BRNCH_COD = v7.deposit_branch_cod
                 and d.DEPOSIT_TYPE = v7.deposit_type
                 and d.CUSTOMER_NUM = v7.customer_num
                 and d.DEPOSIT_SERIAL = v7.deposit_serial
               where d.DEPOSIT_OPEN_DATE > r.REJDATE
                 and (d.DEPOSIT_OPEN_DATE + 1 < r.DELDATE or
                     r.DELDATE is null)
                 and d.ISCURRENT = 1
                 and v7.violation_id is null
                 and d.DEPOSIT_TYPE in (10, 11)
                    --  and d.CUSTOMER_NUM <> 10001
                 and f.DPST2CUST_REL_COD in (0, 2)
              /*and r.REJDATE >
                  to_date('01011390', 'ddmmyyyy', 'nls_calendar=persian')
              and d.DEPOSIT_OPEN_DATE >
                  to_date('01011390', 'ddmmyyyy', 'nls_calendar=persian')*/
               group by d.DEPOSIT_NUM
              union
              select d.DEPOSIT_NUM,
                     max(d.DEPOSIT_OPEN_DATE) as EFFECTIVE_DATE,
                     min(r.REJDATE) as VOUCHER_DATE,
                     max(d.BRNCH_COD) as DEPOSIT_BRANCH_COD,
                     max(d.CUSTOMER_NUM) as CUSTOMER_NUM,
                     sum(r.CHQAMNT) as RVS_CHQ_AMNT,
                     max(d.USER_BRNCH_COD) as FRUD_USER_BRNCH,
                     max(d.USER_COD) as FRUD_USER_COD,
                     count(*) as RVS_CHQ_CNT,
                     max(d.DEPOSIT_TYPE) as DEPOSIT_TYPE,
                     max(d.DEPOSIT_SERIAL) as DEPOSIT_SERIAL,
                     max(d.DEPOSIT_TITLE) as DEPOSIT_TITLE,
                     null as user_changed_date,
                     1 as user_changed_stat,
                     null USER_COD
                from dimdeposit d
               inner join factdep2custfls f
                  on d.BRNCH_COD = f.BRANCH_COD
                 and d.DEPOSIT_TYPE = f.DEPOSIT_TYPE
                 and d.CUSTOMER_NUM = f.DPST_CUSTOMER_NUM
                 and d.DEPOSIT_SERIAL = f.DESPOSIT_SERIAL
               inner join factretchq r
                  on r.CUSTOMER_NUM = f.CUSTOMER_NUM
                left join deposit_violation_7 v7
                  on d.BRNCH_COD = v7.deposit_branch_cod
                 and d.DEPOSIT_TYPE = v7.deposit_type
                 and d.CUSTOMER_NUM = v7.customer_num
                 and d.DEPOSIT_SERIAL = v7.deposit_serial
               where d.DEPOSIT_OPEN_DATE > r.REJDATE
                 and (d.DEPOSIT_OPEN_DATE + 1 < r.DELDATE or
                     r.DELDATE is null)
                 and d.ISCURRENT = 1
                 and d.DEPOSIT_TYPE in (10, 11)
                 and v7.violation_id is null
                    --  and d.CUSTOMER_NUM <> 10001
                 and f.DPST2CUST_REL_COD in (0, 2)
              /* and r.REJDATE >
                  to_date('01011390', 'ddmmyyyy', 'nls_calendar=persian')
              and d.DEPOSIT_OPEN_DATE >
                  to_date('01011390', 'ddmmyyyy', 'nls_calendar=persian')*/
               group by d.DEPOSIT_NUM) tt;
  
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v7',
       'violation_7',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       'INSERT DEPOSIT VIOLATION 7');
    commit;
  end;

  --------------------deposit_violation_8
  procedure ins_dep_v8(currdate date) is
  begin
  
    OperationStart := sysdate;
    /*pkg_utilities.Truncate_Table('tmp_dep_v8');
    insert \*+noappend*\
    into tmp_dep_v8
      select \*+parallel(4)*\
       fd.BRANCH_COD || '-' || fd.DEPOSIT_TYPE || '-' || fd.CUSTOMER_NUM || '-' ||
       fd.DEPOSIT_SERIAL as deposit_num,
       dv.VOUCHER_DESC,
       dv.VOUCHER_DATE,
       fd.TRNS_TYPE_COD,
       dv.VOUCHER_NUM,
       fd.DEPOSIT_KEY,
       null USER_COD,
       fd.BRANCH_COD,
       fd.USER_COD FRUD_USER_COD,
       fd.USER_BRANCH_COD FRUD_USER_BRNCH
        from factdeposittrns fd
       inner join dimvoucher dv
          on fd.VOUCHER_DATE = dv.VOUCHER_DATE
         and fd.VOUCHER_NUM = dv.VOUCHER_NUM
         and fd.BRANCH_COD = dv.BRANCH_COD
       where fd.VOUCHER_DATE >= currdate --:date
         and fd.VOUCHER_DATE < currdate + 1;
    
    commit;*/
  
    insert into deposit_violation_8
      select seq_dep_v8.nextval as violation_id,
             null USER_COD,
             v.DEPOSIT_BRANCH_COD || '-' || v.DEPOSIT_TYPE || '-' ||
             v.CUSTOMER_NUM || '-' || v.DEPOSIT_SERIAL as deposit_num,
             dd.DEPOSIT_CLOSE_DATE,
             dv.voucher_desc,
             v.voucher_date,
             v.trns_type_cod,
             v.voucher_num,
             v.branch_cod,
             trunc(v.voucher_date) as effective_date,
             null as user_changed_date,
             1 as user_changed_stat,
             v.branch_cod,
             v.USER_COD as FRUD_USER_COD,
             v.USER_BRANCH_COD as FRUD_USER_BRNCH,
             null,
             dd.CUSTOMER_NUM
        from factdeposittrns v
       inner join dimdeposit dd
          on v.DEPOSIT_KEY = dd.DEPOSIT_KEY
       inner join dimvoucher dv
          on v.BRANCH_COD = dv.BRANCH_COD
         and v.VOUCHER_DATE = dv.VOUCHER_DATE
         and v.VOUCHER_NUM = dv.VOUCHER_NUM
       where dd.DEPOSIT_CLOSE_DATE < v.VOUCHER_DATE - 1
         and dd.ISCURRENT = 1
         and v.VOUCHER_DATE >= currdate
         and v.VOUCHER_DATE < currdate + 1
         and dv.VOUCHER_DATE >= currdate
         and dv.VOUCHER_DATE < currdate + 1;
  
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v8',
       'violation_8',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       '???? ?????? ????? 8');
    commit;
  end;

  --------------------deposit_violation_11
  procedure ins_dep_v11(currdate date) is
  begin
  
    OperationStart := sysdate;
    insert /*+noappend*/
    into deposit_violation_11
      select /*+parallel(4)*/
       seq_dep_v11.nextval as violation_id,
       null USER_COD,
       f.BRANCH_COD || '-' || f.DEPOSIT_TYPE || '-' || f.CUSTOMER_NUM || '-' ||
       f.DEPOSIT_SERIAL as deposit_num,
       f.CUSTOMER_NUM,
       f.VOUCHER_DATE,
       d.CUST_BIRTH_DATE,
       f.BALANCE,
       f.BALANCE open_bal,
       trunc(f.VOUCHER_DATE) as effective_date,
       null as user_changed_date,
       1 as user_changed_stat,
       f.BRANCH_COD,
       f.USER_COD FRUD_USER_COD,
       d.CUST_TYPE_COD,
       f.USER_BRANCH_COD FRUD_USER_BRNCH,
       f.DEPOSIT_TYPE,
       f.DEPOSIT_SERIAL,
       null
        from factdeposittrns f
       inner join dimcustomer d
          on f.CUSTOMER_NUM = d.CUSTOMER_NUM
        left join deposit_violation_11 v11
          on v11.deposit_branch_cod = f.BRANCH_COd
         and v11.deposit_type = f.DEPOSIT_TYPE
         and v11.customer_num = f.CUSTOMER_NUM
         and v11.deposit_serial = f.DEPOSIT_SERIAL
       where f.TRNS_TYPE_COD = 4
         and f.VOUCHER_DATE >= currdate
         and f.VOUCHER_DATE < currdate + 1
         and f.VOUCHER_DATE <= d.CUST_BIRTH_DATE
            -- and f.AMOUNT <> 0
         and d.CUST_TYPE_COD = 0
         and v11.customer_num is null;
  
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v11',
       'violation_11',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       '???? ?????? ????? 11');
    commit;
  end;

  --------------------deposit_violation_12
  procedure ins_dep_v12(currdate date) is
  begin
  
    OperationStart := sysdate;
  
    insert /*+noappend*/
    into deposit_violation_12
      select /*+parallel(4)*/
       seq_dep_v12.nextval as violation_id,
       null USER_COD,
       f.CUSTOMER_NUM,
       f.BRANCH_COD || '-' || f.DEPOSIT_TYPE || '-' || f.CUSTOMER_NUM || '-' ||
       f.DEPOSIT_SERIAL as deposit_num,
       f.BALANCE,
       f.EFFECTIVE_DATE as effective_date,
       null as user_changed_date,
       1 as user_changed_stat,
       f.BRANCH_COD,
       f.DEPOSIT_TYPE,
       f.DEPOSIT_SERIAL,
       null
        from factdeposit f
       inner join dimdeposit dd
          on dd.BRNCH_COD = f.BRANCH_COD
         and dd.DEPOSIT_TYPE = f.DEPOSIT_TYPE
         and dd.CUSTOMER_NUM = f.CUSTOMER_NUM
         and dd.DEPOSIT_SERIAL = f.DEPOSIT_SERIAL
        left join deposit_violation_12 v12
          on f.BRANCH_COD = v12.deposit_branch_cod
         and f.DEPOSIT_TYPE = v12.deposit_type
         and f.CUSTOMER_NUM = v12.customer_num
         and f.DEPOSIT_SERIAL = v12.deposit_serial
       where dd.START_DATE < currdate
         and dd.END_DATE > currdate
         and dd.DEPOSIT_STAT_COD = 0
            -- and dd.ISCURRENT=1
         and f.BALANCE <> 0
         and f.EFFECTIVE_DATE >= currdate
         and f.EFFECTIVE_DATE < currdate + 1
         and v12.deposit_num is null;
  
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v12',
       'violation_12',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       '???? ?????? ????? 12');
    commit;
  end;

  --------------------deposit_violation_13
  procedure ins_dep_v13 /*(currdate date)*/
   is
  begin
  
    OperationStart := sysdate;
    /*insert \*+noappend*\
    into deposit_violation_13
      select \*+parallel(4)*\
       seq_dep_v13.nextval as violation_id,
       dc.CUSTOMER_NUM,
       d.DEPOSIT_NUM as deposit_num,
       null USER_COD,
       dc.CUST_BIRTH_DATE,
       floor((d.DEPOSIT_OPEN_DATE - dc.CUST_BIRTH_DATE) / 356) as customer_years_old,
       d.DEPOSIT_OPEN_DATE as effective_date,
       null as user_changed_date,
       1 as user_changed_stat,
       d.BRNCH_COD,
       d.DEPOSIT_OPEN_DATE,
       null
        from dimdeposit d
       inner join dimcustomer dc
          on dc.CUSTOMER_NUM = d.CUSTOMER_NUM
        left join deposit_violation_13 v13
          on d.CUSTOMER_NUM = v13.customer_num
       where d.DPST_GROUP_COD = 0
         and d.DEPOSIT_OPEN_DATE - dc.CUST_BIRTH_DATE < 18 * 365
         and dc.CUST_TYPE_COD <> 1
         and d.DEPOSIT_OPEN_DATE >= currdate
         and d.DEPOSIT_OPEN_DATE < currdate + 1
         and d.ISCURRENT = 1
         and v13.customer_num is null;*/
  
    insert /*+noappend*/
    into deposit_violation_13
      select /*+parallel(4)*/
       seq_dep_v13.nextval as violation_id,
       dc.CUSTOMER_NUM,
       d.DEPOSIT_NUM as deposit_num,
       null USER_COD,
       dc.CUST_BIRTH_DATE,
       floor((d.DEPOSIT_OPEN_DATE - dc.CUST_BIRTH_DATE) / 356) as customer_years_old,
       d.DEPOSIT_OPEN_DATE as effective_date,
       null as user_changed_date,
       1 as user_changed_stat,
       d.BRNCH_COD,
       d.DEPOSIT_OPEN_DATE,
       null as comment_f,
       d.USER_BRNCH_COD,
       d.USER_COD as FRAUD_USER_COD,
       du.USER_NAME,
       du.USER_POST,
       du.USER_POST_DESC,
       d.DEPOSIT_TITLE,
       d.DEPOSIT_TYPE,
       d.DEPOSIT_SERIAL
        from dimdeposit d
       inner join dimcustomer dc
          on dc.CUSTOMER_NUM = d.CUSTOMER_NUM
        left join deposit_violation_13 v13
          on d.CUSTOMER_NUM = v13.customer_num
       inner join dimuser du
          on du.BRNCH_COD = d.USER_BRNCH_COD
         and du.USER_COD = d.USER_COD
       where d.DEPOSIT_TYPE in (10, 11)
         and d.DEPOSIT_OPEN_DATE - dc.CUST_BIRTH_DATE < 18 * 365
         and dc.CUST_TYPE_COD = 0
            --  and d.DEPOSIT_OPEN_DATE >= currdate
            --  and d.DEPOSIT_OPEN_DATE < currdate + 1
         and d.DEPOSIT_STAT_COD <> 0
         and d.ISCURRENT = 1
         and v13.customer_num is null;
  
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v13',
       'violation_13',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       '???? ?????? ????? 13');
    commit;
  end;

  --------------------deposit_violation_14
  procedure ins_dep_v14(currdate date) is
  begin
  
    OperationStart := sysdate;
  
    insert /*+noappend*/
    into deposit_violation_14
      select /*+parallel(4)*/
       seq_dep_v14.nextval as violation_id,
       f.EFFECTIVE_DATE ngtve_bal_date,
       f.BRANCH_COD || '-' || f.DEPOSIT_TYPE || '-' || f.CUSTOMER_NUM || '-' ||
       f.DEPOSIT_SERIAL as deposit_num,
       f.CUSTOMER_NUM,
       f.BALANCE,
       f.EFFECTIVE_DATE as effective_date,
       null as user_changed_date,
       1 as user_changed_stat,
       f.BRANCH_COD,
       null USER_CODE,
       f.DEPOSIT_TYPE,
       f.DEPOSIT_SERIAL,
       null
        from factdeposit f
        left join deposit_violation_14 v14
          on f.BRANCH_COD = v14.deposit_branch_cod
         and f.DEPOSIT_TYPE = v14.deposit_type
         and f.CUSTOMER_NUM = v14.customer_num
         and f.DEPOSIT_SERIAL = v14.deposit_serial
       where f.balance < 0
         and f.EFFECTIVE_DATE >= currdate
         and f.EFFECTIVE_DATE < currdate + 1
         and v14.deposit_branch_cod is null
      
      ;
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v14',
       'violation_14',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       '???? ?????? ????? 14');
    commit;
  end;

  --------------------deposit_violation_16
  procedure ins_dep_v16(currdate date) is
  begin
  
    OperationStart := sysdate;
  
    --procedure BODY
    OperationStart := currdate;
    --end of query
  
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v16',
       'violation_16',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       '???? ?????? ????? 16');
    commit;
  end;

  --------------------deposit_violation_17
  --BARRASSSI SHAVAD
  /* procedure ins_dep_v17(currdate date) is
  begin
  
    OperationStart := sysdate;
  
    insert \*+noappend*\
    into deposit_violation_17
      select \*+parallel(4)*\
       seq_dep_v17.nextval as violation_id,
       d.CUSTOMER_NUM,
       d.DEPOSIT_NUM       as deposit_num,
       null                USER_COD,
       d.DEPOSIT_OPEN_DATE,
       fa.BALANCE          main_deposit_bal,
       sfa.balance         support_dpst_bal,
       currdate            as effective_date,
       null                as user_changed_date,
       1                   as user_changed_stat,
       d.BRNCH_COD,
       d.USER_COD          FRUD_USER_COD,
       d.USER_BRNCH_COD    FRUD_USER_BRNCH,
       d.DEPOSIT_TYPE,
       d.DEPOSIT_SERIAL
        from dimdeposit d
       inner join dimdeposittype dt
          on d.DEPOSIT_TYPE = dt.DEPOSIT_TYPE
         and dt.DPST_GROUP_COD = 0
        left outer join FACTDEP2CUSTFLS f
          on d.BRNCH_COD = f.BRANCH_COD
         and d.DEPOSIT_TYPE = f.DEPOSIT_TYPE
         and d.CUSTOMER_NUM = f.DPST_CUSTOMER_NUM
         and d.DEPOSIT_SERIAL = f.DESPOSIT_SERIAL
         and f.DPST2CUST_REL_COD = 3
        left outer join factdepositacc fa
          on d.DEPOSIT_KEY = fa.DEPOSIT_KEY
        left outer join factdep2depfls ff
          on d.BRNCH_COD = ff.MAIN_BRANCH_COD
         and d.DEPOSIT_TYPE = ff.MAIN_DPST_TYPE
         and d.CUSTOMER_NUM = ff.MAIN_CUST_NUM
         and d.DEPOSIT_SERIAL = ff.MAIN_DPST_SERIAL
        left outer join factdepositacc sfa
          on sfa.BRANCH_COD = ff.SUP_BRANCH_COD
         and sfa.DEPOSIT_TYPE = ff.SUP_DPST_TYPE
         and sfa.CUSTOMER_NUM = ff.SUP_CUST_NUM
         and sfa.DEPOSIT_SERIAL = ff.SUP_DPST_SERIAL
        left join deposit_violation_17 v17
          on d.BRNCH_COD = v17.deposit_branch_cod
         and d.DEPOSIT_TYPE = v17.deposit_type
         and d.CUSTOMER_NUM = v17.customer_num
         and d.DEPOSIT_SERIAL = v17.deposit_serial
       where f.BRANCH_COD is null
         AND (F.BRANCH_COD, f.DEPOSIT_TYPE, f.DPST_CUSTOMER_NUM,
              f.DESPOSIT_SERIAL) NOT IN
             (SELECT DISTINCT f.BRANCH_COD,
                              f.DEPOSIT_TYPE,
                              f.DPST_CUSTOMER_NUM,
                              f.DESPOSIT_SERIAL
                FROM FACTDEP2CUSTFLS F
               WHERE F.DPST2CUST_REL_COD = 3)
         and v17.deposit_branch_cod is null;
  
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert \*+ append *\
    into operation_Log
    values
      ('ins_deposit_v17',
       'violation_17',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       '???? ?????? ????? 17');
    commit;
  end;*/

  --------------------deposit_violation_19
  procedure ins_dep_v19(currdate date) is
  begin
  
    OperationStart := sysdate;
  
    insert /*+noappend*/
    into deposit_violation_19
      select /*+parallel(4)*/
       seq_dep_v19.nextval as violation_id,
       fl.MAIN_CUST_NUM,
       fl.MAIN_BRANCH_COD || '-' || fl.MAIN_DPST_TYPE || '-' ||
       fl.MAIN_CUST_NUM || '-' || fl.MAIN_DPST_SERIAL as deposit_num,
       f.BALANCE,
       dd.DEPOSIT_OPEN_DATE,
       fl.SUP_CUST_NUM,
       0 intrs_to_pay,
       currdate effective_date,
       f.VOUCHER_DATE,
       null USER_COD,
       null as user_changed_date,
       1 as user_changed_stat,
       fl.MAIN_BRANCH_COD,
       f.USER_COD FRUD_USER_COD,
       f.USER_BRANCH_COD FRUD_USER_BRNCH,
       f.DEPOSIT_TYPE,
       f.DEPOSIT_SERIAL,
       null,
       f.DEPOSIT_BRANCH_COD
        from factdep2depfls fl
       inner join factdeposittrns f
          on fl.SUP_BRANCH_COD = f.BRANCH_COD
         and fl.SUP_DPST_TYPE = f.DEPOSIT_TYPE
         and fl.SUP_CUST_NUM = f.CUSTOMER_NUM
         and fl.SUP_DPST_SERIAL = f.DEPOSIT_SERIAL
       inner join dimdeposittype dt
          on fl.MAIN_DPST_TYPE = dt.DEPOSIT_TYPE
         and dt.DPST_GROUP_COD = 9
        left join dimdeposit dd
          on fl.MAIN_BRANCH_COD = dd.BRNCH_COD
         and fl.MAIN_DPST_TYPE = dd.DEPOSIT_TYPE
         and fl.MAIN_CUST_NUM = dd.CUSTOMER_NUM
         and fl.MAIN_DPST_SERIAL = dd.DEPOSIT_SERIAL
        left join deposit_violation_19 v19
          on v19.deposit_branch_num = dd.BRNCH_COD
         and v19.deposit_type = dd.DEPOSIT_TYPE
         and v19.main_cust_num = dd.CUSTOMER_NUM
         and v19.deposit_serial = dd.DEPOSIT_SERIAL
       where (fl.RELATION_COD not in (0, 1, 3) or dd.BRNCH_COD is null)
         and dd.ISCURRENT = 1
         and f.VOUCHER_DATE >= currdate
         and f.VOUCHER_DATE < currdate + 1
         and f.TRNS_TYPE_COD in (7, 8)
            
         and v19.deposit_num is null;
  
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v19',
       'violation_19',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       '???? ?????? ????? 19');
    commit;
  end;

  --------------------deposit_violation_20
  procedure ins_dep_v20(currdate date) is
  begin
  
    OperationStart := sysdate;
  
    insert /*+noappend*/
    into deposit_violation_20
    /*  select \*+parallel(4)*\
    seq_dep_v20.nextval as violation_id,
    dd.CUSTOMER_NUM,
    dd.DEPOSIT_NUM as deposit_num,
    '0000000000000000' as account_num,
    dd.DEPOSIT_STAT_desc as account_state,
    dd.BRNCH_COD,
    dd.DEPOSIT_CLOSE_DATE,
    null USER_COD,
    dd.DEPOSIT_CLOSE_DATE as effective_date,
    null as user_changed_date,
    1 as user_changed_stat,
    dd.BRNCH_COD,
    dd.USER_COD FRUD_USER_COD,
    dd.USER_BRNCH_COD FRUD_USER_BRNCH,
    dd.DEPOSIT_TYPE,
    dd.DEPOSIT_SERIAL
     from dimdeposit dd
    inner join factdeposittrns t
       on dd.DEPOSIT_KEY = t.DEPOSIT_KEY
     left join deposit_violation_20 v20
       on v20.deposit_num = dd.DEPOSIT_NUM
    where \*dd.DEPOSIT_CLOSE_DATE < sysdate - 3
                        and dd.DEPOSIT_STAT_COD <> 0
                        and dd.ISCURRENT = 1
                        and trunc(dd.DEPOSIT_OPEN_DATE) <> trunc(dd.START_DATE)*\
    dd.DEPOSIT_CLOSE_DATE < t.VOUCHER_DATE - 1
    and t.TRNS_TYPE_COD = 14
    
    and v20.deposit_num is null;*/
      SELECT /*+Parallel(4)*/
       seq_dep_v20.nextval as violation_id,
       F.CUSTOMER_NUM,
       null USER_COD,
       F2.EFFECTIVE_DATE,
       null as user_changed_date,
       1 as user_changed_stat,
       F.DEPOSIT_TYPE,
       F.DEPOSIT_SERIAL,
       F.BRANCH_COD,
       F.BRANCH_COD || '-' || F.DEPOSIT_TYPE || '-' || F.CUSTOMER_NUM || '-' ||
       F.DEPOSIT_SERIAL,
       null as comment_f
        FROM (SELECT FD.BRANCH_COD,
                     FD.DEPOSIT_TYPE,
                     FD.CUSTOMER_NUM,
                     FD.DEPOSIT_SERIAL
                FROM FACTDEPOSIT FD
               WHERE FD.EFFECTIVE_DATE >= currdate
                 AND FD.EFFECTIVE_DATE < currdate + 1
                 AND FD.DEPOSIT_STATE_COD = 0
               GROUP BY FD.BRANCH_COD,
                        FD.DEPOSIT_TYPE,
                        FD.CUSTOMER_NUM,
                        FD.DEPOSIT_SERIAL) F
       INNER JOIN (SELECT FD.BRANCH_COD,
                          FD.DEPOSIT_TYPE,
                          FD.CUSTOMER_NUM,
                          FD.DEPOSIT_SERIAL,
                          FD.EFFECTIVE_DATE
                     FROM FACTDEPOSIT FD
                    WHERE FD.EFFECTIVE_DATE >= currdate + 1
                      AND FD.EFFECTIVE_DATE < currdate + 2
                      AND FD.DEPOSIT_STATE_COD = 1
                    GROUP BY FD.BRANCH_COD,
                             FD.DEPOSIT_TYPE,
                             FD.CUSTOMER_NUM,
                             FD.DEPOSIT_SERIAL,
                             FD.EFFECTIVE_DATE) F2
          ON F.BRANCH_COD = F2.BRANCH_COD
         AND F.DEPOSIT_TYPE = F2.DEPOSIT_TYPE
         AND F.CUSTOMER_NUM = F2.CUSTOMER_NUM
         AND F.DEPOSIT_SERIAL = F2.DEPOSIT_SERIAL;
  
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v20',
       'violation_20',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       '???? ?????? ?????  20');
    commit;
  end;
  -----------------deposit_violation_21
  procedure ins_dep_v21 is
  begin
  
    OperationStart := sysdate;
  
    insert /*+noappend*/
    into deposit_violation_21
      select /*+parallel(4)*/
       seq_dep_v21.nextval as violation_id,
       BRNCH_COD,
       DEPOSIT_TYPE,
       CUSTOMER_NUM,
       DEPOSIT_SERIAL,
       deposit_num,
       currency_desc,
       deposit_desc,
       Open_date1,
       UserCod1,
       Open_date2,
       UserCod2,
       1,
       null,
       null,
       Effective_date,
       Branch_UserCod1,
       Branch_UserCod2,
       comment_f
        from (select d.BRNCH_COD,
                     d.DEPOSIT_TYPE,
                     d.CUSTOMER_NUM,
                     d.DEPOSIT_SERIAL,
                     d.deposit_num,
                     d.currency_desc,
                     d.deposit_desc,
                     min(d.START_DATE) as Open_date1,
                     max(d.USER_COD) keep(dense_rank first order by d.start_date) as UserCod1,
                     max(d.START_DATE) as Open_date2,
                     max(d.USER_COD) keep(dense_rank last order by d.start_date) as UserCod2,
                     max(d.START_DATE) as effective_Date,
                     max(d.user_brnch_cod) keep(dense_rank first order by d.start_date) as Branch_UserCod1,
                     max(d.user_brnch_cod) keep(dense_rank last order by d.start_date) as Branch_UserCod2,
                     null as comment_f
                from dimdeposit d
                left join deposit_violation_21 v21
                  on v21.brnch_cod = d.brnch_cod
                 and v21.deposit_type = d.deposit_type
                 and v21.customer_num = d.customer_num
                 and v21.deposit_serial = d.deposit_serial
               where d.ISCURRENT = 1
                 and v21.brnch_cod is null
               group by d.BRNCH_COD,
                        d.DEPOSIT_TYPE,
                        d.CUSTOMER_NUM,
                        d.DEPOSIT_SERIAL,
                        d.currency_desc,
                        d.deposit_desc,
                        d.deposit_num
              having count(*) > 1);
  
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v21',
       'violation_21',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       '???? ?????? ????? 21');
    commit;
  end;

  -----------------deposit_violation_22
  procedure ins_dep_v22 is
  begin
  
  --  pkg_utilities.Truncate_Table('deposit_violation_22');
    OperationStart := sysdate;
  
    insert /*+noappend*/
    into deposit_violation_22
      select /*+parallel(4)*/
       seq_dep_v22.nextval as violation_id,
       CUST_NATIONAL_ID,
       cnt_cust,
       
       null,
       null,
       1,
       Effective_date,
       BRANCH_REG,
       customer_num,
       null           as comment_f,
       Cust_Name,
       cust_list,
       branch_list
        from ( /*select nvl(max(dp.BRNCH_COD)
                                                                                               keep(dense_rank last order by dp.DEPOSIT_OPEN_DATE),
                                                                                               0) as BRANCH_REG,
                                                                                           nvl(max(t.CUST_NATIONAL_ID), '-1') CUST_NATIONAL_ID,
                                                                                           max(t.cnt_cust) cnt_cust,
                                                                                           max(t.Effective_date) Effective_date,
                                                                                           max(t.customer_num) as customer_num
                                                                                    
                                                                                      from (select dc.CUST_NATIONAL_ID,
                                                                                                   count(*) as cnt_cust,
                                                                                                   max(dc.CUST_REG_DATE) as Effective_date,
                                                                                                   max(dc.CUSTOMER_NUM) keep(dense_rank last order by dc.CUST_REG_DATE) as customer_num
                                                                                              from dimcustomer dc
                                                                                              left join deposit_violation_22 d22
                                                                                                on d22.cust_book_id = dc.CUST_NATIONAL_ID
                                                                                             where d22.cust_book_id is null
                                                                                               and nidcheck(dc.CUST_NATIONAL_ID) = 1
                                                                                             group by dc.CUST_NATIONAL_ID
                                                                                            having count(*) > 1) t
                                                                                      left join dimdeposit dp
                                                                                        on dp.CUSTOMER_NUM = t.customer_num
                                                                                     group by dp.CUSTOMER_NUM, t.CUST_NATIONAL_ID*/
              select dc.CUST_NATIONAL_ID,
                      count(*) as cnt_cust,
                      max(dc.CUST_REG_DATE) as Effective_date,
                      listagg(dc.CUSTOMER_NUM, ',') within group(order by dc.CUST_REG_DATE) as cust_List,
                      listagg(dc.CUST_BRNCH_CODE, ',') within group(order by dc.CUST_REG_DATE) as Branch_List,
                      max(dc.CUST_BRNCH_CODE) keep(dense_rank last order by dc.CUST_REG_DATE) as BRANCH_REG,
                      min(dc.CUST_FIRST_NAME || '-' || dc.CUST_LAST_NAME) as Cust_Name,
                      max(dc.CUSTOMER_NUM) keep(dense_rank last order by dc.CUST_REG_DATE) as customer_num
                from dimcustomer dc
                left join deposit_violation_22 v22
                on dc.CUST_NATIONAL_ID = v22.cust_book_id
               where  v22.cust_book_id is null
               and dc.CUST_NATIONAL_ID not in
                     ('2222222222',
                      '1111111111',
                      '2249748292',
                      '5555555555',
                      '4444444444',
                      '1234567891',
                      '3333333333',
                      '0000000000',
                      '0123456789',
                      '7777777777',
                      '9999999999',
                      '0')
               group by dc.CUST_NATIONAL_ID
              having count(*) > 1);
  
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v22',
       'violation_22',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       'INSERT DEPOSIT VIOLATION 22');
    commit;
  end;

  -----------------deposit_violation_23
  procedure ins_dep_v23 /*(currdate date)*/
   is
  begin
  
    OperationStart := sysdate;
  
    insert /*+noappend*/
    
    into deposit_violation_23
      select seq_dep_v23.nextval      as violation_Id,
             deposit_brnch_cod,
             deposit_type,
             customer_num,
             deposit_serial,
             CheckBook_Count,
             Effective_date,
             First_CheckBook_Date,
             Last_CheckBook_Date,
             CheckBook_List,
             Last_CheckBook,
             First_CheckBook,
             Last_Cheqbook_UserBranch,
             Last_Cheqbook_UserCode,
             Last_Cheqbook_UserName,
             null                     as user_cod,
             null                     as user_changed_date,
             1                        as User_changed_stat,
             null                     as comment_f
        from (select t.deposit_brnch_cod,
                     t.deposit_type,
                     t.customer_num,
                     t.deposit_serial,
                     count(*) as CheckBook_Count,
                     trunc(t.issue_date) as Effective_date,
                     min(t.issue_date) as First_CheckBook_Date,
                     max(t.issue_date) as Last_CheckBook_Date,
                     listagg(t.chqbook_num, ', ') within group(order by t.issue_date) as CheckBook_List,
                     max(t.chqbook_num) keep(dense_rank last order by t.issue_date) as Last_CheckBook,
                     max(t.chqbook_num) keep(dense_rank first order by t.issue_date) as First_CheckBook,
                     max(t.user_brnch_cod) keep(dense_rank last order by t.issue_date) as Last_Cheqbook_UserBranch,
                     max(t.user_cod) keep(dense_rank last order by t.issue_date) as Last_Cheqbook_UserCode,
                     max(t.user_nam) keep(dense_rank last order by t.issue_date) as Last_Cheqbook_UserName
              
                from DIMCHEQBOOK t
                left join deposit_violation_23 v
                on t.deposit_brnch_cod = v.deposit_brnch_cod
                    and t.deposit_type = v.deposit_type
                    and  t.customer_num = v.customer_num 
                    and  t.deposit_serial = v.deposit_serial
                    and  t.issue_date >= v.effective_date
                    and  t.issue_date < v.effective_date + 1
               where t.chqbook_type = 0
               and v.violation_id is null
               group by t.deposit_brnch_cod,
                        t.deposit_type,
                        t.customer_num,
                        t.deposit_serial,
                        trunc(t.issue_date)
              having count(*) > 1);
  
    /*select \*+parallel(4)*\
    seq_dep_v23.nextval as violation_Id,
    f1.CUSTOMER_NUM,
    f1.BRANCH_COD || '-' || f1.DEPOSIT_TYPE || '-' || f1.CUSTOMER_NUM || '-' ||
    f1.DEPOSIT_SERIAL as Deposit_num,
    f1.CHQBOOK_NUM as CHQBOOK_NUM1,
    dcb1.ISSUE_DATE as ISSUE_DATE1,
    f2.CHQBOOK_NUM as CHQBOOK_NUM2,
    dcb2.ISSUE_DATE as ISSUE_DATE2,
    null as user_cod,
    null as user_changed_date,
    1 as User_changed_stat,
    trunc(dcb2.ISSUE_DATE) as Effective_date,
    f1.BRANCH_COD,
    null as comment_f
     from factcheqbookdaily f1
    inner join dimcheqbook dcb1
       on f1.BRANCH_COD = dcb1.DEPOSIT_BRNCH_COD
      and f1.DEPOSIT_TYPE = dcb1.DEPOSIT_TYPE
      and f1.CUSTOMER_NUM = dcb1.CUSTOMER_NUM
      and f1.DEPOSIT_SERIAL = dcb1.DEPOSIT_SERIAL
      and f1.CHQBOOK_NUM = dcb1.CHQBOOK_NUM
    inner join factcheqbookdaily f2
       on f1.CUSTOMER_NUM = f2.CUSTOMER_NUM
      and f1.BRANCH_COD = f2.BRANCH_COD
      and f1.DEPOSIT_TYPE = f2.DEPOSIT_TYPE
      and f1.DEPOSIT_SERIAL = f2.DEPOSIT_SERIAL
      and f1.EFFECTIVE_DATE = f2.EFFECTIVE_DATE
    inner join dimcheqbook dcb2
       on f2.BRANCH_COD = dcb2.DEPOSIT_BRNCH_COD
      and f2.DEPOSIT_TYPE = dcb2.DEPOSIT_TYPE
      and f2.CUSTOMER_NUM = dcb2.CUSTOMER_NUM
      and f2.DEPOSIT_SERIAL = dcb2.DEPOSIT_SERIAL
      and f2.CHQBOOK_NUM = dcb2.CHQBOOK_NUM
     left join deposit_violation_23 v23
       on v23.customer_num = f2.CUSTOMER_NUM
      and v23.chqbook_num2 = f2.CHQBOOK_NUM
    where dcb1.ISSUE_DATE < dcb2.ISSUE_DATE
      and f1.CHQBOOK_NUM > f2.CHQBOOK_NUM
      and f1.EFFECTIVE_DATE >= currdate
      and f1.EFFECTIVE_DATE < currdate + 1
      and v23.customer_num is null
         -- for limitation of the results
      and dcb1.ISSUE_DATE >= currdate
      and trunc(dcb2.ISSUE_DATE) = currdate;*/
    --   bank customer number
    --and f1.CUSTOMER_NUM <> 10001;
  
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v23',
       'violation_23',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       '???? ?????? ????? 23');
    commit;
  end;

  ------------------------ Deposit_Violation_50
  procedure ins_dep_v50(currdate date) is
  begin
  
    OperationStart := sysdate;
  
    insert /*+noappend*/
    into deposit_violation_50
    /*select SEQ_DEP_V50.NEXTVAL,
            tt.DEPOSIT_KEY,
            tt.BRANCH_COD,
            tt.DEPOSIT_TYPE,
            tt.CUSTOMER_NUM,
            tt.DEPOSIT_SERIAL,
            tt.EFFECTIVE_DATE,
            null                   as DEPOSIT_STATE_COD,
            tt.BALANCE,
            null as WITHDRAW_AMNT,
            null as SETTLE_AMNT,
            tt.PAYED_INTEREST,
            tt.PAYED_AGREED_INTRS,
            tt.DRAWABLE_AMNT,
            tt.FLOOR_AMNT,
            tt.OVER_AMNT,
            tt.MIN_BALANCE,
            tt.MAX_BALANCE,
            tt.BLOCKED_BAL,
            tt.BLCK_BAL_WITH_INTRS,
            tt.PASSIVE_DAYS,
            null  as PASSIVE_DATE,
            null  as SETTELED_PROFIT,
            1     USER_CHANGED_STAT,
            null as USER_CHANGED_DATE,
            null  as USER_COD,
            null  as COMMENT_F
              -----------
     from FACTDEPOSIT tt
    inner join dimdeposittype dpt
       on tt.DEPOSIT_TYPE = dpt.DEPOSIT_TYPE
     left outer join Deposit_Violation_50 vt
       on vt.deposit_key = tt.DEPOSIT_KEY
      and vt.branch_cod = tt.BRANCH_COD
      and vt.deposit_type = tt.DEPOSIT_TYPE
      and vt.deposit_serial = tt.DEPOSIT_SERIAL
      and tt.CUSTOMER_NUM = vt.customer_num
    where vt.deposit_key is null
      and tt.EFFECTIVE_DATE = currdate
      and dpt.DPST_GROUP_COD = 9
      --and t.DEPOSIT_STATE_COD = 1
      and tt.DRAWABLE_AMNT < 0;*/
      select SEQ_DEP_V50.NEXTVAL,
             tt.DEPOSIT_KEY,
             tt.BRANCH_COD,
             tt.DEPOSIT_TYPE,
             tt.CUSTOMER_NUM,
             tt.DEPOSIT_SERIAL,
             tt.EFFECTIVE_DATE,
             tt.DEPOSIT_STATE_COD,
             tt.BALANCE,
             tt.WITHDRAW_AMNT       as WITHDRAW_AMNT,
             tt.SETTLE_AMNT         as SETTLE_AMNT,
             tt.PAYED_INTEREST,
             tt.PAYED_AGREED_INTRS,
             tt.DRAWABLE_AMNT,
             tt.FLOOR_AMNT,
             tt.OVER_AMNT,
             tt.MIN_BALANCE,
             tt.MAX_BALANCE,
             tt.BLOCKED_BAL,
             tt.BLCK_BAL_WITH_INTRS,
             tt.PASSIVE_DAYS,
             tt.PASSIVE_DATE        as PASSIVE_DATE,
             tt.SETTELED_PROFIT     as SETTELED_PROFIT,
             1                      USER_CHANGED_STAT,
             null                   as USER_CHANGED_DATE,
             null                   as USER_COD,
             null                   as COMMENT_F,
             d.DEPOSIT_OPEN_DATE
      -----------
        from FACTDEPOSIT tt
       inner join dimdeposittype dpt
          on tt.DEPOSIT_TYPE = dpt.DEPOSIT_TYPE
       inner join dimdeposit d
          on tt.BRANCH_COD = d.BRNCH_COD
         and tt.DEPOSIT_TYPE = d.DEPOSIT_TYPE
         and tt.CUSTOMER_NUM = d.CUSTOMER_NUM
         and tt.DEPOSIT_SERIAL = d.DEPOSIT_SERIAL
        left outer join Deposit_Violation_50 vt
          on vt.deposit_key = tt.DEPOSIT_KEY
         and vt.branch_cod = tt.BRANCH_COD
         and vt.deposit_type = tt.DEPOSIT_TYPE
         and vt.deposit_serial = tt.DEPOSIT_SERIAL
         and tt.CUSTOMER_NUM = vt.customer_num
       where vt.deposit_key is null
         and tt.EFFECTIVE_DATE = currdate
         and dpt.DPST_GROUP_COD in (6, 8, 9)
         and d.ISCURRENT = 1
            --and t.DEPOSIT_STATE_COD = 1
         and tt.BALANCE < tt.BLCK_BAL_WITH_INTRS;
  
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v50',
       'violation_50',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       '???? ?????? ????? 23');
    commit;
  end;
  ---------------------------
  procedure ins_dep_v38(currdate date) is
  begin
    OperationStart := sysdate;
    INSERT INTO DEPOSIT_VIOLATION_38
      SELECT D.CUST_FIRST_NAME || '-' || D.CUST_LAST_NAME AS CUSTNAM,
             D.CUSTOMER_NUM,
             D.CUST_FATHER_NAME,
             D.CUST_BOOK_ID,
             D.CUST_NATIONAL_ID,
             NIDCHECK2(D.CUST_NATIONAL_ID),
             D.CUST_REG_DATE,
             D.CUST_BIRTH_DATE,
             NULL,
             NULL,
             1,
             NULL,
             seq_dep_v38.nextval as violation_id
        FROM DIMCUSTOMER D
        LEFT JOIN DEPOSIT_VIOLATION_38 V38
          ON V38.CUSTOMER_NUM = D.CUSTOMER_NUM
       WHERE NIDCHECK2(D.CUST_NATIONAL_ID) <> 1
         AND D.CUST_TYPE_COD = 0
         AND D.NATIONALITY_COD = 1
         AND V38.CUSTOMER_NUM IS NULL
         and d.CUST_REG_DATE >= currdate
         and d.CUST_REG_DATE < currdate + 1;
  
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v38',
       'violation_38',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       'ÃœÊ·  Œ·›«  ”Å—œÂ 38˜—œÂ');
    commit;
  end;
  ---------------------------

  procedure ins_dep_v39(currdate date) is
  begin
    OperationStart := sysdate;
    insert into deposit_violation_39
      select t.CUST_NATIONAL_ID,
             t.numberofnationalid,
             seq_dep_v39.nextval as violation_id,
             null,
             null,
             1,
             null,
             t.effdate
      
        from (select dc.CUST_NATIONAL_ID,
                     count(*) as numberofnationalid,
                     max(dc.CUST_REG_DATE) as effdate
                from dimcustomer dc
               where dc.CUST_NATIONAL_ID not in
                    
                     ('1111111111',
                      '0123456789',
                      '9876543210',
                      '0000000000',
                      '2222222222',
                      '3333333333',
                      '4444444444',
                      '5555555555',
                      '6666666666',
                      '7777777777',
                      '8888888888',
                      '9999999999')
                    
                 and dc.CUST_TYPE_COD = 0
                 and dc.NATIONALITY_COD = 1
                 and dc.CUST_REG_DATE >= currdate
                 and dc.CUST_REG_DATE < currdate + 1
               group by dc.CUST_NATIONAL_ID
              having count(*) > 1) t
        left join deposit_violation_39 v39
          on v39.CUST_NATIONAL_ID = t.CUST_NATIONAL_ID
       where v39.CUST_NATIONAL_ID is null;
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v32',
       'violation_32',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       'ÃœÊ·  Œ·›«  ”Å—œÂ 32');
    commit;
  end;
  -----------------------------------------
  procedure ins_dep_v30 is
  begin
    OperationStart := sysdate;
    INSERT INTO DEPOSIT_VIOLATION_30
      (violation_id,
       CHECKID,
       CUSTOMER_NUM,
       CUST_FIRST_NAME,
       CUST_LAST_NAME,
       CUST_FATHER_NAME,
       CUST_BOOK_ID,
       CUST_PASSPORT_NO,
       CUST_NATIONAL_ID,
       NATIONALITY_COD,
       NATIONALITY_DESC,
       CUST_TYPE_COD,
       CUST_TYPE_DESC,
       CUST_JOB,
       CUST_MOBILE_NO,
       SEX_COD,
       CUST_IS_MARRIED,
       CUST_BIRTH_DATE,
       CUST_REG_DATE,
       CUST_ISSUED_LOCAL,
       user_changed_date,
       user_changed_stat,
       user_cod,
       comment_f,
       BRNCH_COD,
       USER_CODE,
       CUST_ENABLE,
       CUST_ENABLE_DESC,
       Has_Deposit)
    
      select seq_dep_v30.nextval as violation_id,
             checkid,
             CUSTOMER_NUM,
             CUST_FIRST_NAME,
             CUST_LAST_NAME,
             CUST_FATHER_NAME,
             CUST_BOOK_ID,
             CUST_PASSPORT_NO,
             CUST_NATIONAL_ID,
             NATIONALITY_COD,
             NATIONALITY_DESC,
             CUST_TYPE_COD,
             CUST_TYPE_DESC,
             CUST_JOB,
             CUST_MOBILE_NO,
             SEX_COD,
             CUST_IS_MARRIED,
             CUST_BIRTH_DATE,
             CUST_REG_DATE,
             CUST_ISSUED_LOCAL,
             null                as user_changed_date,
             1                   as user_changed_stat,
             null                as user_cod,
             null                as comment_f,
             CUST_BRNCH_CODE,
             CUST_USER_COD,
             CUST_ENABLE,
             CUST_ENABLE_DESC,
             Has_Deposit
      
        from (select distinct nidcheck(d.CUST_NATIONAL_ID) as checkid,
                              d.CUSTOMER_NUM,
                              d.CUST_FIRST_NAME,
                              d.CUST_LAST_NAME,
                              d.CUST_FATHER_NAME,
                              d.CUST_BOOK_ID,
                              d.CUST_PASSPORT_NO,
                              d.CUST_NATIONAL_ID,
                              d.NATIONALITY_COD,
                              d.NATIONALITY_DESC,
                              d.CUST_TYPE_COD,
                              d.CUST_TYPE_DESC,
                              d.CUST_JOB,
                              d.CUST_MOBILE_NO,
                              d.SEX_COD,
                              d.CUST_IS_MARRIED,
                              d.CUST_BIRTH_DATE,
                              d.CUST_REG_DATE,
                              d.CUST_ISSUED_LOCAL,
                              d.CUST_BRNCH_CODE,
                              d.CUST_USER_COD,
                              d.CUST_ENABLE,
                              d.CUST_ENABLE_DESC,
                              case
                                when f.DEPOSIT_KEY is null then
                                 '‰œ«—œ'
                                else
                                 'œ«—œ'
                              end as Has_Deposit
              
                from dimcustomer d
                left join dimdeposit f
                  on d.CUSTOMER_NUM = f.CUSTOMER_NUM
                left join deposit_violation_30 v30
                  on v30.customer_num = d.CUSTOMER_NUM
               where NIDCHECK(d.CUST_NATIONAL_ID) <> 1
                 and d.cust_type_cod = 0
                 and d.NATIONALITY_COD = 1
                 and v30.customer_num is null
              
              union
              
              select distinct '4' as checkid,
                              d.CUSTOMER_NUM,
                              d.CUST_FIRST_NAME,
                              d.CUST_LAST_NAME,
                              d.CUST_FATHER_NAME,
                              d.CUST_BOOK_ID,
                              d.CUST_PASSPORT_NO,
                              d.CUST_NATIONAL_ID,
                              d.NATIONALITY_COD,
                              d.NATIONALITY_DESC,
                              d.CUST_TYPE_COD,
                              d.CUST_TYPE_DESC,
                              d.CUST_JOB,
                              d.CUST_MOBILE_NO,
                              d.SEX_COD,
                              d.CUST_IS_MARRIED,
                              d.CUST_BIRTH_DATE,
                              d.CUST_REG_DATE,
                              d.CUST_ISSUED_LOCAL,
                              d.CUST_BRNCH_CODE,
                              d.CUST_USER_COD,
                              d.CUST_ENABLE,
                              d.CUST_ENABLE_DESC,
                              case
                                when f.DEPOSIT_KEY is null then
                                 '‰œ«—œ'
                                else
                                 'œ«—œ'
                              end as Has_Deposit
              
                from dimcustomer d
                left join dimdeposit f
                  on d.CUSTOMER_NUM = f.CUSTOMER_NUM
                left join deposit_violation_30 v30
                  on v30.customer_num = d.CUSTOMER_NUM
               where d.cust_type_cod = 1
                 and (d.CUST_BIRTH_DATE > to_date('02011970', 'ddmmyyyy') or
                     d.CUST_BIRTH_DATE < to_date('31121969', 'ddmmyyyy')) -- neshan dahandeye sherkate dar shorofe tasis
                 and d.CUST_CORP_ID is null
                 and v30.customer_num is null
              
              );
  
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v30',
       'violation_30',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       'INSERT DEPOSIT VIOLATION 30');
    commit;
  end;

  -----------------deposit_violation_40
  procedure ins_dep_v40 is
  begin
    OperationStart := sysdate;
  
    insert into deposit_violation_40
    
      select seq_dep_v40.nextval  as violation_id,
             f.LAST_CUSTOMER_NUM,
             f.CUST_FIRST_NAME,
             f.CUST_LAST_NAME,
             f.CUST_FATHER_NAME,
             f.CUST_BOOK_ID,
             f.CUST_BIRTH_DATE,
             f.CUST_REG_DATE,
             f.mycount,
             null                 as usercode,
             null                 as nuuserchangeddate,
             1                    as userchangedstatus,
             null                 as comment_f,
             f.FIRST_CUSTOMER_NUM,
             f.FIRSTREGDATE,
             f.FIRST_BRANCH_COD,
             f.Last_BRANCH_COD,
             f.cust_List,
             f.FIRST_USER_COD,
             f.Last_USER_COD
      
        from (select count(*) as mycount,
                     d.CUST_FIRST_NAME,
                     d.CUST_LAST_NAME,
                     d.CUST_FATHER_NAME,
                     d.CUST_BOOK_ID,
                     d.CUST_BIRTH_DATE,
                     max(trunc(d.CUST_REG_DATE)) as CUST_REG_DATE,
                     min(trunc(d.CUST_REG_DATE)) as FIRSTREGDATE,
                     max(d.CUSTOMER_NUM) keep(dense_rank last order by d.CUST_REG_DATE) as LAST_CUSTOMER_NUM,
                     max(d.CUSTOMER_NUM) keep(dense_rank First order by d.CUST_REG_DATE) as FIRST_CUSTOMER_NUM,
                     listagg(d.CUSTOMER_NUM, ',') within group(order by d.CUST_REG_DATE) as cust_List,
                     max(d.CUST_BRNCH_CODE) keep(dense_rank First order by d.CUST_REG_DATE) as FIRST_BRANCH_COD,
                     max(d.CUST_BRNCH_CODE) keep(dense_rank Last order by d.CUST_REG_DATE) as Last_BRANCH_COD,
                     max(d.CUST_USER_COD) keep(dense_rank last order by d.CUST_REG_DATE) as LAST_USER_COD,
                     max(d.CUST_USER_COD) keep(dense_rank First order by d.CUST_REG_DATE) as FIRST_USER_COD
                from dimcustomer d
               where d.CUST_TYPE_COD = 0
               group by d.CUST_FIRST_NAME,
                        d.CUST_LAST_NAME,
                        d.CUST_FATHER_NAME,
                        d.CUST_BOOK_ID,
                        d.CUST_BIRTH_DATE
              having count(*) >= 2) f
      
        left join deposit_violation_40 v40
          on v40.lastcustnum = f.LAST_CUSTOMER_NUM
         and v40.mycount = f.mycount
       where v40.lastcustnum is null;
  
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v40',
       'violation_40',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       'INSERT DEPOSIT VIOLATION 40');
    commit;
  end;

  -------------------------------------------------------

  procedure ins_dep_v42 is
  begin
    OperationStart := sysdate;
  
    insert into deposit_violation_42
      select seq_dep_v42.nextval as violation_id,
             BRNCH_COD,
             DEPOSIT_TYPE,
             CUSTOMER_NUM,
             DEPOSIT_SERIAL,
             DEPOSIT_NUM,
             DEPOSIT_TITLE,
             DEPOSIT_OPEN_DATE,
             USER_BRNCH_COD,
             FRAUD_USER_COD,
             SHEET_NUM,
             null                as user_cod,
             null                as user_changed_date,
             1                   as user_changed_stat,
             null                as comment_f,
             DEPOSIT_STAT_COD,
             DEPOSIT_STAT_DESC,
             USER_NAME,
             CUST_TYPE_COD,
             CUST_TYPE_DESC,
             SHEET_DATE,
             SHEET_USER_CODE
        from (select t.BRNCH_COD,
                     t.DEPOSIT_TYPE,
                     t.CUSTOMER_NUM,
                     t.DEPOSIT_SERIAL,
                     t.DEPOSIT_NUM,
                     t.DEPOSIT_TITLE,
                     t.DEPOSIT_OPEN_DATE,
                     t.USER_BRNCH_COD,
                     t.USER_COD as FRAUD_USER_COD,
                     nvl(td.Tdsheetsrl, t.SHEET_NUM) as SHEET_NUM,
                     t.DEPOSIT_STAT_COD,
                     t.DEPOSIT_STAT_DESC,
                     d.USER_NAME,
                     c.CUST_TYPE_COD,
                     c.CUST_TYPE_DESC,
                     td.tdsheetdt as SHEET_DATE,
                     td.abrnchcod || '-' || td.ausrcode as SHEET_USER_CODE
                from dimdeposit t
               inner join dimcustomer c
                  on t.CUSTOMER_NUM = c.CUSTOMER_NUM
                left join sa_negin_iranzamin.TCDPSTSHEET td
                  on t.BRNCH_COD = td.tcd_abrnchcod
                 and t.DEPOSIT_TYPE = td.tbdptype
                 and t.CUSTOMER_NUM = td.cfcifno
                 and t.DEPOSIT_SERIAL = td.tdserial
                left join deposit_violation_42 v42
                  on t.BRNCH_COD = v42.brnch_cod
                 and t.DEPOSIT_TYPE = v42.deposit_type
                 and t.CUSTOMER_NUM = v42.customer_num
                 and t.DEPOSIT_SERIAL = v42.deposit_serial
                 and (t.SHEET_NUM = v42.sheet_num or td.tdsheetsrl = v42.sheet_num)
                 and td.tdsheetdt = v42.sheet_date
               inner join dimuser d
                  on td.abrnchcod = d.BRNCH_COD
                 and td.ausrcode = d.USER_COD
               where t.DPST_GROUP_COD not in (6, 9)
                 and t.SHEET_NUM is not null
                 and t.ISCURRENT = 1
                 and t.SHEET_NUM <> 0
                 and v42.brnch_cod is null);
               /*group by t.BRNCH_COD,
                        t.DEPOSIT_TYPE,
                        t.CUSTOMER_NUM,
                        t.DEPOSIT_SERIAL,
                        t.DEPOSIT_NUM,
                        t.DEPOSIT_TITLE,
                        t.DEPOSIT_OPEN_DATE,
                        t.USER_BRNCH_COD,
                        t.USER_COD,
                        t.SHEET_NUM,
                        t.DEPOSIT_STAT_COD,
                        t.DEPOSIT_STAT_DESC,
                        c.CUST_TYPE_COD,
                        c.CUST_TYPE_DESC);  */
  
    rowcnt := sql%rowcount;
    commit;
  
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_deposit_v42',
       'violation_42',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       trunc(sysdate),
       'INSERT DEPOSIT VIOLATION 42');
    commit;
  end;

  --------------------deposit_violation_main
  procedure ins_main is
  begin
    --currdate := trunc(sysdate) - 1;
  
    /* fromdate := to_date('1/7/2014', 'mm/dd/yyyy');
    todate   := to_date('2/1/2014', 'mm/dd/yyyy') ;*/
  
    select trunc(max(f.VOUCHER_DATE)) into todate from factdeposittrns f;
    select max(a.effective_date)
      into fromdate
      from deposit_violation_all a
     where a.report_id not in ('deposit_violation_42',
                               'deposit_violation_21',
                               'deposit_violation_13',
                               'deposit_violation_4',
                               'deposit_violation_22',
                               'deposit_violation_23',
                               'deposit_violation_30',
                               'deposit_violation_40',
                               'deposit_violation_6',
                               'deposit_violation_7');
    currdate := fromdate;
    currdate := currdate + 1;
    if (currdate <= todate) then
      while (currdate <= todate) loop
        ins_dep_v1(currdate);
        ins_dep_v8(currdate);
        ins_dep_v11(currdate);
        ins_dep_v12(currdate);
        ins_dep_v14(currdate);
        ins_dep_v19(currdate);
        ins_dep_v20(currdate);
        ins_dep_v50(currdate);
        ins_dep_v5(currdate);
      
        currdate := currdate + 1;
      end loop;
      ins_dep_v42;
      ins_dep_v21;
      ins_dep_v13;
      ins_dep_v4;
      ins_dep_v22;
      ins_dep_v23;
      ins_dep_v30;
      ins_dep_v40;
      ins_dep_v6;
      ins_dep_v7;
    
    end if;
  end;

begin

  currdate := trunc(sysdate) - 1;
  t        := 1;

end pkg_deposit_violation;
/

prompt
prompt Creating package body PKG_FINANCIAL_VIOLATION
prompt =============================================
prompt
CREATE OR REPLACE PACKAGE BODY FRAUD2DM_IRANZAMIN."PKG_FINANCIAL_VIOLATION" is
  t              number;
  currdate       date;
  rowcnt         number;
  OperationStart Date;
  OperationEnd   Date;
  max_date       Date;

  --------------------------proc v1
  procedure ins_fin_v1(currdate date) is
  begin
    OperationStart := sysdate;
    insert /*+noappend*/
    into financial_violation_1
      select /*+Parallel(4)*/
       seq_fin_v1.nextval as violation_id,
       TRNS_NUM,
       REMITTANCE_REF,
       FIRST_BRCNH_COD,
       VOUCHER_DATE,
       fuser_branch_cod,
       Fuser_Cod,
       TRNS_TYPE_DESC,
       REVERSE_COD,
       AMOUNT,
       effective_date,
       null,
       null,
       1,
       null,
       AAPPCOD,
       ATRCODE,
       ATRSBCODE
        from (select f.TRNS_NUM,
                     /* case
                       when f.IS_INTERNAL = 0 then
                        f.REMITTANCE_REF
                       else
                        null
                     end REMITTANCE_REF,*/
                     f.REMITTANCE_REF,
                     f.FIRST_BRCNH_COD,
                     f.VOUCHER_DATE,
                     f.USER_BRNCH_COD   as fuser_branch_cod,
                     f.USER_COD         as Fuser_Cod,
                     typ.TRNS_TYPE_DESC,
                     f.REVERSE_COD      as REVERSE_COD,
                     f.AMOUNT,
                     f.VOUCHER_DATE     as effective_date,
                     f.AAPPCOD,
                     f.ATRCODE,
                     f.ATRSBCODE
              
                from facttransaction f
               inner join dimtrnstype typ
                  on f.AAPPCOD = typ.AAPPCOD
                 and f.ATRCODE = typ.ATRCODE
                 and f.ATRSBCODE = typ.ATRSBCODE
               inner join dimtrnspattern p
                  on p.TRNS_PATTERN_ID = f.TRNS_PATTERN_ID
               where ((p.BEDSIDE_ALL_LEVEL like '%|2100|%' and
                     (p.BESSIDE_ALL_LEVEL like '%|3200|%' or
                     p.BESSIDE_ALL_LEVEL like '%|3405|%' or
                     p.BESSIDE_ALL_LEVEL like '%|3406|%' or
                     p.BESSIDE_ALL_LEVEL like '%|3201|%') and
                     p.BESSIDE_ALL_LEVEL not like '%|3240|%' and
                     p.BESSIDE_ALL_LEVEL not like '%|3224|%' and
                     p.BESSIDE_ALL_LEVEL not like '%|3448|%') or
                     (p.BESSIDE_ALL_LEVEL like '%|2100|%' and
                     (p.BEDSIDE_ALL_LEVEL like '%|3200|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|3405|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|3406|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|3201|%') and
                     p.BEDSIDE_ALL_LEVEL not like '%|3240|%' and
                     p.BEDSIDE_ALL_LEVEL not like '%|3224|%' and
                     p.BEDSIDE_ALL_LEVEL not like '%|3448|%'))
                 and f.VOUCHER_DATE = currdate
              /*   and f.TRNS_PATTERN_ID not in
              (select tp.TRNS_PATTERN_ID
                 from dimtrnspattern tp
                where tp.BESSIDE_PATTERN like '%|76|%'
                   or tp.BESSIDE_PATTERN like '%|86|%'
                   or tp.BESSIDE_PATTERN like '%|1155|%'
                   or tp.BESSIDE_PATTERN like '%|1166|%'
                   or tp.BESSIDE_PATTERN like '%|1196|%'
                   or tp.BESSIDE_PATTERN like '%|1191|%'
                   or tp.BESSIDE_PATTERN like '%|1246|%')*/
              );
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_fin_v1',
       'financial_violation_1',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'ÃœÊ·  Œ·›«  „«·Ì 1');
    commit;
  end;
  ------------------------------proc v2

  procedure ins_fin_v2(currdate date) is
  begin
    OperationStart := sysdate;
    insert /*+noappend*/
    into financial_violation_2
      select /*+Parallel(4)*/
       seq_fin_v1.nextval as violation_id,
       ACCOUNT_NUM,
       ACCOUNT_TITLE,
       BAL_BED,
       BAL_BES,
       ACCOUNT_NATURE,
       EFFECTIVE_DATE,
       null,
       null,
       1,
       null
        from (select a.ACCOUNT_NUM,
                     a.ACCOUNT_TITLE,
                     f.BAL_BED,
                     f.BAL_BES,
                     a.ACCOUNT_NATURE,
                     f.EFFECTIVE_DATE
                from factaccount f
               inner join dimaccount a
                  on f.BRANCH_COD = a.BRANCH_COD
                 and f.ACNT_TYPE_COD = a.ACCOUNT_TYPE_COD
                 and f.ACNT_SERIAL = a.ACCOUNT_SERIAL
               where ((a.ACCOUNT_NATURE = 1 and f.BAL_BED <> 0) or
                     (a.ACCOUNT_NATURE = 0 and f.BAL_BES <> 0))
                 and f.EFFECTIVE_DATE = currdate
               group by a.ACCOUNT_NUM,
                        a.ACCOUNT_TITLE,
                        f.BAL_BED,
                        f.BAL_BES,
                        a.ACCOUNT_NATURE,
                        f.EFFECTIVE_DATE);
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_fin_v2',
       'financial_violation_2',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'ÃœÊ·  Œ·›«  „«·Ì 2');
    commit;
  end;
  ------------------------------------proc v3
  /*procedure ins_v3(currdate date) is
  begin
    OperationStart := sysdate;
    insert \*+noappend*\
    into financial_violation_3
      select \*+Parallel(4)*\
       seq_v1.nextval as violation_id,
       TRNS_NUM,
       REMITTANCE_REF,
       FIRST_BRCNH_COD,
       VOUCHER_DATE,
       fuser_branch_cod,
       Fuser_Cod,
       TRNS_TYPE_DESC,
       REVERSE_COD,
       AMOUNT,
  
       effective_date,
       null,
       null,
       1
        from (
  
              select f.TRNS_NUM,
  
                      case
                        when f.IS_INTERNAL = 1 then
                         f.REMITTANCE_REF
                        else
                         null
                      end REMITTANCE_REF,
  
                      f.FIRST_BRCNH_COD,
                      f.VOUCHER_DATE,
                      f.USER_BRNCH_COD as fuser_branch_cod,
                      f.USER_COD as Fuser_Cod,
                      typ.TRNS_TYPE_DESC,
                      f.REVERSE_COD as REVERSE_COD,
                      f.AMOUNT,
                      trunc(f.VOUCHER_DATE) as effective_date
                from facttransaction f
               inner join dimtrnstype typ
                  on f.AAPPCOD = typ.AAPPCOD
                 and f.ATRCODE = typ.ATRCODE
                 and f.ATRSBCODE = typ.ATRSBCODE
                 inner join dimtrnspattern p
                 on p.TRNS_PATTERN_ID = f.TRNS_PATTERN_ID
               where p.BESSIDE_ALL_LEVEL like '%|31|%'
                 and (p.BEDSIDE_ALL_LEVEL like '%|2630|%' or p.BEDSIDE_ALL_LEVEL like '%|2640|%' or p.BEDSIDE_ALL_LEVEL like '%|2650|%'
                 or p.BEDSIDE_ALL_LEVEL like '%|2660|%')
                     and f.VOUCHER_DATE >= currdate
                 and f.VOUCHER_DATE < currdate + 1
                 and f.REVERSE_COD <> 2
                 and typ.TRNS_TYPE_DESC not in
                 (select tp.TRNS_TYPE_DESC from dimtrnstype tp
                 where (tp.AAPPCOD like 'K'
                 and tp.ATRCODE = 81
                 and tp.ATRSBCODE = 6)
                 or
                 (tp.AAPPCOD like 'K'
                 and tp.ATRCODE = 81
                 and tp.ATRSBCODE = 1)
                 or
                 (tp.AAPPCOD like 'K'
                 and tp.ATRCODE = 2
                 and tp.ATRSBCODE = 32)
                 or
                 (tp.AAPPCOD like 'D'
                 and tp.ATRCODE = 31
                 and tp.ATRSBCODE = 2)
                 or
                 (tp.AAPPCOD like 'q'
                 and tp.ATRCODE = 3
                 and tp.ATRSBCODE = 8)
                 or
                 (tp.AAPPCOD like 'A'
                 and tp.ATRCODE = 127
                 and tp.ATRSBCODE = 1)
                 )
  
              );
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert \*+ append *\
    into operation_Log
    values
      ('ins_v3',
       'financial_violation_3',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'ÃœÊ·  Œ·›«  3');
    commit;
  end;*/

  ---------------------------proc v4
  procedure ins_fin_v4(currdate date) is
  begin
    OperationStart := sysdate;
    insert /*+noappend*/
    into financial_violation_4
      select /*+Parallel(4)*/
       seq_fin_v4.nextval as violation_id,
       TRNS_NUM,
       REMITTANCE_REF,
       FIRST_BRCNH_COD,
       VOUCHER_DATE,
       fuser_branch_cod,
       Fuser_Cod,
       TRNS_TYPE_DESC,
       REVERSE_DESC,
       AMOUNT,
       effective_date,
       null,
       null,
       1,
       null,
       AAPPCOD,
       ATRCODE,
       ATRSBCODE
        from (select f.TRNS_NUM,
                     /*case
                       when f.IS_INTERNAL = 1 then
                        f.REMITTANCE_REF
                       else
                        null
                     end REMITTANCE_REF,*/
                     f.REMITTANCE_REF,
                     f.FIRST_BRCNH_COD,
                     f.VOUCHER_DATE,
                     f.USER_BRNCH_COD   as fuser_branch_cod,
                     f.USER_COD         as Fuser_Cod,
                     typ.TRNS_TYPE_DESC,
                     ts.REVERSE_DESC,
                     f.AMOUNT,
                     f.VOUCHER_DATE     as effective_date,
                     f.AAPPCOD,
                     f.ATRCODE,
                     f.ATRSBCODE
                from facttransaction f
               inner join dimtrnstype typ
                  on f.AAPPCOD = typ.AAPPCOD
                 and f.ATRCODE = typ.ATRCODE
                 and f.ATRSBCODE = typ.ATRSBCODE
               inner join dimtrnspattern p
                  on p.TRNS_PATTERN_ID = f.TRNS_PATTERN_ID
               inner join dimtrnsstat ts
                  on f.REVERSE_COD = ts.REVERSE_COD
               where (((p.BEDSIDE_ALL_LEVEL like '%|5500|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|5501|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|9201|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|5518|%') and
                     p.BESSIDE_ALL_LEVEL like '%|51|%'))
                 and f.VOUCHER_DATE = currdate
                 and ((f.AAPPCOD <> 'L' or f.ATRCODE <> 20 or
                     f.ATRSBCODE not in (2, 1, 79, 83, 89, 93)) and
                     (f.AAPPCOD <> 'L' or f.ATRCODE <> 32 or
                     f.ATRSBCODE <> 2) and
                     (f.AAPPCOD <> 'L' or f.ATRCODE <> 48 or
                     f.ATRSBCODE not in (2, 3, 4)) and
                     (f.AAPPCOD <> 'L' or f.ATRCODE <> 14 or
                     f.ATRSBCODE <> 2) and
                     (f.AAPPCOD <> '2' or f.ATRCODE <> 2 or
                     f.ATRSBCODE not in (1, 6, 12)) and
                     (f.AAPPCOD <> '6' or f.ATRCODE <> 61 or
                     f.ATRSBCODE <> 1) and
                     (f.AAPPCOD <> '6' or f.ATRCODE <> 93 or
                     f.ATRSBCODE <> 5)));
    /* and f.TRNS_NUM not in
    (select f.TRNS_NUM
       from facttransaction f
      where f.AAPPCOD <> 'L'
        and f.ATRCODE = 20
        and f.ATRSBCODE = 2
        and f.VOUCHER_DATE = currdate));*/
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_fin_v4',
       'financial_violation_4',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'ÃœÊ·  Œ·›«  „«·Ì 4');
    commit;
  end;

  ---------------------------------proc v5

  procedure ins_fin_v5(currdate date) is
  begin
    OperationStart := sysdate;
    insert /*+noappend*/
    into financial_violation_5
      select /*+Parallel(4)*/
       seq_fin_v5.nextval as violation_id,
       TRNS_NUM,
       REMITTANCE_REF,
       FIRST_BRCNH_COD,
       VOUCHER_DATE,
       fuser_branch_cod,
       Fuser_Cod,
       TRNS_TYPE_DESC,
       REVERSE_COD,
       AMOUNT,
       effective_date,
       null,
       null,
       1,
       null,
       AAPPCOD,
       ATRCODE,
       ATRSBCODE
        from (select f.TRNS_NUM,
                     /* case
                       when f.IS_INTERNAL = 1 then
                        f.REMITTANCE_REF
                       else
                        null
                     end REMITTANCE_REF,*/
                     f.REMITTANCE_REF,
                     f.FIRST_BRCNH_COD,
                     f.VOUCHER_DATE,
                     f.USER_BRNCH_COD   as fuser_branch_cod,
                     f.USER_COD         as Fuser_Cod,
                     typ.TRNS_TYPE_DESC,
                     f.REVERSE_COD      as REVERSE_COD,
                     f.AMOUNT,
                     f.VOUCHER_DATE     as effective_date,
                     f.AAPPCOD,
                     f.ATRCODE,
                     f.ATRSBCODE
                from facttransaction f
               inner join dimtrnspattern p
                  on p.TRNS_PATTERN_ID = f.TRNS_PATTERN_ID
               inner join dimtrnstype typ
                  on f.AAPPCOD = typ.AAPPCOD
                 and f.ATRCODE = typ.ATRCODE
                 and f.ATRSBCODE = typ.ATRSBCODE
               where (p.BEDSIDE_ALL_LEVEL like '%|5509|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|5510|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|5511|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|5512|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|4571|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|5429|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|948|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|962|%')
                 and (f.AAPPCOD <> 'A' or f.ATRCODE not in (16, 137, 138))
                 and f.VOUCHER_DATE = currdate
                 and f.REVERSE_COD <> 2);
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_fin_v5',
       'financial_violation_5',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'ÃœÊ·  Œ·›«  „«·Ì 5');
    commit;
  end;
  -------------------------------------- Proc V6
  procedure ins_fin_v6(currdate date) is
  begin
    OperationStart := sysdate;
    insert /*+noappend*/
    into financial_violation_6
      select /*+Parallel(4)*/
       seq_fin_v6.nextval as violation_id,
       TRNS_NUM,
       REMITTANCE_REF,
       FIRST_BRCNH_COD,
       VOUCHER_DATE,
       fuser_branch_cod,
       Fuser_Cod,
       TRNS_TYPE_DESC,
       REVERSE_COD,
       AMOUNT,
       effective_date,
       null,
       null,
       1,
       null,
       AAPPCOD,
       ATRCODE,
       ATRSBCODE
        from (select f.TRNS_NUM,
                     /*case
                       when f.IS_INTERNAL = 1 then
                        f.REMITTANCE_REF
                       else
                        null
                     end REMITTANCE_REF,*/
                     f.REMITTANCE_REF,
                     f.FIRST_BRCNH_COD,
                     f.VOUCHER_DATE,
                     f.USER_BRNCH_COD   as fuser_branch_cod,
                     f.USER_COD         as Fuser_Cod,
                     typ.TRNS_TYPE_DESC,
                     f.REVERSE_COD,
                     f.AMOUNT,
                     f.VOUCHER_DATE     as effective_date,
                     f.AAPPCOD,
                     f.ATRCODE,
                     f.ATRSBCODE
                from facttransaction f
               inner join dimtrnspattern p
                  on p.TRNS_PATTERN_ID = f.TRNS_PATTERN_ID
               inner join dimtrnstype typ
                  on f.AAPPCOD = typ.AAPPCOD
                 and f.ATRCODE = typ.ATRCODE
                 and f.ATRSBCODE = typ.ATRSBCODE
               where (p.BEDSIDE_ALL_LEVEL like '%|5506|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|5507|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|5508|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|5485|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|5487|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|5488|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|5495|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|5496|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|5497|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|5498|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|5499|%')
                 and (f.AAPPCOD <> 'A' or f.ATRCODE not in (16, 137, 138))
                 and f.VOUCHER_DATE = currdate
                 and f.REVERSE_COD <> 2);
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_fin_v6',
       'financial_violation_6',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'ÃœÊ·  Œ·›«  „«·Ì 6');
    commit;
  end;

  ------------------------------------- Proc V8
  procedure ins_fin_v8(currdate date) is
  begin
    OperationStart := sysdate;
    insert /*+noappend*/
    into financial_violation_8
      select /*+Parallel(4)*/
       seq_fin_v8.nextval as violation_id,
       TRNS_NUM,
       REMITTANCE_REF,
       FIRST_BRCNH_COD,
       VOUCHER_DATE,
       fuser_branch_cod,
       Fuser_Cod,
       TRNS_TYPE_DESC,
       REVERSE_COD,
       AMOUNT,
       effective_date,
       null,
       null,
       1,
       null,
       AAPPCOD,
       ATRCODE,
       ATRSBCODE
        from (select f.TRNS_NUM,
                     /*case
                       when f.IS_INTERNAL = 1 then
                        f.REMITTANCE_REF
                       else
                        null
                     end REMITTANCE_REF,*/
                     f.REMITTANCE_REF,
                     f.FIRST_BRCNH_COD,
                     f.VOUCHER_DATE,
                     f.USER_BRNCH_COD   as fuser_branch_cod,
                     f.USER_COD         as Fuser_Cod,
                     typ.TRNS_TYPE_DESC,
                     f.REVERSE_COD,
                     f.AMOUNT,
                     f.VOUCHER_DATE     as effective_date,
                     f.AAPPCOD,
                     f.ATRCODE,
                     f.ATRSBCODE
                from facttransaction f
               inner join dimtrnspattern p
                  on p.TRNS_PATTERN_ID = f.TRNS_PATTERN_ID
               inner join dimtrnstype typ
                  on f.AAPPCOD = typ.AAPPCOD
                 and f.ATRCODE = typ.ATRCODE
                 and f.ATRSBCODE = typ.ATRSBCODE
               where (p.BEDSIDE_ALL_LEVEL like '%|5513|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|5472|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|5514|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|5525|%' or
                     p.BEDSIDE_ALL_LEVEL like '%|949|%')
                    --sanade ekhtetamie
                 and (f.AAPPCOD <> 'A' or f.ATRCODE not in (16, 137, 138))
                 and f.VOUCHER_DATE = currdate
                 and f.REVERSE_COD <> 2);
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_fin_v8',
       'financial_violation_8',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'ÃœÊ·  Œ·›«  „«·Ì 8');
    commit;
  end;

  ---------------------------------- Proc V10
  procedure ins_fin_v10(currdate date) is
  begin
    OperationStart := sysdate;
    insert /*+noappend*/
    into financial_violation_10
      select /*+Parallel(4)*/
       seq_fin_v10.nextval as violation_id,
       TRNS_NUM,
       REMITTANCE_REF,
       FIRST_BRCNH_COD,
       VOUCHER_DATE,
       fuser_branch_cod,
       Fuser_Cod,
       TRNS_TYPE_DESC,
       REVERSE_DESC,
       AMOUNT,
       effective_date,
       null,
       null,
       1,
       null
        from (select ft.TRNS_NUM,
                     /* case
                       when ft.IS_INTERNAL = 1 then
                        ft.REMITTANCE_REF
                       else
                        null
                     end REMITTANCE_REF,*/
                     ft.REMITTANCE_REF,
                     ft.FIRST_BRCNH_COD,
                     ft.VOUCHER_DATE,
                     ft.USER_BRNCH_COD  as fuser_branch_cod,
                     ft.USER_COD        as Fuser_Cod,
                     typ.TRNS_TYPE_DESC,
                     ds.REVERSE_DESC,
                     ft.AMOUNT,
                     ft.VOUCHER_DATE    as effective_date
                from facttransaction ft
               inner join dimtrnstype typ
                  on ft.AAPPCOD = typ.AAPPCOD
                 and ft.ATRCODE = typ.ATRCODE
                 and ft.ATRSBCODE = typ.ATRSBCODE
               inner join dimuser u
                  on ft.USER_COD = u.USER_COD
                 and ft.USER_BRNCH_COD = u.BRNCH_COD
               inner join dimtrnsstat ds
                  on ds.REVERSE_COD = ft.REVERSE_COD
               where ft.USER_BRNCH_COD <> ft.FIRST_BRCNH_COD
                 and ft.VOUCHER_DATE = currdate
                 and u.USER_POST not in (5)
                    /*and u.USER_POST not in
                     ( \*200, 1, 1,*\2,
                      5 \*, 6, 10, 13, 14, 15, 20, 21, 75*\)
                    \*and ft.TRNS_NUM not in (select f.TRNS_NUM from facttransaction f
                    where f.AAPPCOD = 'D'
                        and f.ATRCODE = 13
                        and f.ATRSBCODE = 5
                        and f.VOUCHER_DATE = currdate)*\*/
                    
                    -- be gofteye Aghaye zayapooryan hazf gardid    
                 and ((ft.AAPPCOD <> 'A' or ft.ATRCODE <> 16 or
                     ft.ATRSBCODE <> 2) and
                     (ft.AAPPCOD <> 'A' or ft.ATRCODE <> 16 or
                     ft.ATRSBCODE not in (7)) and u.USER_POST not in (13)) -- be gofteye Aghaye zayapooryan hazf gardid
              --and u.USER_POST in (2, 3, 11, 12)
              );
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_fin_v10',
       'financial_violation_10',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'ÃœÊ·  Œ·›«  „«·Ì 10');
    commit;
  end;

  ---------------------------------- Proc V13
  procedure ins_fin_v13(currdate date) is
  begin
    OperationStart := sysdate;
    insert /*+noappend*/
    into financial_violation_13
      select seq_fin_v13.nextval as violation_id,
             BRANCH_COD,
             VOUCHER_DATE,
             VOUCHER_NUM,
             fuser_branch_cod,
             Fuser_Cod,
             REVERSE_COD,
             effective_date,
             null,
             null,
             1,
             null
        from (select v.BRANCH_COD,
                     v.VOUCHER_NUM,
                     v.VOUCHER_DATE,
                     v.USER_COD as Fuser_Cod,
                     v.USER_BRNCH_COD as fuser_branch_cod,
                     v.REVERSE_COD as REVERSE_COD,
                     trunc(v.VOUCHER_DATE) as effective_date
                from factvoucherentry v
               where v.TRNS_NUM = -1
                 and v.VOUCHER_DATE >= currdate
                 and v.VOUCHER_DATE < currdate + 1
               group by v.BRANCH_COD,
                        v.VOUCHER_NUM,
                        v.VOUCHER_DATE,
                        v.USER_COD,
                        v.USER_BRNCH_COD,
                        trunc(v.VOUCHER_DATE),
                        REVERSE_COD);
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_fin_v13',
       'financial_violation_13',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'ÃœÊ·  Œ·›«  „«·Ì 13');
    commit;
  end;

  ---------------------------------- Proc V14
  procedure ins_fin_v14(currdate date) is
  begin
    OperationStart := sysdate;
    insert /*+noappend*/
    into financial_violation_14
    
      select /*+parallel(4)*/
       seq_fin_v14.nextval as violation_id,
       BRANCH_COD,
       EFFECTIVE_DATE,
       ACNT_NUM,
       ACCOUNT_DESC,
       ACNT_STATE_DESC,
       AcntNature,
       BAL_BED,
       BAL_BES,
       null                as user_changed_date,
       null                as user_cod,
       1                   as user_changed_stat,
       ACCOUNT_TYPE_COD,
       ACCOUNT_SERIAL,
       null
        from (select a.BRANCH_COD,
                     f.EFFECTIVE_DATE,
                     a.ACCOUNT_NUM as ACNT_NUM,
                     a.ACCOUNT_DESC,
                     da.AISTATEDESC as ACNT_STATE_DESC,
                     decode(a.ACCOUNT_NATURE,
                            0,
                            '„«ÂÌ  »œÂò«—',
                            1,
                            '„«ÂÌ  »” «‰ò«—') as AcntNature,
                     f.BAL_BED,
                     f.BAL_BES,
                     a.ACCOUNT_TYPE_COD,
                     a.ACCOUNT_SERIAL
                from factaccount f
               inner join dimaccount a
                  on f.BRANCH_COD = a.BRANCH_COD
                 and f.ACNT_TYPE_COD = a.ACCOUNT_TYPE_COD
                 and f.ACNT_SERIAL = a.ACCOUNT_SERIAL
               inner join dimaccountstate da
                  on f.ACNT_STATE = da.AISTATE
                left join financial_violation_14 v
                  on a.ACCOUNT_NUM = v.acnt_num
                 and f.BAL_BED = v.bal_bed
                 and f.BAL_BES = v.bal_bes
              
               where a.ACCOUNT_TYPE_COD in (2101,
                                            14,
                                            2119,
                                            823,
                                            963,
                                            976,
                                            3211,
                                            3231,
                                            3409,
                                            823,
                                            824,
                                            827,
                                            3240,
                                            3427,
                                            3428,
                                            871,
                                            873,
                                            875,
                                            3429,
                                            3438,
                                            3471,
                                            3496,
                                            4967,
                                            4968,
                                            4976,
                                            4977,
                                            4981,
                                            5413,
                                            5412,
                                            4994,
                                            4984,
                                            5433,
                                            5475,
                                            5946,
                                            5944,
                                            5931,
                                            5949,
                                            5948,
                                            5947)
                 and (f.BAL_BED != 0 or f.BAL_BES != 0)
                 and v.violation_id is null
                 and f.EFFECTIVE_DATE >= currdate
                 and f.EFFECTIVE_DATE < currdate + 1)
      
      ;
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_fin_v14',
       'financial_violation_14',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'ÃœÊ·  Œ·›«  „«·Ì 14');
    commit;
  end;

  ---------------------------------- Proc V16
  procedure ins_fin_v16(currdate date) is
    NUMOFROWS number;
  begin
    NUMOFROWS := 0;
  
    select count(*)
      into NUMOFROWS
      from dimbranch d
      left join sa_negin_iranzamin.ACBOCH_NEGIN t
        on t.ABRNCHCOD = d.BRNCH_COD
       and t.ACOHOCF = 'C'
       and t.ACOHDESC is null
       and t.ACOHOPDT > currdate
       and t.ACOHOPDT < currdate + 1
     where t.ABRNCHCOD is null;
    if NUMOFROWS < 250 then
      OperationStart := sysdate;
      insert /*+noappend*/
      into financial_violation_16
      
        select /*+parallel(4)*/
         seq_fin_v16.nextval as violation_id,
         BRNCH_COD,
         currdate,
         BRNCH_NAM,
         SUPVSR_COD,
         SUPVSR_NAM,
         null                as user_changed_date,
         null                as user_cod,
         1                   as user_changed_stat,
         null,
         OPEN_OPERATION,
         AUSRCODE
          from (select d.BRNCH_COD,
                       d.BRNCH_NAM,
                       d.SUPVSR_COD,
                       d.SUPVSR_NAM,
                       case
                         when tt.ABRNCHCOD is null then
                          '«‰Ã«„ ‰‘œÂ «” '
                         else
                          '«‰Ã«„ ‘œÂ «” '
                       end as OPEN_OPERATION,
                       tt.AUSRCODE
                  from dimbranch d
                  left join sa_negin_iranzamin.ACBOCH_NEGIN t
                    on t.ABRNCHCOD = d.BRNCH_COD
                   and t.ACOHOCF = 'C'
                   and t.ACOHDESC is null
                   and t.ACOHOPDT > currdate
                   and t.ACOHOPDT < currdate + 1
                  left join sa_negin_iranzamin.ACBOCH_NEGIN tt
                    on tt.ABRNCHCOD = d.BRNCH_COD
                   and tt.ACOHOCF = 'O'
                   and tt.ACOHDESC is null
                   and tt.ACOHOPDT > currdate
                   and tt.ACOHOPDT < currdate + 1
                 where t.ABRNCHCOD is null
                   and d.BRNCH_COD not in (100,
                                           1300,
                                           1000,
                                           2900,
                                           1200,
                                           3400,
                                           2600,
                                           1400,
                                           2500,
                                           2400,
                                           1800,
                                           600,
                                           3500,
                                           1900,
                                           2300,
                                           1100,
                                           2700,
                                           2200,
                                           700,
                                           2000,
                                           1700,
                                           2800,
                                           3600,
                                           3300,
                                           3000,
                                           900,
                                           3100,
                                           3200,
                                           2100,
                                           800,
                                           1600,
                                           1500,
                                           1,
                                           195,
                                           196,
                                           197,
                                           198,
                                           199,
                                           599,
                                           3238,
                                           1215,
                                           2916,
                                           3223,
                                           2813,
                                           1033,
                                           142,
                                           1510,
                                           1039,
                                           1711,
                                           825,
                                           2121,
                                           2715,
                                           3212,
                                           1018,
                                           2218,
                                           1042,
                                           1413,
                                           2413,
                                           3418,
                                           3299,
                                           3399,
                                           2811,
                                           1810,
                                           2012,
                                           1310,
                                           3699,
                                           3111,
                                           2912,
                                           1710,
                                           718,
                                           3311,
                                           2816,
                                           3321,
                                           644,
                                           799,
                                           1610,
                                           1318,
                                           3218,
                                           2010,
                                           3410,
                                           2215,
                                           911,
                                           1516,
                                           637,
                                           1515,
                                           2716,
                                           2724,
                                           1314,
                                           1599,
                                           618,
                                           835,
                                           1411,
                                           3315,
                                           2499,
                                           912));
      rowcnt := sql%rowcount;
      commit;
      OperationEnd := sysdate;
      insert /*+ append */
      into operation_Log
      values
        ('ins_fin_v16',
         'financial_violation_16',
         'Insert',
         rowcnt,
         floor((OperationEnd - OperationStart) * 24 * 60 * 60),
         OperationStart,
         OperationEnd,
         trunc(sysdate),
         currdate,
         'ÃœÊ·  Œ·›«  „«·Ì 16');
      commit;
    end if;
  end;

  ---------------------------------- Proc V18
  procedure ins_fin_v18(currdate date) is
  begin
    OperationStart := sysdate;
    insert /*+noappend*/
    into financial_violation_18
    
      select /*+parallel(4)*/
       seq_fin_v18.nextval as violation_id,
       TRNS_NUM,
       fuser_branch_cod,
       Fuser_Cod,
       VOUCHER_DATE,
       FIRST_BRCNH_COD,
       REVERSE_COD,
       REMITTANCE_REF,
       TRNS_TYPE_DESC,
       AMOUNT,
       null                as user_changed_date,
       null                as user_cod,
       1                   as user_changed_stat,
       null
        from (select f.TRNS_NUM,
                     f.USER_BRNCH_COD  as fuser_branch_cod,
                     f.USER_COD        as Fuser_Cod,
                     f.VOUCHER_DATE,
                     f.FIRST_BRCNH_COD,
                     f.REVERSE_COD,
                     f.REMITTANCE_REF,
                     p.TRNS_TYPE_DESC,
                     f.AMOUNT
                from facttransaction f
               inner join factvoucherentry v
                  on f.TRNS_NUM = v.TRNS_NUM
               inner join dimglstructure g
                  on v.ACNT_TYPE_COD = g.ACCOUNT_TYPE_COD
               inner join dimtrnstype p
                  on f.AAPPCOD = p.AAPPCOD
                 and f.ATRCODE = p.ATRCODE
                 and f.ATRSBCODE = p.ATRSBCODE
               where g.ACCOUNT_HEADING_LEVEL3_ID in (5500, 5501)
                 and ((f.AAPPCOD like 'D' and f.ATRCODE = 31 and
                     f.ATRSBCODE = 12) or
                     (f.AAPPCOD like 'D' and f.ATRCODE = 31 and
                     f.ATRSBCODE = 13) or
                     (f.AAPPCOD like 'K' and f.ATRCODE = 9 and
                     f.ATRSBCODE = 4) or
                     (f.AAPPCOD like 'A' and f.ATRCODE = 105 and
                     f.ATRSBCODE = 1) or
                     (f.AAPPCOD like 'A' and f.ATRCODE = 105 and
                     f.ATRSBCODE = 4) or
                     (f.AAPPCOD like 'L' and f.ATRCODE = 43 and
                     f.ATRSBCODE = 0) or
                     (f.AAPPCOD like 'L' and f.ATRCODE = 44 and
                     f.ATRSBCODE = 0) or
                     (f.AAPPCOD like 'L' and f.ATRCODE = 42 and
                     f.ATRSBCODE = 12) or
                     (f.AAPPCOD like 'L' and f.ATRCODE = 40 and
                     f.ATRSBCODE = 53))
                 and f.VOUCHER_DATE >= currdate
                 and f.VOUCHER_DATE < currdate + 1);
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_fin_v18',
       'financial_violation_18',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'ÃœÊ·  Œ·›«  „«·Ì 18');
    commit;
  end;

  ----------------------------------------------------------

  procedure ins_fin_v20(currdate date) is
    Num number;
  begin
  
    OperationStart := sysdate;
    select count(*)
      into Num
      from facttransaction f
     inner join dimuser d
        on f.USER_BRNCH_COD = d.BRNCH_COD
       and f.USER_COD = d.USER_COD
     where f.VOUCHER_DATE >= currdate
       and f.VOUCHER_DATE < currdate + 1
       and d.USER_POST <> 5;
    if num <= 500 then
      insert /*+noappend*/
      into financial_violation_20
      
        select seq_fin_v20.nextval as violation_id,
               t.TRNS_NUM,
               t.AAPPCOD,
               t.ATRCODE,
               t.ATRSBCODE,
               t.USER_BRNCH_COD,
               t.USER_COD          as FRAUD_USER_COD,
               u.USER_NAME,
               u.USER_POST,
               u.USER_POST_DESC,
               u.USER_TYPE,
               t.AMOUNT,
               d.TRNS_TYPE_DESC,
               t.VOUCHER_DATE      as EFFECTIVE_DATE,
               t.VOUCHER_TIME,
               null                as user_changed_date,
               null                as user_cod,
               1                   as user_changed_stat,
               null                as COMMENT_F,
               t.REMITTANCE_REF,
               t.IS_INTERNAL,
               t.REVERSE_COD
          from facttransaction t
         inner join dimuser u
            on t.USER_BRNCH_COD = u.BRNCH_COD
           and t.USER_COD = u.USER_COD
         inner join dimtrnstype d
            on t.AAPPCOD = d.AAPPCOD
           and t.ATRCODE = d.ATRCODE
           and t.ATRSBCODE = d.ATRSBCODE
          left join financial_violation_20 v
            on t.TRNS_NUM = v.trns_num
           and t.VOUCHER_DATE = v.effective_date
         where t.VOUCHER_DATE >= currdate
           and t.VOUCHER_DATE < currdate + 1
           and u.USER_POST not in (5, 22)
           and (t.AAPPCOD <> 'k' or t.ATRCODE <> 8 and t.ATRSBCODE <> 2)
           and v.trns_num is null;
      rowcnt := sql%rowcount;
      commit;
      OperationEnd := sysdate;
      insert /*+ append */
      into operation_Log
      values
        ('ins_fin_v20',
         'financial_violation_20',
         'Insert',
         rowcnt,
         floor((OperationEnd - OperationStart) * 24 * 60 * 60),
         OperationStart,
         OperationEnd,
         trunc(sysdate),
         currdate,
         'ÃœÊ·  Œ·›«  „«·Ì 20');
      commit;
    
    else
    
      rowcnt := sql%rowcount;
      commit;
      OperationEnd := sysdate;
      insert /*+ append */
      into operation_Log
      values
        ('ins_fin_v20',
         'financial_violation_20',
         'Insert',
         rowcnt,
         floor((OperationEnd - OperationStart) * 24 * 60 * 60),
         OperationStart,
         OperationEnd,
         trunc(sysdate),
         currdate,
         'ÃœÊ·  Œ·›«  „«·Ì 20');
      commit;
    end if;
  end;
  ------------------------------------------------------

  procedure ins_fin_v21(currdate date) is
  
  begin
  
    OperationStart := sysdate;
    insert /*+noappend*/
    into financial_violation_21
    
      select seq_fin_v21.nextval as violation_id,
             TRNS_NUM,
             REMITTANCE_REF,
             FIRST_BRCNH_COD,
             AAPPCOD,
             ATRCODE,
             ATRSBCODE,
             USER_BRNCH_COD,
             FRAUD_USER_COD,
             VOUCHER_DATE,
             VOUCHER_TIME,
             AMOUNT,
             VOUCHER_COUNT,
             REVERSE_COD,
             TRNS_TYPE_DESC,
             null                as user_changed_date,
             null                as user_cod,
             1                   as user_changed_stat,
             null                as COMMENT_F,
             ACNT_TYPE_COD,
             USER_NAME,
             USER_POST,
             USER_POST_DESC
        from ( /*select fc.TRNS_NUM,
                                                                                           fc.REMITTANCE_REF,
                                                                                           fc.FIRST_BRCNH_COD,
                                                                                           fc.AAPPCOD,
                                                                                           fc.ATRCODE,
                                                                                           fc.ATRSBCODE,
                                                                                           fc.USER_BRNCH_COD,
                                                                                           fc.USER_COD as FRAUD_USER_COD,
                                                                                           fc.VOUCHER_DATE,
                                                                                           fc.VOUCHER_TIME,
                                                                                           fc.AMOUNT,
                                                                                           fc.VOUCHER_COUNT,
                                                                                           fc.REVERSE_COD,
                                                                                           dt.TRNS_TYPE_DESC,
                                                                                           max(t.ACNT_TYPE_COD) as ACNT_TYPE_COD
                                                                                      from factvoucherentry t
                                                                                     inner join dimglstructure d
                                                                                        on t.ACNT_TYPE_COD = d.ACCOUNT_TYPE_COD
                                                                                     inner join facttransaction fc
                                                                                        on t.TRNS_NUM = fc.TRNS_NUM
                                                                                     inner join dimtrnstype dt
                                                                                        on t.AAPPCOD = dt.AAPPCOD
                                                                                       and t.ATRCODE = dt.ATRCODE
                                                                                       and t.ATRSBCODE = dt.ATRSBCODE
                                                                                      left join financial_violation_21_tmp f
                                                                                        on f.aappcod = t.AAPPCOD
                                                                                       and f.atrcode = t.ATRCODE
                                                                                       and f.atrsbcode = t.ATRSBCODE
                                                                                       and (f.acnt_type_cod = d.ACCOUNT_TYPE_COD or
                                                                                           f.acnt_type_cod = d.ACCOUNT_HEADING_LEVEL1_ID or
                                                                                           f.acnt_type_cod = d.ACCOUNT_HEADING_LEVEL2_ID or
                                                                                           f.acnt_type_cod = d.ACCOUNT_HEADING_LEVEL3_ID)
                                                                                     where t.VOUCHER_DATE >= currdate
                                                                                       and t.VOUCHER_DATE < currdate +1
                                                                                       and fc.VOUCHER_DATE >= currdate
                                                                                       and fc.VOUCHER_DATE < currdate + 1
                                                                                       and (d.ACCOUNT_TYPE_COD in
                                                                                           (select distinct tt.acnt_type_cod
                                                                                               from financial_violation_21_tmp tt) or
                                                                                           d.ACCOUNT_HEADING_LEVEL1_ID in
                                                                                           (select distinct tt.acnt_type_cod
                                                                                               from financial_violation_21_tmp tt) or
                                                                                           d.ACCOUNT_HEADING_LEVEL2_ID in
                                                                                           (select distinct tt.acnt_type_cod
                                                                                               from financial_violation_21_tmp tt) or
                                                                                           d.ACCOUNT_HEADING_LEVEL3_ID in
                                                                                           (select distinct tt.acnt_type_cod
                                                                                               from financial_violation_21_tmp tt) or
                                                                                           d.ACCOUNT_HEADING_LEVEL4_ID in
                                                                                           (select distinct tt.acnt_type_cod
                                                                                               from financial_violation_21_tmp tt))
                                                                                       and f.aappcod is null
                                                                                     group by fc.TRNS_NUM,
                                                                                              fc.REMITTANCE_REF,
                                                                                              fc.FIRST_BRCNH_COD,
                                                                                              fc.AAPPCOD,
                                                                                              fc.ATRCODE,
                                                                                              fc.ATRSBCODE,
                                                                                              fc.USER_BRNCH_COD,
                                                                                              fc.USER_COD,
                                                                                              fc.VOUCHER_DATE,
                                                                                              fc.VOUCHER_TIME,
                                                                                              fc.AMOUNT,
                                                                                              fc.VOUCHER_COUNT,
                                                                                              fc.REVERSE_COD,
                                                                                              dt.TRNS_TYPE_DESC*/
              
              select fc.TRNS_NUM,
                      fc.REMITTANCE_REF,
                      fc.FIRST_BRCNH_COD,
                      fc.AAPPCOD,
                      fc.ATRCODE,
                      fc.ATRSBCODE,
                      fc.USER_BRNCH_COD,
                      fc.USER_COD as FRAUD_USER_COD,
                      fc.VOUCHER_DATE,
                      fc.VOUCHER_TIME,
                      fc.AMOUNT,
                      fc.VOUCHER_COUNT,
                      fc.REVERSE_COD,
                      dt.TRNS_TYPE_DESC,
                      max(t.ACNT_TYPE_COD) as ACNT_TYPE_COD,
                      max(u.USER_NAME) as USER_NAME,
                      max(u.USER_POST) as USER_POST,
                      max(u.USER_POST_DESC) as USER_POST_DESC
                from factvoucherentry t
               inner join dimglstructure d
                  on t.ACNT_TYPE_COD = d.ACCOUNT_TYPE_COD
               inner join facttransaction fc
                  on t.TRNS_NUM = fc.TRNS_NUM
               inner join dimtrnstype dt
                  on t.AAPPCOD = dt.AAPPCOD
                 and t.ATRCODE = dt.ATRCODE
                 and t.ATRSBCODE = dt.ATRSBCODE
               inner join dimuser u
                  on t.USER_BRNCH_COD = u.BRNCH_COD
                 and t.USER_COD = u.USER_COD
               inner join financial_violation_21_tmp f
                  on f.aappcod = t.AAPPCOD
                 and f.atrcode = t.ATRCODE
                 and f.atrsbcode = t.ATRSBCODE /*
                                                                                       and (f.acnt_type_cod = d.ACCOUNT_TYPE_COD or
                                                                                           f.acnt_type_cod = d.ACCOUNT_HEADING_LEVEL1_ID or
                                                                                           f.acnt_type_cod = d.ACCOUNT_HEADING_LEVEL2_ID or
                                                                                           f.acnt_type_cod = d.ACCOUNT_HEADING_LEVEL3_ID)*/
               where t.VOUCHER_DATE >= currdate
                 and t.VOUCHER_DATE < currdate + 1
                 and fc.VOUCHER_DATE >= currdate
                 and fc.VOUCHER_DATE < currdate + 1
                 and (d.ACCOUNT_TYPE_COD not in (3312, 3489, 5410) or
                     u.USER_POST <> 2)
                 and t.ATRCODE = dt.ATRCODE
                 and t.ATRSBCODE = dt.ATRSBCODE
                    /*and (d.ACCOUNT_TYPE_COD in
                    (select distinct tt.acnt_type_cod
                        from financial_violation_21_tmp tt) or
                    d.ACCOUNT_HEADING_LEVEL1_ID in
                    (select distinct tt.acnt_type_cod
                        from financial_violation_21_tmp tt) or
                    d.ACCOUNT_HEADING_LEVEL2_ID in
                    (select distinct tt.acnt_type_cod
                        from financial_violation_21_tmp tt) or
                    d.ACCOUNT_HEADING_LEVEL3_ID in
                    (select distinct tt.acnt_type_cod
                        from financial_violation_21_tmp tt) or
                    d.ACCOUNT_HEADING_LEVEL4_ID in
                    (select distinct tt.acnt_type_cod
                        from financial_violation_21_tmp tt))*/
                 and (d.ACCOUNT_TYPE_COD in (3231,
                                             3227,
                                             3240,
                                             3438,
                                             3489,
                                             3494,
                                             3495,
                                             5918,
                                             5410,
                                             6502,
                                             6503,
                                             6010,
                                             5903,
                                             5905,
                                             971,
                                             5998) or
                     d.ACCOUNT_HEADING_LEVEL2_ID in (27, 28) or
                     d.ACCOUNT_HEADING_LEVEL3_ID in
                     (3301, 3403, 3407, 64, 3208, 5409, 6104) or
                     d.ACCOUNT_HEADING_LEVEL4_ID = 5925 or
                     (d.ACCOUNT_HEADING_LEVEL2_ID in (51) and
                     d.ACCOUNT_HEADING_LEVEL10_DESC like '%„”œÊœÌ%'))
              
               group by fc.TRNS_NUM,
                         fc.REMITTANCE_REF,
                         fc.FIRST_BRCNH_COD,
                         fc.AAPPCOD,
                         fc.ATRCODE,
                         fc.ATRSBCODE,
                         fc.USER_BRNCH_COD,
                         fc.USER_COD,
                         fc.VOUCHER_DATE,
                         fc.VOUCHER_TIME,
                         fc.AMOUNT,
                         fc.VOUCHER_COUNT,
                         fc.REVERSE_COD,
                         dt.TRNS_TYPE_DESC
              
              );
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert /*+ append */
    into operation_Log
    values
      ('ins_fin_v21',
       'financial_violation_21',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       currdate,
       'ÃœÊ·  Œ·›«  „«·Ì 21');
    commit;
  
  end;

  ----------------------------main_violation

  procedure main_violation is
  begin
    select trunc(max(v.VOUCHER_DATE))
      into max_date
      from factvoucherentry v;
    select max(v.effective_date) + 1
      into currdate
      from financial_violation_all v;
    --  currdate := to_date('03/23/2018', 'mm/dd/yyyy');
    while (currdate <= max_date) loop
      ins_fin_v1(currdate);
      ins_fin_v10(currdate);
      ins_fin_v13(currdate);
      ins_fin_v20(currdate);
      ins_fin_v2(currdate);
      --ins_v3(currdate);
      ins_fin_v4(currdate);
      ins_fin_v5(currdate);
      ins_fin_v6(currdate);
      ins_fin_v8(currdate);
      ins_fin_v14(currdate);
      ins_fin_v2(currdate);
      ins_fin_v16(currdate);
      --  ins_fin_v18(currdate);
      ins_fin_v21(currdate);
      currdate := currdate + 1;
    end loop;
  
  end;
begin
  currdate := trunc(sysdate) - 1;
  t        := 1;
end pkg_financial_violation;
/

prompt
prompt Creating package body PKG_LOAN_VIOLATION
prompt ========================================
prompt
CREATE OR REPLACE PACKAGE BODY FRAUD2DM_IRANZAMIN."PKG_LOAN_VIOLATION" is

  rowcnt         number;
  OperationStart Date;
  OperationEnd   Date;
  t              number;
  fromdate       date;
  --  max_date date;
  --------------------------proc v1
  procedure ins_Loan_v1 is
  begin
  
    OperationStart := sysdate;
    insert /*+noappend*/
    into loan_violation_1
      select /*+Parallel(4)*/
       seq_LOAN_v1.nextval      as violation_id,
       BRNCH_COD,
       CUSTOMER_NUM,
       LoanNumber,
       LOAN_TYPE_DESC,
       CONTRACT_BEGIN_DT,
       ACCEPTED_AMNT,
       LOAN_CURRENT_STATUS_DESC,
       TOTAL_COLLAT_VALUE,
       COLLAT_COUNT,
       CONTRACT_BEGIN_DT        as effective_date,
       null,
       null,
       1,
       LOAN_SERIAL,
       LOAN_MINOR_TYPE,
       LOAN_CURRENT_STATUS,
       LOAN_KEY,
       null                     as comment_f
        from (select dl.BRNCH_COD,
                     dl.CUSTOMER_NUM,
                     replace(TO_CHAR(dl.BRNCH_COD, '0099') || '-' ||
                             TO_CHAR(dl.LOAN_MINOR_TYPE, '099') || '-' ||
                             TO_CHAR(dl.CUSTOMER_NUM, '00000099') || '-' ||
                             TO_CHAR(dl.LOAN_SERIAL, '099'),
                             ' ',
                             '') as LoanNumber,
                     max(dl.LOAN_KEY) LOAN_KEY,
                     max(dl.LOAN_TYPE_DESC) LOAN_TYPE_DESC,
                     max(dl.CONTRACT_BEGIN_DT) CONTRACT_BEGIN_DT,
                     max(dl.ACCEPTED_AMNT) ACCEPTED_AMNT,
                     max(dl.LOAN_CURRENT_STATUS_DESC) LOAN_CURRENT_STATUS_DESC,
                     max(dl.LOAN_CURRENT_STATUS) LOAN_CURRENT_STATUS,
                     sum(fc.COLLAT_VALUE) TOTAL_COLLAT_VALUE,
                     count(fc.COLLAT_KEY) COLLAT_COUNT,
                     dl.LOAN_SERIAL,
                     dl.LOAN_MINOR_TYPE
                from dimloan dl
                left outer join factcollats fc
                  on dl.CUSTOMER_NUM = fc.CUSTOMER_NUM
                 and dl.LOAN_MINOR_TYPE = fc.LOAN_MINOR_TYPE
                 and dl.BRNCH_COD = fc.BRNCH_COD
                 and dl.LOAN_SERIAL = fc.LOAN_SERIAL
                 and fc.COLLATERAL_TYPE_CODE not in (13 , 11) -- 11 be darkhaste ziyapurian hazf shod --soltani
                left outer join dimcollat dc
                  on dc.COLLAT_KEY = fc.COLLAT_KEY
                /*left outer join (select ff.CUSTOMER_NUM,
                                       ff.LOAN_MINOR_TYPE,
                                       ff.BRNCH_COD,
                                       ff.LOAN_SERIAL
                                  from factloan ff
                                 inner join (select --LOAN_KEY,
                                             dll.CUSTOMER_NUM,
                                             dll.LOAN_MINOR_TYPE,
                                             dll.BRNCH_COD,
                                             dll.LOAN_SERIAL,
                                             CONTRACT_APPROVED_DT
                                              from dimloan dll
                                             where dll.LOAN_CURRENT_STATUS in
                                                   ('7', '8')) dloan
                                --on ff.LOAN_KEY = dloan.LOAN_KEY
                                    on ff.CUSTOMER_NUM = dloan.CUSTOMER_NUM
                                   and ff.LOAN_MINOR_TYPE =
                                       dloan.LOAN_MINOR_TYPE
                                   and ff.BRNCH_COD = dloan.BRNCH_COD
                                   and ff.LOAN_SERIAL = dloan.LOAN_SERIAL
                                 where ff.EFFECTIVE_DATE =
                                       dloan.CONTRACT_APPROVED_DT
                                   and ff.LOAN_STATUS in ('7', '8')) Removed_Loan
                  on dl.CUSTOMER_NUM = Removed_Loan.CUSTOMER_NUM
                 and dl.LOAN_MINOR_TYPE = Removed_Loan.LOAN_MINOR_TYPE
                 and dl.BRNCH_COD = Removed_Loan.BRNCH_COD
                 and dl.LOAN_SERIAL = Removed_Loan.LOAN_SERIAL*/
              /*left outer join (select f.CUSTOMER_NUM,
                                    f.LOAN_MINOR_TYPE,
                                    f.BRNCH_COD,
                                    f.LOAN_SERIAL
                               from factloanacc f
                              where f.LOAN_MINOR_TYPE in
                                    (24,28,29,30,31,32,43,46,52,53,54,55,56,57,58,59,60,61,62,65)
                                and ACCEPTED_AMNT = CUST_PREPAID) Zeman
               on dl.CUSTOMER_NUM = Zeman.CUSTOMER_NUM
              and dl.LOAN_MINOR_TYPE = zeman.LOAN_MINOR_TYPE
              and dl.BRNCH_COD = zeman.BRNCH_COD
              and dl.LOAN_SERIAL = zeman.LOAN_SERIAL*/
                left outer join loan_violation_1 lv1
                  on dl.CUSTOMER_NUM = lv1.customer_num
                 and dl.LOAN_MINOR_TYPE = lv1.loan_minor_type
                 and dl.BRNCH_COD = lv1.brnch_cod
                 and dl.LOAN_SERIAL = lv1.loan_serial
               where (dc.IS_CURRENT = 1 or dc.IS_CURRENT is null)
               and dc.COLLAT_STAT_CODE = 1
                /* and Removed_Loan.CUSTOMER_NUM is null
                 and Removed_Loan.LOAN_MINOR_TYPE is null
                 and Removed_Loan.BRNCH_COD is null
                 and Removed_Loan.LOAN_SERIAL is null*/
                    /* and Zeman.CUSTOMER_NUM is null
                    and Zeman.LOAN_MINOR_TYPE is null
                    and Zeman.BRNCH_COD is null
                    and zeman.LOAN_SERIAL is null*/
                    and dl.LOAN_CURRENT_STATUS in('3','4','5','F','6','D','G','H')
                 and lv1.customer_num is null
                    /*and dl.LOAN_MINOR_TYPE not in
                    (250, 251, 252, 253, 254, 256, 255)*/
                 and dl.LOAN_MINOR_TYPE <> 21
                 and dl.CONTRACT_BEGIN_DT <= trunc(sysdate) - 7
              --    and dl.LOAN_MINOR_TYPE <> 340
              --    and dl.LOAN_MINOR_TYPE <> 341
              /*and dl.LOAN_KEY not in
              (select f.LOAN_KEY
                 from factloanacc f
                where f.LOAN_MINOR_TYPE in (400, 410, 420, 430, 440)
                  and ACCEPTED_AMNT = CUST_PREPAID)*/
               group by dl.CUSTOMER_NUM,
                        dl.LOAN_MINOR_TYPE,
                        dl.BRNCH_COD,
                        dl.LOAN_SERIAL
              having nvl(Min(fc.COLLAT_VALUE), 10000000) <  1000000);
  
    /*insert \*+noappend*\
    into loan_violation_1
      select t.*
        from TMP_loan_violation_1 t
        left join sa_negin_iranzamin.lcact2lon l
          on t.brnch_cod = l.abrnchcod
         and t.loan_minor_type = l.lnminortp
         and t.customer_num = l.cfcifno
         and t.loan_serial = l.crserial
         and l.atypcode = 6010
        left join factaccount f
          on f.BRANCH_COD = l.acn_abrnchcod
         and f.ACNT_TYPE_COD = l.atypcode
         and f.ACNT_SERIAL = l.aiserial
         and f.EFFECTIVE_DATE = t.contract_begin_dt
         and (t.accepted_amnt = f.BAL_BES or
             t.accepted_amnt = f.TRNOVER_BES)
        left join loan_violation_1 lv
          on t.brnch_cod = lv.brnch_cod
         and t.loan_minor_type = lv.loan_minor_type
         and t.customer_num = lv.customer_num
         and t.loan_serial = lv.loan_serial
       where f.BRANCH_COD is null
         and lv.violation_id is null;*/
  
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert into operation_Log
    values
      ('ins_Loan_v1',
       'loan_violation_1',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       sysdate,
       'ÃœÊ·  Œ·›«  Ê«„ 1');
    commit;
  end;
  --------------------------------V2
  procedure ins_Loan_v2 is
  begin
  
    OperationStart := sysdate;
    insert /*+noappend*/
    into TMP_loan_violation_2
    /*select \*+Parallel(4)*\
    seq_LOAN_v2.nextval as violation_id,
    BRNCH_COD,
    CUSTOMER_NUM,
    LOAN_SERIAL,
    LOAN_MINOR_TYPE,
    LoanNumber,
    LOAN_TYPE_DESC,
    CONTRACT_BEGIN_DT,
    ACCEPTED_AMNT,
    LOAN_STATUS_DESC,
    LOAN_STATUS,
    REMAINED_DEBT,
    CONTRACT_BEGIN_DT   as effective_date,
    null,
    null,
    1
     from (select fla.BRNCH_COD,
                  fla.CUSTOMER_NUM,
                  fla.LOAN_SERIAL,
                  fla.LOAN_MINOR_TYPE,
                  replace(TO_CHAR(fla.BRNCH_COD, '0099') || '-' ||
                              TO_CHAR(fla.LOAN_MINOR_TYPE, '099') || '-' ||
                              TO_CHAR(fla.CUSTOMER_NUM, '00000099') || '-' ||
                              TO_CHAR(fla.LOAN_SERIAL, '099'),
                              ' ',
                              '') as LoanNumber,
                  max(dlt.LOAN_TYPE_DESC) LOAN_TYPE_DESC,
                  max(fla.CONTRACT_BEGIN_DT) CONTRACT_BEGIN_DT,
                  max(fla.ACCEPTED_AMNT) ACCEPTED_AMNT,
                  max(dls.LOAN_STATUS_DESC) LOAN_STATUS_DESC,
                  max(dls.LOAN_STATUS) LOAN_STATUS,
                  max(fla.REMAINED_DEBT) REMAINED_DEBT
             from factloanacc fla
             left outer join factcollats fc
               --on fla.LOAN_KEY = fc.LOAN_KEY      -------    Be elate vojude vasighe ke vamash moshakhas nabud , dar loan number adade -1 gharar gerefte bud in khat kament shod ta pas as eslah fact az kament kharej shavad va 4 khate badi kament shavad 1395-06-23
               on  fla.CUSTOMER_NUM = fc.CUSTOMER_NUM
               and fla.LOAN_MINOR_TYPE = fc.LOAN_MINOR_TYPE
               and fla.BRNCH_COD = fc.BRNCH_COD
               and fla.LOAN_SERIAL = fc.LOAN_SERIAL
               and fla.CONTRACT_BEGIN_DT = fc.CONTRACT_BEGIN_DT
               and fc.COLLATERAL_TYPE_CODE <> 16
             left outer join dimcollat dc
               on dc.COLLAT_KEY = fc.COLLAT_KEY
             inner join dimloantype dlt
               on fla.LOAN_MINOR_TYPE = dlt.LOAN_MINOR_TYPE
             inner join dimloanstatus dls
               on dls.LOAN_STATUS = fla.LOAN_STATUS
             left outer join (select ff.CUSTOMER_NUM,ff.LOAN_MINOR_TYPE,ff.BRNCH_COD,ff.LOAN_SERIAL
                               from factloan ff
                              inner join (select -- LOAN_KEY,
                                                 DLL.CUSTOMER_NUM,DLL.LOAN_MINOR_TYPE,DLL.BRNCH_COD,DLL.LOAN_SERIAL,
                                                CONTRACT_APPROVED_DT
                                           from dimloan dll
                                          where dll.LOAN_CURRENT_STATUS in
                                                ('7', '8')) dloan
                                  on ff.CUSTOMER_NUM = dloan.CUSTOMER_NUM
                                 AND FF.LOAN_MINOR_TYPE = dloan.LOAN_MINOR_TYPE
                                 AND FF.BRNCH_COD = dloan.BRNCH_COD
                                 AND FF.LOAN_SERIAL = dloan.LOAN_SERIAL
                              where ff.EFFECTIVE_DATE =
                                    dloan.CONTRACT_APPROVED_DT
                                and ff.LOAN_STATUS in ('7', '8')) Removed_Loan
               on fla.CUSTOMER_NUM = Removed_Loan.CUSTOMER_NUM
               AND FLA.LOAN_MINOR_TYPE = Removed_Loan.LOAN_MINOR_TYPE
               AND FLA.BRNCH_COD = Removed_Loan.BRNCH_COD
               AND FLA.LOAN_SERIAL = Removed_Loan.LOAN_SERIAL
             left outer join (select --f.LOAN_KEY
                                     F.CUSTOMER_NUM,F.LOAN_MINOR_TYPE,F.BRNCH_COD,F.LOAN_SERIAL
                               from factloanacc f
                              where f.LOAN_MINOR_TYPE in
                                    (400, 410, 420, 430, 440)
                               ) Zeman
               on fla.CUSTOMER_NUM = Zeman.CUSTOMER_NUM
               AND FLA.LOAN_MINOR_TYPE = ZEMAN.LOAN_MINOR_TYPE
               AND FLA.BRNCH_COD = ZEMAN.BRNCH_COD
               AND FLA.LOAN_SERIAL = ZEMAN.LOAN_SERIAL
             left outer join loan_violation_2 lv2
               on fla.CUSTOMER_NUM = lv2.customer_num
              and fla.LOAN_MINOR_TYPE = lv2.loan_minor_type
              and fla.BRNCH_COD = lv2.brnch_cod
              and fla.LOAN_SERIAL = lv2.loan_serial
            where (dc.IS_CURRENT = 1 or dc.IS_CURRENT is null)
              and lv2.customer_num is null
              and Removed_Loan.CUSTOMER_NUM is null
               and Removed_Loan.LOAN_MINOR_TYPE is null
               and Removed_Loan.BRNCH_COD is null
               and Removed_Loan.LOAN_SERIAL is null
    
              and Zeman.CUSTOMER_NUM is null
               and Zeman.LOAN_MINOR_TYPE is null
               and Zeman.BRNCH_COD is null
               and Zeman.LOAN_SERIAL  is null
              and fla.LOAN_MINOR_TYPE not in (250, 251, 252, 253, 254,256,255,340,341)
           \*and fla.LOAN_KEY not in
           (select f.LOAN_KEY
              from factloanacc f
             where f.LOAN_MINOR_TYPE in (400, 410, 420, 430, 440)
               and ACCEPTED_AMNT = CUST_PREPAID)*\
            group by fla.CUSTOMER_NUM,fla.LOAN_MINOR_TYPE,fla.BRNCH_COD,fla.LOAN_SERIAL
           having count(fc.COLLAT_KEY) = 0);*/
    
      select /*+Parallel(4)*/
       seq_LOAN_v2.nextval as violation_id,
       BRNCH_COD,
       CUSTOMER_NUM,
       LOAN_SERIAL,
       LOAN_MINOR_TYPE,
       LoanNumber,
       LOAN_TYPE_DESC,
       CONTRACT_BEGIN_DT,
       ACCEPTED_AMNT,
       LOAN_STATUS_DESC,
       LOAN_STATUS,
       REMAINED_DEBT,
       CONTRACT_BEGIN_DT   as effective_date,
       null,
       null,
       1,
       null                as comment_f,
       ECONOMIC_UNIT_COD,
       ECONOMIC_UNIT_DESC,
       CustName
        from (select fla.BRNCH_COD,
                     fla.CUSTOMER_NUM,
                     fla.LOAN_SERIAL,
                     fla.LOAN_MINOR_TYPE,
                     replace(TO_CHAR(fla.BRNCH_COD, '0099') || '-' ||
                             TO_CHAR(fla.LOAN_MINOR_TYPE, '099') || '-' ||
                             TO_CHAR(fla.CUSTOMER_NUM, '00000099') || '-' ||
                             TO_CHAR(fla.LOAN_SERIAL, '099'),
                             ' ',
                             '') as LoanNumber,
                     dlt.LOAN_TYPE_DESC LOAN_TYPE_DESC,
                     fla.CONTRACT_BEGIN_DT CONTRACT_BEGIN_DT,
                     fla.ACCEPTED_AMNT ACCEPTED_AMNT,
                     dls.LOAN_STATUS_DESC LOAN_STATUS_DESC,
                     dls.LOAN_STATUS LOAN_STATUS,
                     fla.REMAINED_DEBT REMAINED_DEBT,
                     dl.ECONOMIC_UNIT_COD,
                     dl.ECONOMIC_UNIT_DESC,
                     c.CUST_FIRST_NAME || '-' || c.CUST_LAST_NAME as CustName
                from factloanacc fla
                left outer join factcollats fc
              --on fla.LOAN_KEY = fc.LOAN_KEY      -------    Be elate vojude vasighe ke vamash moshakhas nabud , dar loan number adade -1 gharar gerefte bud in khat kament shod ta pas as eslah fact az kament kharej shavad va 4 khate badi kament shavad 1395-06-23
                  on fla.CUSTOMER_NUM = fc.CUSTOMER_NUM
                 and fla.LOAN_MINOR_TYPE = fc.LOAN_MINOR_TYPE
                 and fla.BRNCH_COD = fc.BRNCH_COD
                 and fla.LOAN_SERIAL = fc.LOAN_SERIAL
              --    and fla.CONTRACT_BEGIN_DT = fc.CONTRACT_BEGIN_DT
              --  and fc.COLLATERAL_TYPE_CODE <> 16  mehre eghtesad code 16 nadarad (soltani)
                left outer join dimcollat dc
                  on dc.COLLAT_KEY = fc.COLLAT_KEY
               inner join dimloantype dlt
                  on fla.LOAN_MINOR_TYPE = dlt.LOAN_MINOR_TYPE
               inner join dimloanstatus dls
                  on dls.LOAN_STATUS = fla.LOAN_STATUS
              
              /* left outer join (select --f.LOAN_KEY
                              F.CUSTOMER_NUM,
                              F.LOAN_MINOR_TYPE,
                              F.BRNCH_COD,
                              F.LOAN_SERIAL
                               from factloanacc f
                              where f.LOAN_MINOR_TYPE in
                                    (400, 410, 420, 430, 440)) Zeman  --mehre eghtesad zeman nadarad (soltani)
               on fla.CUSTOMER_NUM = Zeman.CUSTOMER_NUM
              AND FLA.LOAN_MINOR_TYPE = ZEMAN.LOAN_MINOR_TYPE
              AND FLA.BRNCH_COD = ZEMAN.BRNCH_COD
              AND FLA.LOAN_SERIAL = ZEMAN.LOAN_SERIAL*/
                left outer join TMP_loan_violation_2 lv2
                  on fla.CUSTOMER_NUM = lv2.customer_num
                 and fla.LOAN_MINOR_TYPE = lv2.loan_minor_type
                 and fla.BRNCH_COD = lv2.brnch_cod
                 and fla.LOAN_SERIAL = lv2.loan_serial
               inner join dimloan dl
                  on fla.CUSTOMER_NUM = dl.CUSTOMER_NUM
                 and fla.LOAN_MINOR_TYPE = dl.loan_minor_type
                 and fla.BRNCH_COD = dl.BRNCH_COD
                 and fla.LOAN_SERIAL = dl.LOAN_SERIAL
               inner join dimcustomer c
                  on fla.CUSTOMER_NUM = c.CUSTOMER_NUM
              
               where (dc.IS_CURRENT = 1 or dc.IS_CURRENT is null)
                 and lv2.customer_num is null
                 and dl.LOAN_CURRENT_STATUS in('3','4','5','F','6','D','G','H')
                    
                    -- and Zeman.CUSTOMER_NUM is null
                    /*and fla.LOAN_MINOR_TYPE not in
                    (185,250, 251, 252, 253, 254, 256, 255, 340, 341)*/
                 and fc.LOAN_KEY is null);
  
    insert /*+noappend*/
    into loan_violation_2
      select t.*
        from TMP_loan_violation_2 t
        left join sa_negin_iranzamin.lcact2lon l
          on t.brnch_cod = l.abrnchcod
         and t.loan_minor_type = l.lnminortp
         and t.customer_num = l.cfcifno
         and t.loan_serial = l.crserial
         and l.atypcode = 6010
        left join factaccount f
          on f.BRANCH_COD = l.acn_abrnchcod
         and f.ACNT_TYPE_COD = l.atypcode
         and f.ACNT_SERIAL = l.aiserial
         and f.EFFECTIVE_DATE = t.contract_begin_dt
         and (t.accepted_amnt = f.BAL_BES or
             t.accepted_amnt = f.TRNOVER_BES)
        left join loan_violation_2 lv
          on t.brnch_cod = lv.brnch_cod
         and t.loan_minor_type = lv.loan_minor_type
         and t.customer_num = lv.customer_num
         and t.loan_serial = lv.loan_serial
       where f.BRANCH_COD is null
         and lv.violation_id is null;
  
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert into operation_Log
    values
      ('ins_Loan_v2',
       'loan_violation_2',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       sysdate,
       'ÃœÊ·  Œ·›«  Ê«„ 2');
    commit;
  end;
  ---------------------------V3
  procedure ins_Loan_v3 is
  begin
  
    OperationStart := sysdate;
    insert /*+noappend*/
    INTO LOAN_VIOLATION_3
      select /*+Parallel(4)*/
       seq_LOAN_v3.nextval      as violation_id,
       BRNCH_COD,
       CUSTOMER_NUM,
       LoanNumber,
       LOAN_MINOR_TYPE,
       LOAN_SERIAL,
       LOAN_TYPE_DESC,
       ACCEPTED_AMNT,
       LOAN_CURRENT_STATUS,
       LOAN_CURRENT_STATUS_DESC,
       WAGE_RATE,
       EXPECTED_PROFIT_RATE,
       DISCOUNT_RATE,
       PENALTY_RATE,
       CONTRACT_BEGIN_DT,
       CONTRACT_BEGIN_DT        as effective_date,
       null,
       null,
       1,
       LOAN_KEY,
       null                     as comment_f,
       ECONOMIC_UNIT_COD,
       LOAN_TITLE
        from (select f.BRNCH_COD,
                     f.CUSTOMER_NUM,
                     replace(TO_CHAR(d.BRNCH_COD, '0099') || '-' ||
                             TO_CHAR(d.LOAN_MINOR_TYPE, '099') || '-' ||
                             TO_CHAR(d.CUSTOMER_NUM, '00000099') || '-' ||
                             TO_CHAR(d.LOAN_SERIAL, '099'),
                             ' ',
                             '') as LoanNumber,
                     f.LOAN_MINOR_TYPE,
                     f.LOAN_SERIAL,
                     d.LOAN_TYPE_DESC,
                     f.ACCEPTED_AMNT,
                     d.LOAN_CURRENT_STATUS,
                     d.LOAN_CURRENT_STATUS_DESC,
                     d.WAGE_RATE,
                     d.EXPECTED_PROFIT_RATE,
                     d.DISCOUNT_RATE,
                     d.PENALTY_RATE,
                     f.CONTRACT_BEGIN_DT,
                     d.LOAN_KEY,
                     d.ECONOMIC_UNIT_COD,
                     d.LOAN_TITLE
                from factloanacc f
               inner join dimloan d
                  on d.LOAN_KEY = f.LOAN_KEY
                left outer join (select FF.CUSTOMER_NUM,
                                       FF.LOAN_MINOR_TYPE,
                                       FF.BRNCH_COD,
                                       FF.LOAN_SERIAL
                                  from factloan ff
                                 inner join (select DLL.CUSTOMER_NUM,
                                                   DLL.LOAN_MINOR_TYPE,
                                                   DLL.BRNCH_COD,
                                                   DLL.LOAN_SERIAL,
                                                   CONTRACT_APPROVED_DT
                                              from dimloan dll
                                             where dll.LOAN_CURRENT_STATUS in
                                                   ('7', '8')) dloan
                                    on ff.CUSTOMER_NUM = dloan.CUSTOMER_NUM
                                   AND ff.LOAN_MINOR_TYPE =
                                       dloan.LOAN_MINOR_TYPE
                                   AND ff.BRNCH_COD = dloan.BRNCH_COD
                                   AND ff.LOAN_SERIAL = dloan.LOAN_SERIAL
                                 where ff.EFFECTIVE_DATE =
                                       dloan.CONTRACT_APPROVED_DT
                                   and ff.LOAN_STATUS in ('7', '8')) Removed_Loan
                  on d.CUSTOMER_NUM = Removed_Loan.CUSTOMER_NUM
                 AND D.LOAN_MINOR_TYPE = Removed_Loan.LOAN_MINOR_TYPE
                 AND D.BRNCH_COD = Removed_Loan.BRNCH_COD
                 AND D.LOAN_SERIAL = Removed_Loan.LOAN_SERIAL
                left outer join loan_violation_3 lv3
                  on d.CUSTOMER_NUM = lv3.customer_num
                 AND D.LOAN_MINOR_TYPE = LV3.LOAN_MINOR_TYPE
                 AND D.BRNCH_COD = LV3.BRNCH_COD
                 AND D.LOAN_SERIAL = LV3.LOAN_SERIAL
               where lv3.customer_num is null
                 and Removed_Loan.CUSTOMER_NUM is null
                 and Removed_Loan.LOAN_MINOR_TYPE is null
                 and Removed_Loan.BRNCH_COD is null
                 and Removed_Loan.LOAN_SERIAL is null
                 and d.LOAN_CURRENT_STATUS in('3','4','5','F','6','D','G','H')
                 and d.WAGE_RATE = 0
                 and d.LOAN_MAJOR_TYPE = 35
                 and d.EXPECTED_PROFIT_RATE = 0);
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert into operation_Log
    values
      ('ins_Loan_v3',
       'loan_violation_3',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       sysdate,
       'ÃœÊ·  Œ·›«  Ê«„ 3');
    commit;
  end;
  ----------------------------V4
  procedure ins_Loan_v4 is
  begin
  
    OperationStart := sysdate;
    insert /*+noappend*/
    into loan_violation_4
      select /*+Parallel(4)*/
       seq_LOAN_v4.nextval as violation_id,
       LOAN_KEY,
       CUSTOMER_NUM,
       LoanNumber,
       LOAN_MINOR_TYPE,
       BRNCH_COD,
       LOAN_SERIAL,
       LOAN_STATUS,
       CONTRACT_BEGIN_DT,
       ACCEPTED_AMNT,
       age,
       TOTAL_COLLAT_VALUE,
       CONTRACT_BEGIN_DT   as effective_date,
       null,
       null,
       1,
       null                as comment_f,
       CUST_BIRTH_DATE
        from (select /*+parallel(4)*/
               max(f.LOAN_KEY) as loan_key,
               f.CUSTOMER_NUM,
               replace(TO_CHAR(f.BRNCH_COD, '0099') || '-' ||
                       TO_CHAR(f.LOAN_MINOR_TYPE, '099') || '-' ||
                       TO_CHAR(f.CUSTOMER_NUM, '00000099') || '-' ||
                       TO_CHAR(f.LOAN_SERIAL, '099'),
                       ' ',
                       '') as LoanNumber,
               f.LOAN_MINOR_TYPE,
               f.BRNCH_COD,
               f.LOAN_SERIAL,
               max(f.LOAN_STATUS) LOAN_STATUS,
               max(f.CONTRACT_BEGIN_DT) CONTRACT_BEGIN_DT,
               max(f.ACCEPTED_AMNT) ACCEPTED_AMNT,
               max((trunc(MONTHS_BETWEEN(f.CONTRACT_BEGIN_DT,
                                         d.CUST_BIRTH_DATE) / 12))) as age, -- sen dar tarikhe shoroe gharardad
               sum(fc.COLLAT_VALUE) TOTAL_COLLAT_VALUE,
               max(d.CUST_BIRTH_DATE) CUST_BIRTH_DATE
                from factloanacc f
               inner join dimcustomer d
                  on d.CUSTOMER_NUM = f.CUSTOMER_NUM
               inner join factcollats fc
                  on fc.CUSTOMER_NUM = f.CUSTOMER_NUM
                 and fc.LOAN_MINOR_TYPE = f.LOAN_MINOR_TYPE
                 and fc.BRNCH_COD = f.BRNCH_COD
                 and fc.LOAN_SERIAL = f.LOAN_SERIAL
              --on fc.LOAN_KEY = f.LOAN_KEY
               inner join dimcollat dc
                  on dc.COLLAT_KEY = fc.COLLAT_KEY
                /*left outer join (select ff.CUSTOMER_NUM,
                                       ff.LOAN_MINOR_TYPE,
                                       ff.BRNCH_COD,
                                       ff.LOAN_SERIAL
                                  from factloan ff
                                 inner join (select dll.CUSTOMER_NUM,
                                                   dll.LOAN_MINOR_TYPE,
                                                   dll.BRNCH_COD,
                                                   dll.LOAN_SERIAL,
                                                   CONTRACT_APPROVED_DT
                                              from dimloan dll
                                             where dll.LOAN_CURRENT_STATUS in
                                                   ('7', '8')) dloan
                                    on ff.CUSTOMER_NUM = dloan.CUSTOMER_NUM
                                   and ff.LOAN_MINOR_TYPE =
                                       dloan.LOAN_MINOR_TYPE
                                   and ff.BRNCH_COD = dloan.BRNCH_COD
                                   and ff.LOAN_SERIAL = dloan.LOAN_SERIAL
                                 where ff.EFFECTIVE_DATE =
                                       dloan.CONTRACT_APPROVED_DT
                                   and ff.LOAN_STATUS in ('7', '8')) Removed_Loan
                  on f.CUSTOMER_NUM = Removed_Loan.CUSTOMER_NUM
                 and f.LOAN_MINOR_TYPE = Removed_Loan.LOAN_MINOR_TYPE
                 and f.BRNCH_COD = Removed_Loan.BRNCH_COD
                 and f.LOAN_SERIAL = Removed_Loan.LOAN_SERIAL*/
                left outer join loan_violation_4 lv4
                  on f.CUSTOMER_NUM = lv4.customer_num
                 and f.LOAN_MINOR_TYPE = lv4.loan_minor_type
                 and f.BRNCH_COD = lv4.brnch_cod
                 and f.LOAN_SERIAL = lv4.loan_serial
               where lv4.customer_num is null
                 /*and Removed_Loan.CUSTOMER_NUM is null
                 and Removed_Loan.LOAN_MINOR_TYPE is null
                 and Removed_Loan.BRNCH_COD is null
                 and Removed_Loan.LOAN_SERIAL is null*/
                 and dc.IS_CURRENT = 1
                 and f.LOAN_STATUS in('3','4','5','F','6','D','G','H')
                 and f.LOAN_MINOR_TYPE not in (350)
                 and (MONTHS_BETWEEN(trunc(f.CONTRACT_BEGIN_DT),
                                     trunc(d.CUST_BIRTH_DATE))) / 12 < 18
                 and d.CUST_TYPE_COD = 0
               group by f.CUSTOMER_NUM,
                        f.LOAN_MINOR_TYPE,
                        f.BRNCH_COD,
                        f.LOAN_SERIAL);
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert into operation_Log
    values
      ('ins_Loan_v4',
       'loan_violation_4',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       sysdate,
       'ÃœÊ·  Œ·›«  Ê«„ 4');
    commit;
  end;
  ---------------------------
  procedure ins_Loan_v6 is
  begin
  
    OperationStart := sysdate;
    insert /*+noappend*/
    into loan_violation_6
      select /*+Parallel(4)*/
       seq_LOAN_v6.nextval          as violation_id,
       CUSTOMER_NUM,
       LoanNumber_Moavagh,
       LoanNumber_New,
       Branch_Moavagh,
       LOAN_MINOR_TYPE_Moavagh,
       LOAN_SERIAL_Moavagh,
       ACCEPTED_AMNT_Moavagh,
       LOAN_TYPE_DESC_Moavagh,
       CONTRACT_BEGIN_DT_Moavagh,
       loan_status_moavagh,
       LOAN_STATUS_DESC_Moavagh,
       REMAINED_DEBT_Moavagh,
       BRNCH_COD_New,
       LOAN_MINOR_TYPE_New,
       LOAN_SERIAL_New,
       ACCEPTED_AMNT_New,
       LOAN_TYPE_DESC_New,
       CONTRACT_BEGIN_DT_new,
       loan_current_status_new,
       loan_current_status_desc_new,
       CONTRACT_BEGIN_DT_new        as effective_date,
       null,
       null,
       1,
       null                         as comment_f
        from (select dl.CUSTOMER_NUM,
                     replace(TO_CHAR(fl2.BRNCH_COD, '0099') || '-' ||
                             TO_CHAR(fl2.LOAN_MINOR_TYPE, '099') || '-' ||
                             TO_CHAR(fl2.CUSTOMER_NUM, '00000099') || '-' ||
                             TO_CHAR(fl2.LOAN_SERIAL, '099'),
                             ' ',
                             '') as LoanNumber_Moavagh,
                     replace(TO_CHAR(dl.BRNCH_COD, '0099') || '-' ||
                             TO_CHAR(dl.LOAN_MINOR_TYPE, '099') || '-' ||
                             TO_CHAR(dl.CUSTOMER_NUM, '00000099') || '-' ||
                             TO_CHAR(dl.LOAN_SERIAL, '099'),
                             ' ',
                             '') as LoanNumber_New,
                     fl2.BRNCH_COD Branch_Moavagh,
                     fl2.LOAN_MINOR_TYPE LOAN_MINOR_TYPE_Moavagh,
                     fl2.LOAN_SERIAL LOAN_SERIAL_Moavagh,
                     fl2.ACCEPTED_AMNT ACCEPTED_AMNT_Moavagh,
                     fl2.LOAN_TYPE_DESC LOAN_TYPE_DESC_Moavagh,
                     fl2.CONTRACT_BEGIN_DT CONTRACT_BEGIN_DT_Moavagh,
                     fl2.LOAN_STATUS loan_status_moavagh,
                     fl2.LOAN_STATUS_DESC LOAN_STATUS_DESC_Moavagh,
                     fl2.REMAINED_DEBT REMAINED_DEBT_Moavagh,
                     dl.BRNCH_COD BRNCH_COD_New,
                     dl.LOAN_MINOR_TYPE LOAN_MINOR_TYPE_New,
                     dl.LOAN_SERIAL LOAN_SERIAL_New,
                     dl.ACCEPTED_AMNT ACCEPTED_AMNT_New,
                     dl.LOAN_TYPE_DESC LOAN_TYPE_DESC_New,
                     dl.CONTRACT_BEGIN_DT CONTRACT_BEGIN_DT_new,
                     dl.LOAN_CURRENT_STATUS loan_current_status_new,
                     dl.LOAN_CURRENT_STATUS_DESC loan_current_status_desc_new
                from dimloan dl  /*(select CUSTOMER_NUM,
                             BRNCH_COD,
                             LOAN_MINOR_TYPE,
                             LOAN_SERIAL,
                             ACCEPTED_AMNT,
                             LOAN_TYPE_DESC,
                             CONTRACT_BEGIN_DT,
                             CONTRACT_APPROVED_DT,
                             b.LOAN_CURRENT_STATUS,
                             b.LOAN_CURRENT_STATUS_DESC
                        from dimloan b
                       where b.LOAN_MINOR_TYPE not in
                             (116,
                              201,
                              163,
                              126,
                              250,
                              251,
                              252,
                              253,
                              254,
                              256,
                              257,
                              258)) dl*/
               inner join (select x1.CUSTOMER_NUM,
                                 x1.LOAN_KEY,
                                 x1.LOAN_MINOR_TYPE,
                                 x1.BRNCH_COD,
                                 x1.LOAN_SERIAL,
                                 x1.EFFECTIVE_DATE,
                                 dls.LOAN_STATUS_DESC,
                                 dls.LOAN_STATUS,
                                 dd.ACCEPTED_AMNT,
                                 dd.LOAN_TYPE_DESC,
                                 dd.CONTRACT_BEGIN_DT,
                                 x1.REMAINED_DEBT
                            from (select CUSTOMER_NUM,
                                         LOAN_MINOR_TYPE,
                                         BRNCH_COD,
                                         LOAN_SERIAL,
                                         EFFECTIVE_DATE,
                                         REMAINED_DEBT,
                                         LOAN_KEY,
                                         LOAN_STATUS
                                    from factloan x
                                   where x.LOAN_STATUS in ('4', '5', 'F')) x1
                           inner join dimloan dd
                              on x1.CUSTOMER_NUM = dd.CUSTOMER_NUM
                             and x1.LOAN_MINOR_TYPE = dd.LOAN_MINOR_TYPE
                             and x1.BRNCH_COD = dd.BRNCH_COD
                             and x1.LOAN_SERIAL = dd.LOAN_SERIAL
                           inner join dimloanstatus dls
                              on x1.LOAN_STATUS = dls.LOAN_STATUS) fl2
                  on dl.CUSTOMER_NUM = fl2.CUSTOMER_NUM
               /* left outer join (select ff.CUSTOMER_NUM,
                                       ff.LOAN_MINOR_TYPE,
                                       ff.BRNCH_COD,
                                       ff.LOAN_SERIAL
                                -- ff.LOAN_KEY
                                  from factloan ff
                                 inner join (select dll.CUSTOMER_NUM,
                                                   dll.LOAN_MINOR_TYPE,
                                                   dll.BRNCH_COD,
                                                   dll.LOAN_SERIAL,
                                                   -- dll.LOAN_KEY,
                                                   CONTRACT_APPROVED_DT
                                              from dimloan dll
                                             where dll.LOAN_CURRENT_STATUS in
                                                   ('7', '8')) dloan
                                    on ff.CUSTOMER_NUM = dloan.CUSTOMER_NUM
                                   and ff.LOAN_MINOR_TYPE =
                                       dloan.LOAN_MINOR_TYPE
                                   and ff.BRNCH_COD = dloan.BRNCH_COD
                                   and ff.LOAN_SERIAL = dloan.LOAN_SERIAL
                                 where ff.EFFECTIVE_DATE =
                                       dloan.CONTRACT_APPROVED_DT
                                   and ff.LOAN_STATUS in ('7', '8')) Removed_Loan
                  on fl2.CUSTOMER_NUM = Removed_Loan.CUSTOMER_NUM
                 and fl2.LOAN_MINOR_TYPE = Removed_Loan.LOAN_MINOR_TYPE
                 and fl2.BRNCH_COD = Removed_Loan.BRNCH_COD
                 and fl2.LOAN_SERIAL = Removed_Loan.LOAN_SERIAL*/
              --on fl2.LOAN_KEY = Removed_Loan.LOAN_KEY
                left outer join loan_violation_6 lv6
                  on dl.CUSTOMER_NUM = lv6.customer_num
                 and dl.LOAN_MINOR_TYPE = lv6.LOAN_MINOR_TYPE_New
                 and dl.BRNCH_COD = lv6.BRNCH_COD_New
                 and dl.LOAN_SERIAL = lv6.LOAN_SERIAL_New
               where lv6.customer_num is null
                 /*and Removed_Loan.CUSTOMER_NUM is null
                 and Removed_Loan.LOAN_MINOR_TYPE is null
                 and Removed_Loan.BRNCH_COD is null
                 and Removed_Loan.LOAN_SERIAL is null*/
                 and dl.LOAN_CURRENT_STATUS in('3','4','5','F','6','D','G','H')
                 and fl2.EFFECTIVE_DATE = dl.CONTRACT_APPROVED_DT
                 and replace(TO_CHAR(fl2.BRNCH_COD, '0099') || '-' ||
                             TO_CHAR(fl2.LOAN_MINOR_TYPE, '099') || '-' ||
                             TO_CHAR(fl2.CUSTOMER_NUM, '00000099') || '-' ||
                             TO_CHAR(fl2.LOAN_SERIAL, '099'),
                             ' ',
                             '') <>
                     replace(TO_CHAR(dl.BRNCH_COD, '0099') || '-' ||
                             TO_CHAR(dl.LOAN_MINOR_TYPE, '099') || '-' ||
                             TO_CHAR(dl.CUSTOMER_NUM, '00000099') || '-' ||
                             TO_CHAR(dl.LOAN_SERIAL, '099'),
                             ' ',
                             '')
              
              /*fl2.BRNCH_COD <> dl.BRNCH_COD
              and fl2.LOAN_MINOR_TYPE <> dl.LOAN_MINOR_TYPE
              and fl2.CUSTOMER_NUM <> dl.CUSTOMER_NUM
              and fl2.LOAN_SERIAL <> dl.LOAN_SERIAL*/
              );
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert into operation_Log
    values
      ('ins_Loan_v6',
       'loan_violation_6',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       sysdate,
       'ÃœÊ·  Œ·›«  Ê«„ 6');
    commit;
  end;

  ---------------------------
  ---------------------------- V9
  /*  procedure ins_Loan_v9 is
  begin
  
    OperationStart := sysdate;
    insert \*+noappend*\
    into loan_violation_9
      select \*+Parallel(4)*\
       seq_LOAN_v9.nextval      as violation_id,
       LOAN_KEY,
       loannumber,
       CUSTOMER_NUM,
       LOAN_MINOR_TYPE,
       BRNCH_COD,
       LOAN_SERIAL,
       ACCEPTED_AMNT,
       REMAINED_DEBT,
       LOAN_CURRENT_STATUS,
       LOAN_CURRENT_STATUS_DESC,
       BankPaidDate,
       CONTRACT_APPROVED_DT,
       Total_COLLAT_VALUE,
       BankPaidDate             as effective_date,
       null,
       null,
       1
        from (select \*parallel(4)*\
               f.LOAN_KEY,
               max(replace(TO_CHAR(f.BRNCH_COD, '0099') || '-' ||
                           TO_CHAR(f.LOAN_MINOR_TYPE, '099') || '-' ||
                           TO_CHAR(f.CUSTOMER_NUM, '00000099') || '-' ||
                           TO_CHAR(f.LOAN_SERIAL, '099'),
                           ' ',
                           '')) as LoanNumber,
               max(f.CUSTOMER_NUM) CUSTOMER_NUM,
               max(f.LOAN_MINOR_TYPE) LOAN_MINOR_TYPE,
               max(f.BRNCH_COD) BRNCH_COD,
               max(f.LOAN_SERIAL) LOAN_SERIAL,
               max(d.ACCEPTED_AMNT) ACCEPTED_AMNT,
               max(f.REMAINED_DEBT) REMAINED_DEBT,
               max(d.LOAN_CURRENT_STATUS) LOAN_CURRENT_STATUS,
               max(d.LOAN_CURRENT_STATUS_DESC) LOAN_CURRENT_STATUS_DESC,
               min(f.EFFECTIVE_DATE) BankPaidDate, --tarikhe pardakht
               max(d.CONTRACT_APPROVED_DT) CONTRACT_APPROVED_DT,
               sum(fc.COLLAT_VALUE) Total_COLLAT_VALUE
                from factloan f
               inner join dimloan d
                  on d.LOAN_KEY = f.LOAN_KEY
               inner join factcollats fc
                  on fc.LOAN_KEY = d.LOAN_KEY
               inner join dimcollat dc
                  on dc.COLLAT_KEY = fc.COLLAT_KEY
                left outer join (select ff.LOAN_KEY
                                  from factloan ff
                                 inner join (select LOAN_KEY,
                                                   CONTRACT_APPROVED_DT
                                              from dimloan dll
                                             where dll.LOAN_CURRENT_STATUS in
                                                   ('7', '8')) dloan
                                    on ff.LOAN_KEY = dloan.LOAN_KEY
                                 where ff.EFFECTIVE_DATE =
                                       dloan.CONTRACT_APPROVED_DT
                                   and ff.LOAN_STATUS in ('7', '8')) Removed_Loan
                  on d.LOAN_KEY = Removed_Loan.LOAN_KEY
  
                left outer join loan_violation_9 lv9
                  on f.LOAN_KEY = lv9.loan_key
  
               where lv9.customer_num is null
                 and Removed_Loan.LOAN_KEY is null
                 and dc.IS_CURRENT = 1
                 and f.BANK_PAID_AMNT > 0
                 and f.EFFECTIVE_DATE < d.CONTRACT_APPROVED_DT
  
               group by f.LOAN_KEY);
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert into operation_Log
    values
      ('ins_Loan_v9',
       'loan_violation_9',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       sysdate,
       'ÃœÊ·  Œ·›«  Ê«„ 9');
    commit;
  end;*/

  procedure ins_Loan_v9 is
  begin
  
    OperationStart := sysdate;
    insert /*+noappend*/
    into loan_violation_9
      select /*+Parallel(4)*/
       seq_LOAN_v9.nextval      as violation_id,
       LOAN_KEY,
       loannumber,
       CUSTOMER_NUM,
       LOAN_MINOR_TYPE,
       BRNCH_COD,
       LOAN_SERIAL,
       ACCEPTED_AMNT,
       REMAINED_DEBT,
       LOAN_CURRENT_STATUS,
       LOAN_CURRENT_STATUS_DESC,
       BankPaidDate,
       CONTRACT_APPROVED_DT,
       Total_COLLAT_VALUE,
       BankPaidDate             as effective_date,
       null,
       null,
       1,
       null                     as comment_f,
       LOAN_TITLE
        from (select /*parallel(4)*/
               max(d.LOAN_KEY) as LOAN_KEY,
               replace(TO_CHAR(f.BRNCH_COD, '0099') || '-' ||
                       TO_CHAR(f.LOAN_MINOR_TYPE, '099') || '-' ||
                       TO_CHAR(f.CUSTOMER_NUM, '00000099') || '-' ||
                       TO_CHAR(f.LOAN_SERIAL, '099'),
                       ' ',
                       '') as LoanNumber,
               f.CUSTOMER_NUM,
               f.LOAN_MINOR_TYPE,
               f.BRNCH_COD,
               f.LOAN_SERIAL,
               max(d.ACCEPTED_AMNT) ACCEPTED_AMNT,
               max(f.REMAINED_DEBT) REMAINED_DEBT,
               max(d.LOAN_CURRENT_STATUS) LOAN_CURRENT_STATUS,
               max(d.LOAN_CURRENT_STATUS_DESC) LOAN_CURRENT_STATUS_DESC,
               min(d.CONTRACT_BEGIN_DT) BankPaidDate, --tarikhe pardakht
               max(d.CONTRACT_APPROVED_DT) CONTRACT_APPROVED_DT,
               sum(fc.COLLAT_VALUE) Total_COLLAT_VALUE,
               max(d.LOAN_TITLE) LOAN_TITLE
                from factloan f
               inner join dimloan d
                  on d.CUSTOMER_NUM = f.CUSTOMER_NUM
                 and d.LOAN_MINOR_TYPE = f.LOAN_MINOR_TYPE
                 and d.BRNCH_COD = f.BRNCH_COD
                 and d.LOAN_SERIAL = f.LOAN_SERIAL
               inner join factcollats fc
                  on fc.CUSTOMER_NUM = d.CUSTOMER_NUM
                 and fc.LOAN_MINOR_TYPE = d.LOAN_MINOR_TYPE
                 and fc.BRNCH_COD = d.BRNCH_COD
                 and fc.LOAN_SERIAL = d.LOAN_SERIAL
               inner join dimcollat dc
                  on dc.COLLAT_KEY = fc.COLLAT_KEY
              
                left outer join loan_violation_9 lv9
                  on f.CUSTOMER_NUM = lv9.customer_num
                 and f.LOAN_MINOR_TYPE = lv9.loan_minor_type
                 and f.BRNCH_COD = lv9.brnch_cod
                 and f.LOAN_SERIAL = lv9.loan_serial
              
               where lv9.customer_num is null
                 and dc.IS_CURRENT = 1
                 and d.ACCEPTED_AMNT > 0
                 and d.LOAN_CURRENT_STATUS in('3','4','5','F','6','D','G','H')
                 and d.CONTRACT_BEGIN_DT <> d.CONTRACT_APPROVED_DT
                 and f.EFFECTIVE_DATE = trunc(sysdate) - 1
               group by f.CUSTOMER_NUM,
                        f.LOAN_MINOR_TYPE,
                        f.BRNCH_COD,
                        f.LOAN_SERIAL);
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert into operation_Log
    values
      ('ins_Loan_v9',
       'loan_violation_9',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       fromdate,
       sysdate,
       'ÃœÊ·  Œ·›«  Ê«„ 9');
    commit;
  end;

  --------------------------------V19
  procedure ins_Loan_v19 is
  begin
  
    OperationStart := sysdate;
    insert /*+noappend*/
    into loan_violation_19
      select /*+Parallel(4)*/
       seq_LOAN_v19.nextval as violation_id,
       LOAN_KEY,
       LoanNumber,
       CUSTOMER_NUM,
       LOAN_MINOR_TYPE,
       BRNCH_COD,
       LOAN_SERIAL,
       CONTRACT_BEGIN_DT,
       ACCEPTED_AMNT,
       LOAN_CURRENT_STATUS,
       LOAN_STATUS_DESC,
       EXPECTED_PROFIT_RATE,
       PENALTY_RATE,
       WAGE_RATE,
       DISCOUNT_RATE,
       CONTRACT_BEGIN_DT    as effective_date,
       null,
       null,
       1,
       null                 as comment_f,
       ECONOMIC_UNIT_COD,
       ECONOMIC_UNIT_DESC,
       LOAN_TITLE
        from (select d.LOAN_KEY,
                     d.CUSTOMER_NUM,
                     replace(TO_CHAR(d.BRNCH_COD, '0099') || '-' ||
                             TO_CHAR(d.LOAN_MINOR_TYPE, '099') || '-' ||
                             TO_CHAR(d.CUSTOMER_NUM, '00000099') || '-' ||
                             TO_CHAR(d.LOAN_SERIAL, '099'),
                             ' ',
                             '') as LoanNumber,
                     d.LOAN_MINOR_TYPE,
                     d.BRNCH_COD,
                     d.LOAN_SERIAL,
                     d.CONTRACT_BEGIN_DT,
                     d.ACCEPTED_AMNT,
                     d.LOAN_CURRENT_STATUS,
                     ds.LOAN_STATUS_DESC,
                     d.EXPECTED_PROFIT_RATE,
                     d.PENALTY_RATE,
                     d.WAGE_RATE,
                     d.DISCOUNT_RATE,
                     d.ECONOMIC_UNIT_COD,
                     d.ECONOMIC_UNIT_DESC,
                     d.LOAN_TITLE
                from /*factloanacc f
                     inner join*/ dimloan d
               inner join DIMLOANSTATUS ds
                  on trim(d.LOAN_CURRENT_STATUS) = trim(ds.LOAN_STATUS)
              /*on d.LOAN_KEY = f.LOAN_KEY*/
               /* left outer join (select ff.CUSTOMER_NUM,
                                       ff.LOAN_MINOR_TYPE,
                                       ff.BRNCH_COD,
                                       ff.LOAN_SERIAL
                                  from factloan ff
                                 inner join (select dll.CUSTOMER_NUM,
                                                   dll.LOAN_MINOR_TYPE,
                                                   dll.BRNCH_COD,
                                                   dll.LOAN_SERIAL,
                                                   CONTRACT_APPROVED_DT
                                              from dimloan dll
                                             where dll.LOAN_CURRENT_STATUS in
                                                   ('7', '8')) dloan
                                    on ff.CUSTOMER_NUM = dloan.CUSTOMER_NUM
                                   and ff.LOAN_MINOR_TYPE =
                                       dloan.LOAN_MINOR_TYPE
                                   and ff.BRNCH_COD = dloan.BRNCH_COD
                                   and ff.LOAN_SERIAL = dloan.LOAN_SERIAL
                                 where ff.EFFECTIVE_DATE =
                                       dloan.CONTRACT_APPROVED_DT
                                   and ff.LOAN_STATUS in ('7', '8')) Removed_Loan
                  on d.CUSTOMER_NUM = Removed_Loan.CUSTOMER_NUM
                 and d.LOAN_MINOR_TYPE = Removed_Loan.CUSTOMER_NUM
                 and d.BRNCH_COD = Removed_Loan.CUSTOMER_NUM
                 and d.LOAN_SERIAL = Removed_Loan.CUSTOMER_NUM*/
                left outer join loan_violation_19 lv19
                  on d.CUSTOMER_NUM = lv19.customer_num
                 and d.LOAN_MINOR_TYPE = lv19.loan_minor_type
                 and d.BRNCH_COD = lv19.brnch_cod
                 and d.LOAN_SERIAL = lv19.loan_serial
               where lv19.customer_num is null
                /* and Removed_Loan.CUSTOMER_NUM is null
                 and Removed_Loan.loan_minor_type is null
                 and Removed_Loan.brnch_cod is null
                 and Removed_Loan.loan_serial is null*/
                 and d.LOAN_CURRENT_STATUS in('3','4','5','F','6','D','G','H')
                 and d.LOAN_MAJOR_TYPE not in (35, 21)
                 and d.EXPECTED_PROFIT_RATE = 0);
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert into operation_Log
    values
      ('ins_Loan_v19',
       'loan_violation_19',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       sysdate,
       'ÃœÊ·  Œ·›«  Ê«„ 19');
    commit;
  end;
  -------------------------------------------------V21
  procedure ins_Loan_v21 is
  begin
  
    OperationStart := sysdate;
    insert /*+noappend*/
    into loan_violation_21
      select /*+Parallel(4)*/
       seq_LOAN_v21.nextval     as violation_id,
       LOAN_KEY,
       LoanNumber,
       CUSTOMER_NUM,
       LOAN_MINOR_TYPE,
       BRNCH_COD,
       LOAN_SERIAL,
       LOAN_TYPE_DESC,
       CONTRACT_APPROVED_DT,
       EFFECTIVE_DATE,
       CONTRACT_BEGIN_DT,
       ACCEPTED_AMNT,
       LOAN_CURRENT_STATUS,
       LOAN_CURRENT_STATUS_DESC,
       null,
       null,
       1,
       BRNCH_NAM,
       null                     as comment_f
        from (select f.LOAN_KEY,
                     max(replace(TO_CHAR(f.BRNCH_COD, '0099') || '-' ||
                                 TO_CHAR(f.LOAN_MINOR_TYPE, '099') || '-' ||
                                 TO_CHAR(f.CUSTOMER_NUM, '00000099') || '-' ||
                                 TO_CHAR(f.LOAN_SERIAL, '099'),
                                 ' ',
                                 '')) as LoanNumber,
                     max(f.CUSTOMER_NUM) CUSTOMER_NUM,
                     max(f.LOAN_MINOR_TYPE) LOAN_MINOR_TYPE,
                     max(f.BRNCH_COD) BRNCH_COD,
                     max(f.LOAN_SERIAL) LOAN_SERIAL,
                     min(f.EFFECTIVE_DATE) EFFECTIVE_DATE,
                     max(d.LOAN_TYPE_DESC) LOAN_TYPE_DESC,
                     max(d.CONTRACT_APPROVED_DT) CONTRACT_APPROVED_DT,
                     max(d.CONTRACT_BEGIN_DT) CONTRACT_BEGIN_DT,
                     max(d.ACCEPTED_AMNT) ACCEPTED_AMNT,
                     max(d.LOAN_CURRENT_STATUS) LOAN_CURRENT_STATUS,
                     max(d.LOAN_CURRENT_STATUS_DESC) LOAN_CURRENT_STATUS_DESC,
                     max(db.BRNCH_NAM) BRNCH_NAM
              
                from factloan f
               inner join dimbranch db
                  on db.BRNCH_COD = f.BRNCH_COD
               inner join (select *
                            from dimloan dl
                           where dl.LOAN_MAJOR_TYPE = 21
                             and dl.LOAN_CURRENT_STATUS in ( 'F')
                             ------------- bakhshe zir(union All) dar banke iran zamin va be darkhaste bank ezafe gardid  , shayad dar bank digar niyazi nabashad (check gardad) 
                          union all
                          select t.*
                            from dimloan t
                           inner join sa_negin_iranzamin.lcact2lon l
                              on t.BRNCH_COD = l.abrnchcod
                             and t.LOAN_MINOR_TYPE = l.lnminortp
                             and t.CUSTOMER_NUM = l.cfcifno
                             and t.LOAN_SERIAL = l.crserial
                             and l.atypcode in (2820, 2821)
                           inner join factaccount ff
                              on l.acn_abrnchcod = ff.BRANCH_COD
                             and l.atypcode = ff.ACNT_TYPE_COD
                             and l.aiserial = ff.ACNT_SERIAL
                           where t.LOAN_MAJOR_TYPE = 21
                             and ff.EFFECTIVE_DATE = trunc(sysdate) - 1
                             and t.LOAN_CURRENT_STATUS = '3'
                             and (ff.BAL_BED <> 0 or ff.BAL_BES <> 0)
                             ---------------------------------   
                             ) d
                  on f.LOAN_KEY = d.LOAN_KEY
              
              /* left outer join (select ff.LOAN_KEY
                              from factloan ff
                             inner join (select LOAN_KEY,
                                               CONTRACT_APPROVED_DT
                                          from dimloan dll
                                         where dll.LOAN_CURRENT_STATUS in
                                               ('7', '8')) dloan
                                on ff.LOAN_KEY = dloan.LOAN_KEY
                             where ff.EFFECTIVE_DATE =
                                   dloan.CONTRACT_APPROVED_DT
                               and ff.LOAN_STATUS in ('7', '8')) Removed_Loan
              on d.LOAN_KEY = Removed_Loan.LOAN_KEY*/
                left outer join loan_violation_21 lv21
                  on d.LOAN_KEY = lv21.loan_key
              
               where lv21.customer_num is null
                    /*  and Removed_Loan.LOAN_KEY is null*/
                 and f.LOAN_STATUS in ('4', '5', 'F','3')
               group by f.LOAN_KEY
              
              );
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert into operation_Log
    values
      ('ins_Loan_v21',
       'loan_violation_21',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       sysdate,
       'ÃœÊ·  Œ·›«  Ê«„ 21');
    commit;
  end;

  --------------------------------------------------------------V22

  procedure ins_Loan_v22 is
  begin
  
    OperationStart := sysdate;
    insert /*+noappend*/
    into loan_violation_22
      select /*+Parallel(4)*/
       seq_LOAN_v22.nextval     as violation_id,
       LOAN_KEY,
       LoanNumber,
       CUSTOMER_NUM,
       LOAN_MINOR_TYPE,
       BRNCH_COD,
       LOAN_SERIAL,
       CONTRACT_BEGIN_DT        as CONTRACT_APPROVED_DT,
       MIN_VOUCHER_DATE,
       CONTRACT_BEGIN_DT        EFFECTIVE_DATE,
       LOAN_TYPE_DESC,
       LOAN_CURRENT_STATUS,
       LOAN_CURRENT_STATUS_DESC,
       ACCEPTED_AMNT,
       null,
       null,
       1,
       NULL,
       NUMOFCHQ,
       ECONOMIC_UNIT_COD,
       ECONOMIC_UNIT_DESC,
       LOAN_TITLE
        from (select min(d.LOAN_KEY) LOAN_KEY,
                     replace(TO_CHAR(d.BRNCH_COD, '0099') || '-' ||
                             TO_CHAR(d.LOAN_MINOR_TYPE, '099') || '-' ||
                             TO_CHAR(d.CUSTOMER_NUM, '00000099') || '-' ||
                             TO_CHAR(d.LOAN_SERIAL, '099'),
                             ' ',
                             '') as LoanNumber,
                     d.CUSTOMER_NUM,
                     d.LOAN_MINOR_TYPE,
                     d.BRNCH_COD,
                     d.LOAN_SERIAL,
                     max(d.CONTRACT_BEGIN_DT) CONTRACT_BEGIN_DT,
                     min(f.REJDATE) MIN_VOUCHER_DATE,
                     min(d.LOAN_TYPE_DESC) LOAN_TYPE_DESC,
                     min(d.LOAN_CURRENT_STATUS) LOAN_CURRENT_STATUS,
                     min(dls.LOAN_STATUS_DESC) LOAN_CURRENT_STATUS_DESC,
                     min(d.ACCEPTED_AMNT) ACCEPTED_AMNT,
                     count(*) NUMOFCHQ,
                     min(d.ECONOMIC_UNIT_COD) ECONOMIC_UNIT_COD,
                     min(d.ECONOMIC_UNIT_DESC) ECONOMIC_UNIT_DESC,
                     min(d.LOAN_TITLE) LOAN_TITLE
                from dimloan d
               inner join factretchq f
                  on f.CUSTOMER_NUM = d.CUSTOMER_NUM
               inner join dimloanstatus dls
                  on d.LOAN_CURRENT_STATUS = dls.LOAN_STATUS
                left outer join loan_violation_22 lv22
                  on d.LOAN_KEY = lv22.loan_key
               where lv22.customer_num is null
                 and d.CONTRACT_BEGIN_DT > f.REJDATE
                 and (d.CONTRACT_BEGIN_DT + 1 <= f.DELDATE or
                     f.DELDATE is null)
                     and d.LOAN_CURRENT_STATUS in('3','4','5','F','6','D','G','H')
               group by d.BRNCH_COD,
                        d.LOAN_MINOR_TYPE,
                        d.CUSTOMER_NUM,
                        d.LOAN_SERIAL
              
              );
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert into operation_Log
    values
      ('ins_Loan_v22',
       'loan_violation_22',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       sysdate,
       'ÃœÊ·  Œ·›«  Ê«„ 22');
    commit;
  end;

  --------------------------------------------------------------V23
  procedure ins_Loan_v23(fromdate date) is
  begin
  
    OperationStart := sysdate;
    insert /*+noappend*/
    into loan_violation_23
      select /*+Parallel(4)*/
       seq_LOAN_v23.nextval         as violation_id,
       Old_Loan_Key,
       Old_LoanNumber,
       New_LoanNumber,
       CUSTOMER_NUM,
       Old_LOAN_MINOR_TYPE,
       Old_BRAN,
       Old_LOAN_SERIAL,
       Old_ACCEPTED_AMNT,
       EFFECTIVE_DATE,
       Old_Stat_effdat,
       old_LOAN_STATUS_DESC_effdat,
       Old_CONTRACT_BEGIN_DT,
       Old_LOAN_TYPE_DESC,
       old_LOAN_CURRENT_STATUS_DESC,
       old_LOAN_CURRENT_STATUS,
       NEW_Loan_Key,
       LOAN_MINOR_TYPE,
       BRNCH_COD,
       LOAN_SERIAL,
       ACCEPTED_AMNT,
       CONTRACT_BEGIN_DT,
       LOAN_TYPE_DESC,
       LOAN_CURRENT_STATUS,
       LOAN_CURRENT_STATUS_DESC,
       null,
       null,
       1,
       BRNCH_NAM,
       null                         as comment_f
        from (select t.Old_Loan_Key,
                     replace(TO_CHAR(t.Old_BRAN, '0099') || '-' ||
                             TO_CHAR(t.Old_LOAN_MINOR_TYPE, '099') || '-' ||
                             TO_CHAR(t.CUSTOMER_NUM, '00000099') || '-' ||
                             TO_CHAR(t.Old_LOAN_SERIAL, '099'),
                             ' ',
                             '') as Old_LoanNumber,
                     replace(TO_CHAR(t.BRNCH_COD, '0099') || '-' ||
                             TO_CHAR(t.LOAN_MINOR_TYPE, '099') || '-' ||
                             TO_CHAR(t.CUSTOMER_NUM, '00000099') || '-' ||
                             TO_CHAR(t.LOAN_SERIAL, '099'),
                             ' ',
                             '') as New_LoanNumber,
                     t.CUSTOMER_NUM,
                     t.Old_LOAN_MINOR_TYPE,
                     t.Old_BRAN,
                     t.Old_LOAN_SERIAL,
                     dll.ACCEPTED_AMNT Old_ACCEPTED_AMNT,
                     t.EFFECTIVE_DATE,
                     t.Old_Stat_effdat,
                     t.old_LOAN_STATUS_DESC_effdat,
                     dll.CONTRACT_BEGIN_DT Old_CONTRACT_BEGIN_DT,
                     dll.LOAN_TYPE_DESC Old_LOAN_TYPE_DESC,
                     dll.LOAN_CURRENT_STATUS_DESC old_LOAN_CURRENT_STATUS_DESC,
                     dll.LOAN_CURRENT_STATUS old_LOAN_CURRENT_STATUS,
                     t.NEW_Loan_Key,
                     t.LOAN_MINOR_TYPE,
                     t.BRNCH_COD,
                     t.LOAN_SERIAL,
                     t.ACCEPTED_AMNT,
                     t.CONTRACT_BEGIN_DT,
                     t.LOAN_TYPE_DESC,
                     t.LOAN_CURRENT_STATUS,
                     t.LOAN_CURRENT_STATUS_DESC,
                     db.BRNCH_NAM
                from dimloan dll
               inner join dimbranch db
                  on db.BRNCH_COD = dll.BRNCH_COD
              
               inner join (select ff.LOAN_KEY                 Old_Loan_Key,
                                 ff.LOAN_MINOR_TYPE          Old_LOAN_MINOR_TYPE,
                                 ff.BRNCH_COD                Old_BRAN,
                                 ff.LOAN_SERIAL              Old_LOAN_SERIAL,
                                 ff.EFFECTIVE_DATE,
                                 ff.LOAN_STATUS              Old_Stat_effdat,
                                 dls.LOAN_STATUS_DESC        old_LOAN_STATUS_DESC_effdat,
                                 dl.LOAN_KEY                 NEW_Loan_Key,
                                 dl.CUSTOMER_NUM,
                                 dl.LOAN_MINOR_TYPE,
                                 dl.BRNCH_COD,
                                 dl.LOAN_SERIAL,
                                 dl.CONTRACT_BEGIN_DT,
                                 dl.LOAN_TYPE_DESC,
                                 dl.ACCEPTED_AMNT,
                                 dl.LOAN_CURRENT_STATUS,
                                 dl.LOAN_CURRENT_STATUS_DESC
                            from factloan ff
                           inner join dimloan dl
                              on ff.CUSTOMER_NUM = dl.CUSTOMER_NUM
                           inner join dimloanstatus dls
                              on dls.LOAN_STATUS = ff.LOAN_STATUS
                          
                          /*left outer join (select ff.LOAN_KEY
                                          from factloan ff
                                         inner join (select LOAN_KEY,
                                                           CONTRACT_APPROVED_DT
                                                      from dimloan dll
                                                     where dll.LOAN_CURRENT_STATUS in
                                                           ('7', '8')) dloan
                                            on ff.LOAN_KEY =
                                               dloan.LOAN_KEY
                                         where ff.EFFECTIVE_DATE =
                                               dloan.CONTRACT_APPROVED_DT
                                           and ff.LOAN_STATUS in
                                               ('7', '8')) Removed_Loan
                          on dl.LOAN_KEY = Removed_Loan.LOAN_KEY*/
                            left outer join loan_violation_23 lv23
                              on dl.LOAN_KEY = lv23.NEW_Loan_Key
                           where lv23.customer_num is null
                                /*and Removed_Loan.LOAN_KEY is null*/
                             and ff.EFFECTIVE_DATE = dl.CONTRACT_BEGIN_DT
                             and ff.BRNCH_COD <> dl.BRNCH_COD
                             and ff.LOAN_STATUS  in ('3','4','5','F','G','D')
                             and dl.LOAN_CURRENT_STATUS in('3','4','5','F','G','D')
                             and dl.CONTRACT_BEGIN_DT > fromdate) t
              
                  on dll.LOAN_KEY = t.Old_Loan_Key
              
              );
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert into operation_Log
    values
      ('ins_Loan_v23',
       'loan_violation_23',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       fromdate,
       sysdate,
       'ÃœÊ·  Œ·›«  Ê«„ 23');
    commit;
  end;

  procedure ins_Loan_v24 is
  begin
  
    OperationStart := sysdate;
    insert /*+noappend*/
    into loan_violation_24
      select /*+Parallel(4)*/
       seq_LOAN_v24.nextval     as violation_id,
       LOAN_KEY,
       loannumber,
       CUSTOMER_NUM,
       LOAN_MINOR_TYPE,
       BRNCH_COD,
       LOAN_SERIAL,
       CONTRACT_BEGIN_DT,
       LOAN_TYPE_DESC,
       LOAN_CURRENT_STATUS,
       LOAN_CURRENT_STATUS_DESC,
       ACCEPTED_AMNT,
       CONTRACT_BEGIN_DT        as effective_date,
       null,
       null,
       1,
       null,
       COLCUM
        from (select t.BRNCH_COD,
                     t.LOAN_MINOR_TYPE,
                     t.CUSTOMER_NUM,
                     t.LOAN_SERIAL,
                     max(d.LOAN_KEY) as LOAN_KEY,
                     t.BRNCH_COD || '-' || t.LOAN_MINOR_TYPE || '-' ||
                     t.CUSTOMER_NUM || '-' || t.LOAN_SERIAL as loannumber,
                     max(d.CONTRACT_BEGIN_DT) as CONTRACT_BEGIN_DT,
                     max(d.LOAN_TYPE_DESC) as LOAN_TYPE_DESC,
                     max(d.LOAN_CURRENT_STATUS) as LOAN_CURRENT_STATUS,
                     max(d.LOAN_CURRENT_STATUS_DESC) as LOAN_CURRENT_STATUS_DESC,
                     max(d.ACCEPTED_AMNT) as ACCEPTED_AMNT,
                     count(*) as COLCUM
                from factcollats t
               inner join dimloan d
                  on t.BRNCH_COD = d.BRNCH_COD
                 and t.LOAN_MINOR_TYPE = d.LOAN_MINOR_TYPE
                 and t.CUSTOMER_NUM = d.CUSTOMER_NUM
                 and t.LOAN_SERIAL = d.LOAN_SERIAL
                left join (select f.BRNCH_COD,
                                 f.LOAN_MINOR_TYPE,
                                 f.CUSTOMER_NUM,
                                 f.LOAN_SERIAL
                            from factcollats f
                           where f.COLLATERAL_TYPE_CODE <> 13
                           group by f.BRNCH_COD,
                                    f.LOAN_MINOR_TYPE,
                                    f.CUSTOMER_NUM,
                                    f.LOAN_SERIAL) ff
                  on t.BRNCH_COD = ff.BRNCH_COD
                 and t.LOAN_MINOR_TYPE = ff.LOAN_MINOR_TYPE
                 and t.CUSTOMER_NUM = ff.CUSTOMER_NUM
                 and t.LOAN_SERIAL = ff.LOAN_SERIAL
                left join loan_violation_24 lv
                  on t.BRNCH_COD = lv.BRNCH_COD
                 and t.LOAN_MINOR_TYPE = lv.LOAN_MINOR_TYPE
                 and t.CUSTOMER_NUM = lv.CUSTOMER_NUM
                 and t.LOAN_SERIAL = lv.LOAN_SERIAL
               where ff.brnch_cod is null
                 and lv.customer_num is null
                 and d.LOAN_CURRENT_STATUS in('3','4','5','F','6','D','G','H')
                 and t.COLLATERAL_TYPE_CODE = 13
               group by t.BRNCH_COD,
                        t.LOAN_MINOR_TYPE,
                        t.CUSTOMER_NUM,
                        t.LOAN_SERIAL
              
              );
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert into operation_Log
    values
      ('ins_Loan_v24',
       'loan_violation_24',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       trunc(sysdate),
       sysdate,
       'ÃœÊ·  Œ·›«  Ê«„ 24');
    commit;
  end;

  --------------------------------------------------------

  procedure ins_Loan_v28(currdate date) is
  begin
  
    OperationStart := sysdate;
    insert /*+noappend*/
    into loan_violation_28
      select /*+Parallel(4)*/
       seq_LOAN_v28.nextval     as violation_id,
       BRNCH_COD,
       LOAN_MINOR_TYPE,
       CUSTOMER_NUM,
       LOAN_SERIAL,
       LOAN_TITLE,
       LOAN_MAJOR_TYPE,
       LOAN_MAJOR_DESC,
       ACCEPTED_AMNT,
       CONTRACT_BEGIN_DT,
       currdate                 as EFFECTIVE_DATE,
       LOAN_CURRENT_STATUS,
       LOAN_CURRENT_STATUS_DESC,
       ECONOMIC_UNIT_COD,
       ECONOMIC_UNIT_DESC,
       BANK_PAID_AMNT,
       null                     as user_changed_date,
       null                     as user_cod,
       1                        as user_changed_stat,
       null                     as comment_f,
       BANK_PAID_COUNT,
       ACC_BRANCH_COD,
       ACNT_TYPE_COD,
       ACNT_SERIAL
        from (select max(t.BRNCH_COD) as BRNCH_COD,
                     max(t.LOAN_MINOR_TYPE) as LOAN_MINOR_TYPE,
                     max(t.CUSTOMER_NUM) as CUSTOMER_NUM,
                     max(t.LOAN_SERIAL) as LOAN_SERIAL,
                     max(t.LOAN_TITLE) as LOAN_TITLE,
                     max(t.LOAN_MAJOR_TYPE) as LOAN_MAJOR_TYPE,
                     max(t.LOAN_MAJOR_DESC) as LOAN_MAJOR_DESC,
                     max(t.ACCEPTED_AMNT) as ACCEPTED_AMNT,
                     max(t.CONTRACT_BEGIN_DT) as CONTRACT_BEGIN_DT,
                     max(t.LOAN_CURRENT_STATUS) as LOAN_CURRENT_STATUS,
                     max(t.LOAN_CURRENT_STATUS_DESC) as LOAN_CURRENT_STATUS_DESC,
                     max(t.ECONOMIC_UNIT_COD) as ECONOMIC_UNIT_COD,
                     max(t.ECONOMIC_UNIT_DESC) as ECONOMIC_UNIT_DESC,
                     sum(f.AMOUNT) as BANK_PAID_AMNT,
                     count(*) as BANK_PAID_COUNT,
                     f.ACC_BRANCH_COD,
                     f.ACNT_TYPE_COD,
                     f.ACNT_SERIAL
                from dimloan t
               inner join sa_negin_iranzamin.lcact2lon d
                  on t.BRNCH_COD = d.abrnchcod
                 and t.LOAN_MINOR_TYPE = d.lnminortp
                 and t.CUSTOMER_NUM = d.cfcifno
                 and t.LOAN_SERIAL = d.crserial
               inner join factvoucherentry f
                  on d.acn_abrnchcod = f.ACC_BRANCH_COD
                 and d.atypcode = f.ACNT_TYPE_COD
                 and d.aiserial = f.ACNT_SERIAL
                left join loan_violation_28 lv28
                  on lv28.brnch_cod = t.BRNCH_COD
                 and lv28.loan_minor_type = t.LOAN_MINOR_TYPE
                 and lv28.customer_num = t.CUSTOMER_NUM
                 and lv28.loan_serial = t.LOAN_SERIAL
                 and lv28.contract_begin_dt = t.CONTRACT_BEGIN_DT
              
               where f.TRNOVER_TYPE = 0
                 and f.VOUCHER_DATE >= currdate
                 and f.VOUCHER_DATE < currdate + 5
                 and t.CONTRACT_BEGIN_DT = currdate
                 and f.AAPPCOD = 'L'
                 and f.ATRCODE = 14
                 and f.ATRSBCODE in (1, 2)
                 and f.REVERSE_COD = 0
                 and lv28.brnch_cod is null
               group by f.ACC_BRANCH_COD, f.ACNT_TYPE_COD, f.ACNT_SERIAL
              having count(*) > 1 and sum(abs(f.AMOUNT)) > max(t.ACCEPTED_AMNT));
  
    rowcnt := sql%rowcount;
    commit;
    OperationEnd := sysdate;
    insert into operation_Log
    values
      ('ins_Loan_v28',
       'loan_violation_28',
       'Insert',
       rowcnt,
       floor((OperationEnd - OperationStart) * 24 * 60 * 60),
       OperationStart,
       OperationEnd,
       currdate,
       sysdate,
       'ÃœÊ·  Œ·›«  Ê«„ 28');
    commit;
  end;

  --------------------------------V30

  --------------------------------V31

  --------------------------------V32

  --------------------------------V33

  ----------------------------main_violation
  procedure main_violation is
  begin
    ins_Loan_v1;
    ins_Loan_v2;
    ins_Loan_v3;
    ins_Loan_v4;
    ins_Loan_v6;
    ins_Loan_v9;
    ins_Loan_v19;
    ins_Loan_v22;
    ins_Loan_v24;
    select max(lv.effective_date) + 1
      into fromdate
      from loan_violation_23 lv;
    ins_Loan_v23(fromdate); --ezafe gardad soltani
  end;
begin
  t := 1;
end PKG_LOAN_VIOLATION
/


spool off
