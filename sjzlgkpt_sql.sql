--创建数组对象，供存储过程解析字符串时使用
create or replace type sjzl_arryList is table of varchar2(4000);


--创建数据质量游标，用于返回检测的结果供java代码处理
CREATE OR REPLACE PACKAGE SJZLGKPT
AS
    type RESULT_TYPE is ref cursor;

    END SJZLGKPT;


--用于检测 是否为日期格式的值，是则返回1 ，否则返回0
CREATE OR REPLACE FUNCTION sjzl_is_date(parmin VARCHAR2) RETURN NUMBER IS
  val DATE;
BEGIN
  val := TO_DATE(NVL(parmin, 'a'), 'yyyy-mm-dd hh24:mi:ss');
  RETURN 1;
EXCEPTION
  WHEN OTHERS THEN
    RETURN 0;
END;

--用于检测 是否为数字格式的值，是则返回1 ，否则返回0
CREATE OR REPLACE FUNCTION sjzl_is_number(parmin VARCHAR2) RETURN NUMBER IS
  val NUMBER;
BEGIN
  val := TO_NUMBER(NVL(parmin, 'a'));
  RETURN 1;
EXCEPTION
  WHEN OTHERS THEN
    RETURN 0;
END;

--将字符串按指定的分隔符解析字符串，并存放到数组中
create or replace function sjzl_split_string(pi_str in varchar2, pi_separator in varchar2) --创建函数
return sjzl_arryList
 is
  v_char_table sjzl_arryList;
  v_temp varchar2(4000);
  v_element varchar2(4000);
begin
   v_char_table := sjzl_arryList();
   v_temp := pi_str;
   while instr(v_temp, pi_separator) > 0
    loop
        v_element := substr(v_temp,1,instr(v_temp, pi_separator)-1);
        v_temp := substr(v_temp, instr(v_temp,pi_separator)+ length(pi_separator) , length(v_temp));
        v_char_table.extend;
        v_char_table(v_char_table.count) := v_element;

   end loop;
    v_char_table.extend;
    v_char_table(v_char_table.count) := v_temp;
   return v_char_table;
end sjzl_split_string;

--检测非法日期格式的记录
CREATE OR REPLACE PROCEDURE SP_SJZL_CHK_illegalDate(output_rs        out SJZLGKPT.RESULT_TYPE,
                                               output_col_num   out int,
                                               check_tableName  in varchar2,
                                               check_column     in varchar2,
                                               output_column    in varchar2,
                                               increment_column in varchar2)

 AS
  sql_str         varchar2(2000);
  output_col_list sjzl_arryList;
BEGIN
  output_col_list := sjzl_split_string(output_column, ',');
  output_col_num  := output_col_list.count;
  sql_str         := 'select  ' || output_column || ' from ' ||
                     check_tableName || ' where  sjzl_is_date(' ||check_column ||
                     ')=0 ';--返回非日期格式的记录
  if (increment_column is not null ) then
       sql_str := sql_str ||increment_column ;
  end if;
  OPEN output_rs FOR sql_str;
END SP_SJZL_CHK_illegalDate;

--检测非法日期格式的记录
CREATE OR REPLACE PROCEDURE SP_SJZL_CHK_illegalDate(output_rs        out SJZLGKPT.RESULT_TYPE,
                                               output_col_num   out int,
                                               check_tableName  in varchar2,
                                               check_column     in varchar2,
                                               output_column    in varchar2,
                                               increment_column in varchar2)

 AS
  sql_str         varchar2(2000);
  output_col_list sjzl_arryList;
BEGIN
  output_col_list := sjzl_split_string(output_column, ',');
  output_col_num  := output_col_list.count;
  sql_str         := 'select  ' || output_column || ' from ' ||
                     check_tableName || ' where  sjzl_is_date(' ||check_column ||
                     ')=0 ';--返回非日期格式的记录
  if (increment_column is not null ) then
       sql_str := sql_str ||increment_column ;
  end if;
  OPEN output_rs FOR sql_str;
END SP_SJZL_CHK_illegalDate;


--检测 为空串的数据格式记录
CREATE OR REPLACE PROCEDURE SP_SJZL_CHK_ISNBLANK(output_rs        out SJZLGKPT.RESULT_TYPE,
                                                 output_col_num   out int,
                                                 check_tableName  in varchar2,
                                                 check_column     in varchar2,
                                                 output_column    in varchar2,
                                                 increment_column in varchar2)

 AS
  sql_str         varchar2(2000);
  output_col_list sjzl_arryList;
BEGIN
  output_col_list := sjzl_split_string(output_column, ',');
  output_col_num  := output_col_list.count;
  sql_str         := 'select  ' || output_column || ' from ' ||
                     check_tableName || ' where trim(' || check_column ||
                     ')=''''';
  if (increment_column is not null) then
    sql_str := sql_str || increment_column;
  end if;
  OPEN output_rs FOR sql_str;
END SP_SJZL_CHK_ISNBLANK;
--检测为null值的记录
CREATE OR REPLACE PROCEDURE SP_SJZL_CHK_ISNULL(output_rs        out SJZLGKPT.RESULT_TYPE,
                                               output_col_num   out int,
                                               check_tableName  in varchar2,
                                               check_column     in varchar2,
                                               output_column    in varchar2,
                                               increment_column in varchar2)

 AS
  sql_str         varchar2(2000);
  output_col_list sjzl_arryList;
BEGIN
  output_col_list := sjzl_split_string(output_column, ',');
  output_col_num  := output_col_list.count;
  sql_str         := 'select  ' || output_column || ' from ' ||
                     check_tableName || ' where ' || check_column ||
                     ' is null ';
  if (increment_column is not null) then
    sql_str := sql_str || increment_column;
  end if;
  OPEN output_rs FOR sql_str;
END SP_SJZL_CHK_ISNULL;


--返回检测表_检测字段最大值对应的记录
CREATE OR REPLACE PROCEDURE SP_SJZL_CHK_MaxValue(output_rs        out SJZLGKPT.RESULT_TYPE,
                                                 output_col_num   out int,
                                                 check_tableName  in varchar2,
                                                 check_column     in varchar2,
                                                 output_column    in varchar2,
                                                 increment_column in varchar2)

 AS
  sql_str         varchar2(2000);
  output_col_list sjzl_arryList;
BEGIN
  output_col_list := sjzl_split_string(output_column, ',');
  output_col_num  := output_col_list.count;
  sql_str         := 'select  ' || output_column || ' from ( select ' ||
                     output_column || ',row_number() over (order by ' ||
                     check_column || ' desc ) rn from ' || check_tableName ||
                     ' where ' || check_column ||
                     ' is not null ) where rn=1 ';
  if (increment_column is not null) then
    sql_str := sql_str || increment_column;
  end if;
  OPEN output_rs FOR sql_str;
END SP_SJZL_CHK_MaxValue;


--返回检测表_检测字段最小值对应的记录
CREATE OR REPLACE PROCEDURE SP_SJZL_CHK_MinValue(output_rs        out SJZLGKPT.RESULT_TYPE,
                                                 output_col_num   out int,
                                                 check_tableName  in varchar2,
                                                 check_column     in varchar2,
                                                 output_column    in varchar2,
                                                 increment_column in varchar2)

 AS
  sql_str         varchar2(2000);
  output_col_list sjzl_arryList;
BEGIN
  output_col_list := sjzl_split_string(output_column, ',');
  output_col_num  := output_col_list.count;
  sql_str         := 'select  ' || output_column || ' from ( select ' ||
                     output_column || ',row_number() over (order by ' ||
                     check_column || '  ) rn from ' || check_tableName ||
                     ' where ' || check_column ||
                     ' is not null) where rn=1 ';
  if (increment_column is not null) then
    sql_str := sql_str || increment_column;
  end if;
  OPEN output_rs FOR sql_str;
END SP_SJZL_CHK_MinValue;


