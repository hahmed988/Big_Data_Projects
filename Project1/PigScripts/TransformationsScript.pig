departments = LOAD 'insofe_empdb.departments' USING org.apache.hive.hcatalog.pig.HCatLoader();
dept_emp = LOAD 'insofe_empdb.dept_emp' USING org.apache.hive.hcatalog.pig.HCatLoader();
active_dept_emp = FILTER dept_emp BY to_date == '9999-01-01';
dept_manager = LOAD 'insofe_empdb.dept_manager' USING org.apache.hive.hcatalog.pig.HCatLoader();
active_dept_manager = FILTER dept_manager BY to_date == '9999-01-01';
employees = LOAD 'insofe_empdb.employees' USING org.apache.hive.hcatalog.pig.HCatLoader();
salaries = LOAD 'insofe_empdb.salaries' USING org.apache.hive.hcatalog.pig.HCatLoader();
active_salaries = FILTER salaries BY to_date == '9999-01-01';
titles = LOAD 'insofe_empdb.titles' USING org.apache.hive.hcatalog.pig.HCatLoader();
active_titles = FILTER titles BY to_date == '9999-01-01';


dept_rel = join departments by (dept_no), active_dept_manager by (dept_no);
dept_rel2 = join dept_rel by (active_dept_manager::emp_no), employees by (emp_no);
final_dept_rel = FOREACH dept_rel2 GENERATE $0 AS dept_no, $1 AS dept_name, $5 AS manager_emp_no, $6 AS manager_from_date, $7 AS manager_to_date, $10 AS manager_birth_date, $11 AS manager_first_name, $12 AS manager_last_name, $13 AS manager_gender, $14 AS manager_hire_date;

emp_dept_rel = join active_dept_emp by (emp_no), employees by (emp_no);
final_emp_dept_rel = FOREACH emp_dept_rel GENERATE $6 AS emp_no, $7 AS birth_date, $8 AS first_name, $9 AS last_name, $10 AS gender, $11 AS hire_date, $2 AS dept_no, $3 AS dept_from_date, $4 AS dept_to_date;

active_emp_rel1 = join final_emp_dept_rel by (dept_no), final_dept_rel by (dept_no); 
sal_title_rel = join active_titles by (emp_no), active_salaries by (emp_no);
final_sal_title_rel = FOREACH sal_title_rel GENERATE $1 AS emp_no, $8 AS salary, $9 AS salary_from_date, $10 AS salary_to_date, $2 AS title, $3 AS title_from_date, $4 AS title_to_date;

active_emp_rel2 = join active_emp_rel1 by (final_emp_dept_rel::emp_no), final_sal_title_rel by (emp_no);
active_employees_data = FOREACH active_emp_rel2 GENERATE $0 AS emp_no, $2 AS first_name, $3 AS last_name, $4 AS gender, $1 AS birth_date, $5 AS hire_date, $6 AS dept_no, $10 AS dept_name, $7 AS dept_from_date, $20 AS salary, $21 AS salary_from_date, $23 AS title, $24 AS title_from_date, $11 AS manager_emp_no, $15 AS manager_first_name, $16 AS manager_last_name, $17 AS manager_gender, $14 AS manager_birth_date, $18 AS manager_hire_date, $12 AS manager_from_date, YearsBetween(ToDate(ToString(CurrentTime(),'yyyy-MM-dd')), ToDate($1,'yyyy-MM-dd')) AS age, YearsBetween(ToDate(ToString(CurrentTime(),'yyyy-MM-dd')), ToDate($5,'yyyy-MM-dd')) AS tenure, YearsBetween(ToDate(ToString(CurrentTime(),'yyyy-MM-dd')), ToDate($14,'yyyy-MM-dd')) AS manager_age, YearsBetween(ToDate(ToString(CurrentTime(),'yyyy-MM-dd')), ToDate($18,'yyyy-MM-dd')) AS manager_tenure, YearsBetween(ToDate(ToString(CurrentTime(),'yyyy-MM-dd')), ToDate($21,'yyyy-MM-dd')) AS salary_since, YearsBetween(ToDate(ToString(CurrentTime(),'yyyy-MM-dd')), ToDate($24,'yyyy-MM-dd')) AS role_since;

STORE active_employees_data INTO '/user/1289B29/datasets/employeesdb/active_employees_data/' USING PigStorage(',');