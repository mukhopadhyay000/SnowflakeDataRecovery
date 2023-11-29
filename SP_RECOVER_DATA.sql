-- Stored Procedure to recover data in RAW/STG/TRFN tables w/ to the query id 
-- Proc Name : SP_RECOVER_DATA
-- Input Parameters
--      A. Database Name
--		B. Source Schema Name
--		C. Target Schema Name
--		F. Load Date of the tables
-- Author : Anup Mukhopadhyay (IBM Consultant) 

/* ####################################################################
Code Description:
1. Checks the feaseability of recovery w/ to the difference between the current date and mentioned recover date should not be more than the database retention time.
2. Join the ING_PX_LOAD_LOG and recover config table to get the table names to recover, the stream name to reset and the query id on the basis of which recovery will be done.
3. truncate the RAW/STG/TRFN tables. Recover the data. Reset the stream.
Changes:

#################################################################### */

CREATE OR REPLACE PROCEDURE SP_RECOVER_DATA(P_DB_NAME varchar, P_SRC_SCHMA_NAME varchar, P_TGT_SCHMA_NAME varchar, P_LOAD_DT varchar)
RETURNS TEXT NOT NULL
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS

$$
		// define variables
		
		var qryid = [];
		var tbl = [];
        var raw_tbl = [];
        var stg_tbl = [];
        var trfn_tbl = [];
        var strm_nm = [];
		var split_str = [];
        var trunc_raw_qry_str = [];
        var trunc_stg_qry_str = [];
        var trunc_trfn_qry_str = [];
        var ins_raw_qry_str = [];
        var ins_stg_qry_str = [];
        var ins_trfn_qry_str = [];
        var rst_strm_qry_str = [];
        var ins_raw_qry_str = [];

    try {
        var v_step = 0; // check the recover feaseability w/ to the set time travel of the database.

        var tt_qry = `select retention_time from information_schema.databases where database_name = '${P_DB_NAME}';`;
        var tt_stmt = snowflake.createStatement({sqlText: tt_qry});
        var tt_exec = tt_stmt.execute();
            tt_exec.next();
                var tt_val = tt_exec.getColumnValue(1);
        
        var dt_qry = `select to_char(current_date());`;
        var dt_stmt = snowflake.createStatement({sqlText: dt_qry});
        var dt_exec = dt_stmt.execute();
            dt_exec.next();
                var curr_dt = dt_exec.getColumnValue(1);

        var d1 = new Date(`${P_LOAD_DT}`);
        var d2 = new Date(curr_dt);
        
        var diff = d2.getTime() - d1.getTime();
        var datediff = diff/(1000*60*60*24);

        var sql_cnt_qry = `select count(*) from (
                         select Y.table_nm, Y.queryid, Y.load_ts, X.raw_tbl_nm, X.stg_tbl_nm, X.trfn_tbl_nm, X.strm_nm, X.recvr_flg from C_ING_PX_RECVR_CONFIG X
                            inner join (select table_nm, queryid, load_ts, RANK() OVER (PARTITION BY table_nm ORDER BY load_ts DESC) AS rnk_col from ING_PX_LOAD_LOG where load_ts like '${P_LOAD_DT}%') Y
                                on Y.table_nm = X.raw_tbl_nm
                                and X.recvr_flg = 'T'
                                and Y.rnk_col = 1);`;
		
        var sql_cnt_stmt = snowflake.createStatement({sqlText: sql_cnt_qry});
		var sql_cnt_exec = sql_cnt_stmt.execute();
		    sql_cnt_exec.next()
                var count = sql_cnt_exec.getColumnValue(1);
        
    }
	catch(err) {
		return ("Error in step: " + v_step + "; Messege :" + err);
    }

        if ((datediff < tt_val) || (datediff == tt_val)) {
            
            try {
                var v_step = 10; // identify the tables, query id to recover the data before the specified query id 

                var sql_qry1 = `select distinct Y.table_nm, Y.queryid, Y.load_ts, X.raw_tbl_nm, X.stg_tbl_nm, X.trfn_tbl_nm, X.strm_nm, X.recvr_flg from C_ING_PX_RECVR_CONFIG X
                                    inner join (select table_nm, queryid, load_ts, RANK() OVER (PARTITION BY table_nm ORDER BY load_ts DESC) AS rnk_col from ING_PX_LOAD_LOG where load_ts like '${P_LOAD_DT}%') Y
                                        on Y.table_nm = X.raw_tbl_nm
                                        and X.recvr_flg = 'T'
                                        and Y.rnk_col = 1;`;
                
                var sql_stmt1 = snowflake.createStatement({sqlText: sql_qry1});
                var sql_exec1 = sql_stmt1.execute();
                
                    while (sql_exec1.next()) {
                        tbl.push(sql_exec1.getColumnValue(1));
                        qryid.push(sql_exec1.getColumnValue(2));
                        raw_tbl.push(sql_exec1.getColumnValue(4));
                        stg_tbl.push(sql_exec1.getColumnValue(5));
                        trfn_tbl.push(sql_exec1.getColumnValue(6));
                        strm_nm.push(sql_exec1.getColumnValue(7));                
                    }
                    
                    var x = tbl.toString(); 
                    var split_tbl = x.split(',');

                    var y = qryid.toString(); 
                    var split_qryid = y.split(',');
                    
                    var z = stg_tbl.toString();
                    var split_stg_tbl = z.split(',');

                    var k = trfn_tbl.toString();
                    var split_trfn_tbl = k.split(',');

                    var p = raw_tbl.toString();
                    var split_raw_tbl = p.split(',');

                    var s = strm_nm.toString();
                    var split_strm_nm = s.split(',');
            }
            
            catch(err) {
                return ("Error in step: " + v_step + "; Messege :" + err);
            }

            try {
                var v_step = 20; // truncate and recover the raw/stg tables. reset the stream.
                    
                    var i;
                        for (i = 0; i < split_stg_tbl.length; i++) {
                            trunc_raw_qry_str.push('truncate table ' + P_SRC_SCHMA_NAME + "." + split_raw_tbl[i] + ";");
                            trunc_stg_qry_str.push('truncate table ' + P_SRC_SCHMA_NAME + "." + split_stg_tbl[i] + ";");
                            rst_strm_qry_str.push("insert into " + P_SRC_SCHMA_NAME + "." + split_raw_tbl[i] + " select *exclude(METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID) from " + split_strm_nm[i] + " where 1=0;");
                            ins_raw_qry_str.push("insert into " + P_SRC_SCHMA_NAME + "." + split_raw_tbl[i] + " select * from " + P_SRC_SCHMA_NAME + "." + split_raw_tbl[i] + " before (statement => '" + split_qryid[i] + "');");
                            ins_stg_qry_str.push("insert into " + P_SRC_SCHMA_NAME + "." + split_stg_tbl[i] + " select * from " + P_SRC_SCHMA_NAME + "." + split_stg_tbl[i] + " before (statement => '" + split_qryid[i] + "');");
                        }
                            var trunc_raw_qry = trunc_raw_qry_str.join('|');
                            var trunc_raw_qry_split = trunc_raw_qry.split('|');

                            var trunc_stg_qry = trunc_stg_qry_str.join('|');
                            var trunc_stg_qry_split = trunc_stg_qry.split('|');

                            var ins_raw_qry = ins_raw_qry_str.join('|');
                            var ins_raw_qry_split = ins_raw_qry.split('|');
                            
                            var ins_stg_qry = ins_stg_qry_str.join('|');
                            var ins_stg_qry_split = ins_stg_qry.split('|');

                            var rst_strm_qry = rst_strm_qry_str.join('|');
                            var rst_strm_qry_split = rst_strm_qry.split('|');
                                           
                    var j;
                        for (j = 0; j < trunc_stg_qry_split.length; j++) {
                            var tr_j = trunc_raw_qry_split[j];                    
                            var ts_j = trunc_stg_qry_split[j];
                            var ir_j = ins_raw_qry_split[j];
                            var is_j = ins_stg_qry_split[j];
                            var s_j = rst_strm_qry_split[j];
                            
                            var trunc_rqry1 = tr_j;
                            var trunc_rstmt1 = snowflake.createStatement({sqlText: trunc_rqry1});
                            var trunc_rexec1 = trunc_rstmt1.execute();
         
                            var trunc_sqry1 = ts_j;
                            var trunc_sstmt1 = snowflake.createStatement({sqlText: trunc_sqry1});
                            var trunc_sexec1 = trunc_sstmt1.execute();

                            var ins_rqry1 = ir_j;
                            var ins_rstmt1 = snowflake.createStatement({sqlText: ins_rqry1});
                            var ins_rexec1 = ins_rstmt1.execute();
                            
                            var ins_sqry1 = is_j;
                            var ins_sstmt1 = snowflake.createStatement({sqlText: ins_sqry1});
                            var ins_sexec1 = ins_sstmt1.execute();

                            var strm_qry1 = s_j;
                            var strm_stmt1 = snowflake.createStatement({sqlText: strm_qry1});
                            var strm_exec1 = strm_stmt1.execute();
                        }
                var messege1 = "RAW/STG tables truncated and recovered successfully. STRMS reset successful";
            }
            
            catch(err) {
                return ("Error in step: " + v_step + "; Messege :" + err);
            }
                 
            
            try {
                var v_step = 30; // truncate and recover the trfn tables

                     var m;
                        for (m = 0; m < split_trfn_tbl.length; m++) {
                            trunc_trfn_qry_str.push('truncate table ' + P_TGT_SCHMA_NAME + "." + split_trfn_tbl[m] + ";");
                            ins_trfn_qry_str.push("insert into " + P_TGT_SCHMA_NAME + "." + split_trfn_tbl[m] + " select * from " + P_TGT_SCHMA_NAME + "." + split_trfn_tbl[m] + " before (statement => '" + split_qryid[m] + "');");
                        }
                            
                            var trunc_trfn_qry = trunc_trfn_qry_str.join('|');
                            var trunc_trfn_qry_split = trunc_trfn_qry.split('|');

                            var ins_trfn_qry = ins_trfn_qry_str.join('|');
                            var ins_trfn_qry_split = ins_trfn_qry.split('|');
                
                    var n;
                        for (n = 0; n < trunc_trfn_qry_split.length; n++) {
                            var t_n = trunc_trfn_qry_split[n];
                            var i_n = ins_trfn_qry_split[n];
                                    
                            var trunc_qry2 = t_n;
                            var trunc_stmt2 = snowflake.createStatement({sqlText: trunc_qry2});
                            var trunc_exec2 = trunc_stmt2.execute();

                            var ins_qry2 = i_n;
                            var ins_stmt2 = snowflake.createStatement({sqlText: ins_qry2});
                            var ins_exec2 = ins_stmt2.execute();
                        }
                var messege2 = "TRFN tables truncated and recovered successfully.";
            return messege1 + ";" + messege2;
            }
            
            catch(err) {
                return ("Error in step: " + v_step + "; Messege :" + err);
            }
        }
        else {
            var messege = "Database retention time is: " + tt_val + ". Recovery days mentioned : " + datediff + ". Recovery not possible.";
            throw messege;
        }
$$;