using CPISData.Data;
using MonthBackup_FE.EAR.Provider;
using MonthBackup_FE.EAR_DEL.Provider;
using MonthBackup_FE.Helper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MonthBackup_FE.EAR_DEL.Service
{
    public class EarDeleteService
    {
        private static string OutputFolder = $"EarDel_{DateTime.Now.ToString("yyyyMM")}";

        public static void SetOutputFolder(string folder)
        {
            OutputFolder = folder;
        }

        /// <summary>
        /// 通用處理方法：根據 deleteMode 決定刪除或匯出
        /// </summary>
        private static void ProcessData(IFXTransaction tx, string sql, string fileName, Action<string> logCallback, string displayMessage)
        {
            logCallback?.Invoke(displayMessage);

            if (EarDeleteProvider.IsDeleteMode)
            {
                // 刪除模式
                string deleteSql = sql.Replace("SELECT *", "DELETE");
                int affectedRows = IfxDataAccess.ExecuteNonQuery(tx, deleteSql);
                logCallback?.Invoke($"刪除完成，影響筆數: {affectedRows}");
            }
            else
            {
                // 匯出模式
                DataTable result = IfxDataAccess.ExecuteDataTable(tx, sql);
                DataExporter.ExportData_Append(result, fileName, OutputFolder, logCallback);
            }
        }

        /// <summary>
        /// 處理 ntcenh - 需要特殊的計數邏輯
        /// </summary>
        public static void ProcessNtcenh(IFXTransaction tx, string entity, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke($"delete ntcenh ..... {entity} {transDate}");

            // 先查詢總計數
            string countAllSql = $@"SELECT COUNT(*)  as cnt
                                   FROM ntcenh 
                                   WHERE nenh_facility = 'ASSY' 
                                   AND nenh_entity = '{entity}'";
            DataTable countAllResult = IfxDataAccess.ExecuteDataTable(tx, countAllSql);
            int tmpCnt1 = countAllResult.Rows.Count > 0 ? Convert.ToInt32(countAllResult.Rows[0]["cnt"]) : 0;

            // 查詢當日計數
            string countDateSql = $@"SELECT COUNT(*)  as cnt
                                    FROM ntcenh 
                                    WHERE nenh_facility = 'ASSY' 
                                    AND nenh_entity = '{entity}' 
                                    AND nenh_trans_date = {transDate}";
            DataTable countDateResult = IfxDataAccess.ExecuteDataTable(tx, countDateSql);
            int tmpCnt2 = countDateResult.Rows.Count > 0 ? Convert.ToInt32(countDateResult.Rows[0]["cnt"]) : 0;

            // 如果當日計數 >= (總計數 - 2)，則跳過
            if (tmpCnt2 >= (tmpCnt1 - 2))
            {
                logCallback?.Invoke($"跳過 ntcenh: {entity} {transDate} (計數檢查)");
                return;
            }

            logCallback?.Invoke($"      ntcenh(entity)... {entity}  {transDate}");

            string sql = $@"SELECT * FROM ntcenh 
                           WHERE nenh_facility = 'ASSY' 
                           AND nenh_entity = '{entity}' 
                           AND nenh_trans_date = {transDate}";

            ProcessData(tx, sql, "ntcenh.995", logCallback, null);
        }
        public static void ProcessNtcenh(IFXTransaction tx, List<string> entityList, int transDate, Action<string> logCallback)
        {
            if (entityList == null || entityList.Count == 0) return;

            logCallback?.Invoke($"delete ntcenh ..... entities: {string.Join(",", entityList)} {transDate}");

            // 組合 IN 子句
            string entityInClause = "'" + string.Join("','", entityList) + "'";

            // 一次查詢：找出需要跳過的 entity（當日計數 >= 總計數-2）
            //    string skipCheckSql = $@"
            //SELECT t1.nenh_entity
            //FROM (
            //    SELECT nenh_entity, COUNT(*) as date_cnt
            //    FROM ntcenh
            //    WHERE nenh_facility = 'ASSY'
            //    AND nenh_entity IN ({entityInClause})
            //    AND nenh_trans_date = {transDate}
            //    GROUP BY nenh_entity
            //) t1
            //INNER JOIN (
            //    SELECT nenh_entity, COUNT(*) as total_cnt
            //    FROM ntcenh
            //    WHERE nenh_facility = 'ASSY'
            //    AND nenh_entity IN ({entityInClause})
            //    GROUP BY nenh_entity
            //) t2 ON t1.nenh_entity = t2.nenh_entity
            //WHERE t1.date_cnt >= (t2.total_cnt - 2)";

            //            string skipCheckSql = $@"
            //SELECT nenh_entity
            //FROM ntcenh
            //WHERE nenh_facility = 'ASSY'
            //AND nenh_entity IN ({entityInClause})
            //GROUP BY nenh_entity
            //HAVING COUNT(CASE WHEN nenh_trans_date = {transDate} THEN 1 END) >= (COUNT(*) - 2)";

            string skipCheckSql = $@"
    SELECT t.nenh_entity
    FROM ntcenh t
    INNER JOIN ntcent e ON t.nenh_entity = e.nent_entity
    WHERE t.nenh_facility = 'ASSY'
      AND e.nent_facility = 'ASSY'
    GROUP BY t.nenh_entity
    HAVING COUNT(CASE WHEN t.nenh_trans_date = {transDate} THEN 1 END) >= (COUNT(*) - 2)";

            DataTable skipResult = IfxDataAccess.ExecuteDataTable(tx, skipCheckSql,2000);
            HashSet<string> skipEntities = new HashSet<string>();
            foreach (DataRow row in skipResult.Rows)
            {
                string skipEntity = row["nenh_entity"].ToString();
                skipEntities.Add(skipEntity);
                logCallback?.Invoke($"跳過 ntcenh: {skipEntity} {transDate} (計數檢查)");
            }

            // 過濾掉需要跳過的 entity
            var validEntities = entityList.Where(e => !skipEntities.Contains(e)).ToList();

            if (validEntities.Count == 0)
            {
                logCallback?.Invoke("所有 entity 都被跳過");
                return;
            }

            // 組合有效的 entity IN 子句
            string validEntityInClause = "'" + string.Join("','", validEntities) + "'";

            logCallback?.Invoke($"      ntcenh(entities)... {string.Join(",", validEntities)}  {transDate}");

            // 一條 SQL 查詢所有符合條件的資料
            string sql = $@"SELECT * FROM ntcenh 
                   WHERE nenh_facility = 'ASSY' 
                   AND nenh_entity IN ({validEntityInClause})
                   AND nenh_trans_date = {transDate}";

            ProcessData(tx, sql, "ntcenh.995", logCallback, null);
        }

        public static void ProcessWipent(IFXTransaction tx, List<string> entityList, int transDate, Action<string> logCallback)
        {
            if (entityList == null || entityList.Count == 0) return;

            // 先過濾掉需要跳過的 entity
            var validEntities = entityList
                .Where(e => !EarDeleteProvider.ShouldSkipEntityDate(e, transDate))
                .ToList();

            // 記錄跳過的
            foreach (var entity in entityList.Except(validEntities))
            {
                logCallback($"跳過 wipent: {entity} {transDate}");
            }

            if (validEntities.Count == 0)
            {
                logCallback("所有 entity 都被跳過");
                return;
            }

            logCallback?.Invoke($"delete from wipent ... {string.Join(",", validEntities)}  {transDate}");

            // 組合 IN 子句
            string entityInClause = "'" + string.Join("','", validEntities) + "'";

            // 一條 SQL 處理所有 entity
            string sql = $@"SELECT * FROM wipent
                   WHERE went_facility = 'ASSY'
                   AND went_mac_id_or_ent IN ({entityInClause})
                   AND went_ntc_date = {transDate}";

            ProcessData(tx, sql, "wipent.995", logCallback, null);
        }
        /// <summary>
        /// 處理 wipent
        /// </summary>
        public static void ProcessWipent(IFXTransaction tx, string entity, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke($"delete from wipent ... {entity}  {transDate}");

            string sql = $@"SELECT * FROM wipent
                           WHERE went_facility = 'ASSY'
                           AND went_mac_id_or_ent = '{entity}'
                           AND went_ntc_date = {transDate}";

            ProcessData(tx, sql, "wipent.995", logCallback, null);
        }

        /// <summary>
        /// 處理 ent_job
        /// </summary>
        public static void ProcessEntJob(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM ent_job WHERE ejob_trans_date = {transDate}";
            ProcessData(tx, sql, "ent_job.995", logCallback, "delete ent_job .......");
        }

        /// <summary>
        /// 處理 backup_ejb
        /// </summary>
        public static void ProcessBackupEjb(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $@"SELECT * FROM backup_ejb 
                           WHERE bejb_facility = 'ASSY' 
                           AND bejb_trans_date = {transDate}";
            ProcessData(tx, sql, "backup_ejb.995", logCallback, "delete backup_ejb .......");
        }

        /// <summary>
        /// 處理 cyctime
        /// </summary>
        public static void ProcessCyctime(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM cyctime WHERE cdate = {transDate}";
            ProcessData(tx, sql, "cyctime.995", logCallback, "delete cyctime .......");
        }

        /// <summary>
        /// 處理 dacyc
        /// </summary>
        public static void ProcessDacyc(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM dacyc WHERE cdate = {transDate}";
            ProcessData(tx, sql, "dacyc.995", logCallback, "delete dacyc .......");
        }

        /// <summary>
        /// 處理 wbcyc
        /// </summary>
        public static void ProcessWbcyc(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM wbcyc WHERE cdate = {transDate}";
            ProcessData(tx, sql, "wbcyc.995", logCallback, "delete wbcyc .......");
        }

        /// <summary>
        /// 處理 mdcyc
        /// </summary>
        public static void ProcessMdcyc(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM mdcyc WHERE cdate = {transDate}";
            ProcessData(tx, sql, "mdcyc.995", logCallback, "delete mdcyc .......");
        }

        /// <summary>
        /// 處理 tfcyc
        /// </summary>
        public static void ProcessTfcyc(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM tfcyc WHERE cdate = {transDate}";
            ProcessData(tx, sql, "tfcyc.995", logCallback, "delete tfcyc .......");
        }

        /// <summary>
        /// 處理 wip_onhand
        /// </summary>
        public static void ProcessWipOnhand(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM wip_onhand WHERE wonh_date = {transDate}";
            ProcessData(tx, sql, "wip_onh.995", logCallback, "delete wip_onhand .....");
        }

        /// <summary>
        /// 處理 turn_hist 和 turn_lot_hist
        /// </summary>
        public static void ProcessTurnHist(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke("delete turn_lot_hist .....");

            // 先查詢要處理的 turn_hist 記錄
            string selectTurnHistSql = $@"SELECT turn_entity, turn_hist_seq 
                                         FROM turn_hist 
                                         WHERE turn_end_date = {transDate}";

            DataTable turnHistData = IfxDataAccess.ExecuteDataTable(tx, selectTurnHistSql);

            foreach (DataRow row in turnHistData.Rows)
            {
                string tEntity = row["turn_entity"].ToString().Trim();
                int tHistSeq = Convert.ToInt32(row["turn_hist_seq"]);

                string sql = $@"SELECT * FROM turn_lot_hist 
                               WHERE turn_entity = '{tEntity}' 
                               AND turn_hist_seq = {tHistSeq}";

                if (EarDeleteProvider.IsDeleteMode)
                {
                    string deleteSql = $@"DELETE FROM turn_lot_hist 
                                         WHERE turn_entity = '{tEntity}' 
                                         AND turn_hist_seq = {tHistSeq}";
                    IfxDataAccess.ExecuteNonQuery(tx, deleteSql);
                }
                else
                {
                    DataTable result = IfxDataAccess.ExecuteDataTable(tx, sql);
                    DataExporter.ExportData_Append(result, "turn_lot_hist.995", OutputFolder, logCallback);
                }
            }

            // 處理 turn_hist
            logCallback?.Invoke("delete turn_hist .....");
            string turnHistSql = $"SELECT * FROM turn_hist WHERE turn_end_date = {transDate}";
            ProcessData(tx, turnHistSql, "turn_hist.995", logCallback, null);
        }

        /// <summary>
        /// 處理 csi_file 和 csj_file
        /// </summary>
        public static void ProcessCsiCsjFile(IFXTransaction tx, string receDate, Action<string> logCallback)
        {
            logCallback?.Invoke($"delete csj_file: {receDate}");

            // 先查詢 csi_file 的 receipt_no
            string selectCsiSql = $@"SELECT receipt_no 
                                    FROM csi_file 
                                    WHERE receipt_date = '{receDate}'";

            DataTable csiData = IfxDataAccess.ExecuteDataTable(tx, selectCsiSql);

            foreach (DataRow row in csiData.Rows)
            {
                string receDocNo = row["receipt_no"].ToString().Trim();

                string sql = $@"SELECT * FROM csj_file WHERE receipt_no = '{receDocNo}'";

                if (EarDeleteProvider.IsDeleteMode)
                {
                    string deleteSql = $@"DELETE FROM csj_file WHERE receipt_no = '{receDocNo}'";
                    IfxDataAccess.ExecuteNonQuery(tx, deleteSql);
                }
                else
                {
                    DataTable result = IfxDataAccess.ExecuteDataTable(tx, sql);
                    DataExporter.ExportData_Append(result, "csj_file.995", OutputFolder, logCallback);
                }
            }

            // 處理 csi_file
            logCallback?.Invoke($"delete csi_file: {receDate}");
            string csiFileSql = $@"SELECT * FROM csi_file WHERE receipt_date = '{receDate}'";
            ProcessData(tx, csiFileSql, "csi_file.995", logCallback, null);
        }

        /// <summary>
        /// 處理 curing_h
        /// </summary>
        public static void ProcessCuringH(IFXTransaction tx, DateTime date, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM curing_h WHERE start_date = TO_DATE('{date:yyyy/MM/dd}', '%Y/%m/%d')";
            ProcessData(tx, sql, "curing_h.995", logCallback, $"delete curing_h: {date:yyyyMMdd}");
        }

        /// <summary>
        /// 處理 curing_d
        /// </summary>
        public static void ProcessCuringD(IFXTransaction tx, DateTime date, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM curing_d WHERE start_date = TO_DATE('{date:yyyy/MM/dd}', '%Y/%m/%d')";
            ProcessData(tx, sql, "curing_d.995", logCallback, $"delete curing_d: {date:yyyyMMdd}");
        }

        /// <summary>
        /// 處理 lot_attr_rec
        /// </summary>
        public static void ProcessLotAttrRec(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM lot_attr_rec WHERE larc_trans_date = {transDate}";
            ProcessData(tx, sql, "lot_attr_rec.995", logCallback, "delete lot_attr_rec .....");
        }

        /// <summary>
        /// 處理 loc_wb_hist
        /// </summary>
        public static void ProcessLocWbHist(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM loc_wb_hist WHERE trans_date = {transDate}";
            ProcessData(tx, sql, "loc_wb_hist.995", logCallback, "delete loc_wb_hist .....");
        }

        /// <summary>
        /// 處理 ent_attr_rec_hist
        /// </summary>
        public static void ProcessEntAttrRecHist(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM ent_attr_rec_hist WHERE earh_trans_date = {transDate}";
            ProcessData(tx, sql, "ent_attr_rec_hist.995", logCallback, "delete ent_attr_rec_hist .....");
        }

        /// <summary>
        /// 處理 lf_st_hist
        /// </summary>
        public static void ProcessLfStHist(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM lf_st_hist WHERE trans_date = {transDate}";
            ProcessData(tx, sql, "lf_st_hist.995", logCallback, "delete lf_st_hist .....");
        }

        /// <summary>
        /// 處理 lf_st_extra
        /// </summary>
        public static void ProcessLfStExtra(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM lf_st_extra WHERE trans_date = {transDate}";
            ProcessData(tx, sql, "lf_st_extra.995", logCallback, "delete lf_st_extra .....");
        }

        /// <summary>
        /// 處理 epoxy_hist
        /// </summary>
        public static void ProcessEpoxyHist(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM epoxy_hist WHERE trans_date = {transDate}";
            ProcessData(tx, sql, "epoxy_hist.995", logCallback, "delete epoxy_hist .....");
        }

        /// <summary>
        /// 處理 md_cp_hist
        /// </summary>
        public static void ProcessMdCpHist(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM md_cp_hist WHERE trans_date = {transDate}";
            ProcessData(tx, sql, "md_cp_hist.995", logCallback, "delete md_cp_hist .....");
        }

        /// <summary>
        /// 處理 wafer_id_rec
        /// </summary>
        public static void ProcessWaferIdRec(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM wafer_id_rec WHERE trans_date = {transDate}";
            ProcessData(tx, sql, "wafer_id_rec.995", logCallback, "delete wafer_id_rec .....");
        }

        /// <summary>
        /// 處理 special_rllt
        /// </summary>
        public static void ProcessSpecialRllt(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM special_rllt WHERE sprl_trans_date = {transDate}";
            ProcessData(tx, sql, "special_rllt.995", logCallback, "delete special_rllt .....");
        }

        /// <summary>
        /// 處理 swr_lot
        /// </summary>
        public static void ProcessSwrLot(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM swr_lot WHERE swr_trans_date = {transDate}";
            ProcessData(tx, sql, "swr_lot.995", logCallback, "delete swr_lot .....");
        }

        /// <summary>
        /// 處理 efrm 相關表格
        /// </summary>
        public static void ProcessEfrmTables(IFXTransaction tx, DateTime date, Action<string> logCallback)
        {
            logCallback?.Invoke("delete efrm .....");

            string sql = @"SELECT DISTINCT LOWER(rec_table_name) 
                          FROM efrm_form_main 
                          WHERE rec_table_name IS NOT NULL 
                          AND LENGTH(rec_table_name) != 0 
                          AND rec_table_name NOT LIKE 'M%' 
                          AND rec_table_name NOT LIKE 'EFRMAPG306%'";

            DataTable tables = IfxDataAccess.ExecuteDataTable(tx, sql);

            foreach (DataRow row in tables.Rows)
            {
                string tableName = row[0].ToString().Trim();
                string dynamicSql = $"SELECT * FROM {tableName} WHERE form_key_1 = TO_DATE('{date:yyyy/MM/dd}', '%Y/%m/%d')";

                try
                {
                    if (EarDeleteProvider.IsDeleteMode)
                    {
                        string deleteSql = $"DELETE FROM {tableName} WHERE form_key_1 = TO_DATE('{date:yyyy/MM/dd}', '%Y/%m/%d')";
                        int affectedRows = IfxDataAccess.ExecuteNonQuery(tx, deleteSql);
                        logCallback?.Invoke($"{tableName} 刪除完成，影響筆數: {affectedRows}");
                    }
                    else
                    {
                        DataTable result = IfxDataAccess.ExecuteDataTable(tx, dynamicSql);
                        DataExporter.ExportData_Append(result, $"{tableName}.995", OutputFolder, logCallback);
                    }
                }
                catch (Exception ex)
                {
                    logCallback?.Invoke($"處理 {tableName} 時發生錯誤: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// 處理 ent_lot
        /// </summary>
        public static void ProcessEntLot(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM ent_lot WHERE end_date = {transDate}";
            ProcessData(tx, sql, "ent_lot.995", logCallback, "delete ent_lot .....");
        }

        /// <summary>
        /// 處理 ent_lot1
        /// </summary>
        public static void ProcessEntLot1(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM ent_lot1 WHERE end_date = {transDate}";
            ProcessData(tx, sql, "ent_lot1.995", logCallback, "delete ent_lot1 .....");
        }

        /// <summary>
        /// 處理 ent_use
        /// </summary>
        public static void ProcessEntUse(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM ent_use WHERE start_date = {transDate}";
            ProcessData(tx, sql, "ent_use.995", logCallback, "delete ent_use .....");
        }

        /// <summary>
        /// 處理 sta_transa
        /// </summary>
        public static void ProcessStaTransa(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            string sql = $"SELECT * FROM sta_transa WHERE cost_trans_date = {transDate}";
            ProcessData(tx, sql, "sta_a.995", logCallback, "delete sta_transa .....");
        }
    }
}