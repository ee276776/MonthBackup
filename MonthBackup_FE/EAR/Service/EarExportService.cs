using CPISData.Data;
using MonthBackup_FE.Helper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MonthBackup_FE.EAR.Service
{
    public class EarExportService
    {
        private static string OutputFolder = $"Ear_{DateTime.Now.ToString("yyyyMM")}";

        public static void SetOutputFolder(string folder)
        {
            OutputFolder = folder;
        }

        /// <summary>
        /// 匯出 ntcenh
        /// </summary>
        public static void ExportNtcenh(IFXTransaction tx, string entity, int transDate,Action<string>logCallback)
        {
            Console.WriteLine($"unload ntcenh ..... {entity} {transDate}");

            string sql = $@"SELECT * FROM ntcenh 
                           WHERE nenh_facility = 'ASSY' 
                           AND nenh_entity = '{entity}' 
                           AND nenh_trans_date = {transDate}";

            DataTable result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "ntcenh.995", OutputFolder, logCallback);
        }

        public static void ExportWipent(IFXTransaction tx, string entity, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke($"unload wipent ..... {entity} {transDate}");
            string sql = $@"SELECT * FROM wipent
                        WHERE went_facility = 'ASSY'
                        AND went_mac_id_or_ent = '{entity}'
                        AND went_ntc_date = {transDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "wipent.995", OutputFolder, logCallback);
        }

        /// <summary>
        /// 處理 ntcenh 匯出（使用 IN 查詢所有 entity）
        /// </summary>
        public static void ProcessNtcenh(IFXTransaction tx, List<string> entityList, int transDate, Action<string> logCallback)
        {
            logCallback($"unload ntcenh ..... transDate={transDate}");

            // 將 entityList 轉換為 SQL IN 語句格式
            string entityInClause = string.Join(",", entityList.Select(e => $"'{e}'"));

            string sql = $@"SELECT * FROM ntcenh 
                   WHERE nenh_facility = 'ASSY' 
                   AND nenh_entity IN ({entityInClause})
                   AND nenh_trans_date = {transDate}";

            DataTable result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "ntcenh.995", OutputFolder, logCallback);

            logCallback($"[V] ntcenh 匯出完成 (共 {result.Rows.Count} 筆)\n");
        }

        /// <summary>
        /// 處理 wipent 匯出（使用 IN 查詢所有 entity）
        /// </summary>
        public static void ProcessWipent(IFXTransaction tx, List<string> entityList, int transDate, Action<string> logCallback)
        {
            logCallback($"unload wipent ..... transDate={transDate}");

            // 將 entityList 轉換為 SQL IN 語句格式
            string entityInClause = string.Join(",", entityList.Select(e => $"'{e}'"));

            string sql = $@"SELECT * FROM wipent
                   WHERE went_facility = 'ASSY'
                   AND went_mac_id_or_ent IN ({entityInClause})
                   AND went_ntc_date = {transDate}";

            DataTable result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "wipent.995", OutputFolder, logCallback);

            logCallback($"[V] wipent 匯出完成 (共 {result.Rows.Count} 筆)\n");
        }



        public static void ExportEntJob(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload ent_job .......");
            string sql = $"SELECT * FROM ent_job WHERE ejob_trans_date = {transDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "ent_job.995", OutputFolder, logCallback);
        }

        public static void ExportCyctime(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload cyctime .......");
            string sql = $"SELECT * FROM cyctime WHERE cdate = {transDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "cyctime.995", OutputFolder, logCallback);
        }

        public static void ExportDacyc(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload dacyc .......");
            string sql = $"SELECT * FROM dacyc WHERE cdate = {transDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "dacyc.995", OutputFolder, logCallback);
        }

        public static void ExportWbcyc(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload wbcyc .......");
            string sql = $"SELECT * FROM wbcyc WHERE cdate = {transDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "wbcyc.995", OutputFolder, logCallback);
        }

        public static void ExportMdcyc(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload mdcyc .......");
            string sql = $"SELECT * FROM mdcyc WHERE cdate = {transDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "mdcyc.995", OutputFolder, logCallback);
        }

        public static void ExportTfcyc(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload tfcyc .......");
            string sql = $"SELECT * FROM tfcyc WHERE cdate = {transDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "tfcyc.995", OutputFolder, logCallback);
        }

        public static void ExportWipOnhand(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload wip_onhand .....");
            string sql = $"SELECT * FROM wip_onhand WHERE wonh_date = {transDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "wip_onh.995", OutputFolder, logCallback);
        }

        public static void ExportCuringH(IFXTransaction tx, DateTime date, Action<string> logCallback)
        {
            logCallback?.Invoke("unload curing_h .....");
            // 修改 SQL 查詢以使用 TO_DATE 函數，並將 C# DateTime 格式化為 YYYY/MM/DD
            string sql = $"SELECT * FROM curing_h WHERE start_date = TO_DATE('{date:yyyy/MM/dd}', '%Y/%m/%d')";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "curing_h.995", OutputFolder, logCallback);
        }

        public static void ExportCuringD(IFXTransaction tx, DateTime date, Action<string> logCallback)
        {
            logCallback?.Invoke("unload curing_d .....");
            string sql = $"SELECT * FROM curing_d WHERE start_date = TO_DATE('{date:yyyy/MM/dd}', '%Y/%m/%d')";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "curing_d.995", OutputFolder, logCallback);
        }

        public static void ExportLotAttrRec(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload lot_attr_rec .....");
            string sql = $"SELECT * FROM lot_attr_rec WHERE larc_trans_date = {transDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "lot_attr_rec.995", OutputFolder, logCallback);
        }

        public static void ExportLocWbHist(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload loc_wb_hist .....");
            string sql = $"SELECT * FROM loc_wb_hist WHERE trans_date = {transDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "loc_wb_hist.995", OutputFolder, logCallback);
        }

        public static void ExportEntAttrRecHist(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload ent_attr_rec_hist .....");
            string sql = $"SELECT * FROM ent_attr_rec_hist WHERE earh_trans_date = {transDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "ent_attr_rec_hist.995", OutputFolder, logCallback);
        }

        public static void ExportLfStHist(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload lf_st_hist .....");
            string sql = $"SELECT * FROM lf_st_hist WHERE trans_date = {transDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "lf_st_hist.995", OutputFolder, logCallback);
        }

        public static void ExportLfStExtra(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload lf_st_extra .....");
            string sql = $"SELECT * FROM lf_st_extra WHERE trans_date = {transDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "lf_st_extra.995", OutputFolder, logCallback);
        }

        public static void ExportEpoxyHist(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload epoxy_hist .....");
            string sql = $"SELECT * FROM epoxy_hist WHERE trans_date = {transDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "epoxy_hist.995", OutputFolder, logCallback);
        }

        public static void ExportMdCpHist(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload md_cp_hist .....");
            string sql = $"SELECT * FROM md_cp_hist WHERE trans_date = {transDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "md_cp_hist.995", OutputFolder, logCallback);
        }

        public static void ExportWaferIdRec(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload wafer_id_rec .....");
            string sql = $"SELECT * FROM wafer_id_rec WHERE trans_date = {transDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "wafer_id_rec.995", OutputFolder, logCallback);
        }

        public static void ExportSpecialRllt(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload special_rllt .....");
            string sql = $"SELECT * FROM special_rllt WHERE sprl_trans_date = {transDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "special_rllt.995", OutputFolder, logCallback);
        }

        public static void ExportSwrLot(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload swr_lot .....");
            string sql = $"SELECT * FROM swr_lot WHERE swr_trans_date = {transDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "swr_lot.995", OutputFolder, logCallback);
        }

        /// <summary>
        /// 匯出 efrm 相關表格
        /// </summary>
        public static void ExportEfrmTables(IFXTransaction tx, DateTime date, Action<string> logCallback)
        {
            logCallback?.Invoke("unload efrm .....");
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
                //string dynamicSql = $"SELECT * FROM {tableName} WHERE form_key_1 = '{date:yyyy-MM-dd}'";
                string dynamicSql = $"SELECT * FROM {tableName} WHERE form_key_1 = TO_DATE('{date:yyyy/MM/dd}', '%Y/%m/%d')";

                try
                {
                    var result = IfxDataAccess.ExecuteDataTable(tx, dynamicSql);
                    DataExporter.ExportData_Append(result, $"{tableName}.995", OutputFolder, logCallback);
                }
                catch (Exception ex)
                {
                    logCallback?.Invoke($"匯出 {tableName} 時發生錯誤: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// 匯出 ent_lot
        /// </summary>
        public static void ExportEntLot(IFXTransaction tx, int endDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload ent_lot .....");
            string sql = $"SELECT * FROM ent_lot WHERE end_date = {endDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "ent_lot.995", OutputFolder, logCallback);
        }

        /// <summary>
        /// 匯出 ent_lot1
        /// </summary>
        public static void ExportEntLot1(IFXTransaction tx, int endDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload ent_lot1 .....");
            string sql = $"SELECT * FROM ent_lot1 WHERE end_date = {endDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "ent_lot1.995", OutputFolder, logCallback);
        }

        /// <summary>
        /// 匯出 ent_use
        /// </summary>
        public static void ExportEntUse(IFXTransaction tx, int startDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload ent_use .....");
            string sql = $"SELECT * FROM ent_use WHERE start_date = {startDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "ent_use.995", OutputFolder, logCallback);
        }

        /// <summary>
        /// 匯出 sta_transa
        /// </summary>
        public static void ExportStaTransa(IFXTransaction tx, int transDate, Action<string> logCallback)
        {
            logCallback?.Invoke("unload sta_transa .....");
            string sql = $"SELECT * FROM sta_transa WHERE cost_trans_date = {transDate}";
            var result = IfxDataAccess.ExecuteDataTable(tx, sql);
            DataExporter.ExportData_Append(result, "sta_a.995", OutputFolder, logCallback);
        }
    }

}
