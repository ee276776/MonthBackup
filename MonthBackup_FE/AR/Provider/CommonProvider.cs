using CPISData.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MonthBackup_FE.AR.Provider
{
    public static class CommonProvider
    {
        /// <summary>
        /// 取得 bin_hist 資料
        /// </summary>
        public static DataTable GetBinHistData(IFXTransaction tx)
        {
            return FetchByCchuSubquery(tx, "bin_hist", "bhis_lot_number", "bin_hist.995");
        }

        /// <summary>
        /// 取得 bin_hllt 資料
        /// </summary>
        public static DataTable GetBinHlltData(IFXTransaction tx)
        {
            return FetchByCchuSubquery(tx, "bin_hllt", "bhlt_lot_number", "bin_hllt.995");
        }

        /// <summary>
        /// 取得 fail_dat 資料
        /// </summary>
        public static DataTable GetFailDatData(IFXTransaction tx)
        {
            return FetchByCchuSubquery(tx, "fail_dat", "fail_lot_number", "fail_dat.995");
        }
        public static DataTable GetAbnMvouHistData(IFXTransaction tx) => FetchByCchuSubquery(tx, "abn_mvou_hist", "abnm_lot_number","abn_mvou.995");
        public static DataTable GetAspdsData(IFXTransaction tx) => FetchByCchuSubquery(tx, "as_pds", "apds_assylot", "as_pds.995");

        public static DataTable GetFailHistData(IFXTransaction tx) => FetchByCchuSubquery(tx, "fail_hist", "fail_lot_number", "fail_his.995");
        public static DataTable GetFailHist2Data(IFXTransaction tx) => FetchByCchuSubquery(tx, "fail_hist2", "fail_lot_number", "fail_h2.995");
        public static DataTable GetHoldHistData(IFXTransaction tx) => FetchByCchuSubquery(tx, "hold_hist", "lot_number", "hold_his.995");
        public static DataTable GetInkHistData(IFXTransaction tx) => FetchByCchuSubquery(tx, "ink_hist", "ink_lot_number", "ink_hist.995");
        public static DataTable GetLblHistData(IFXTransaction tx) => FetchByCchuSubquery(tx, "lbl_hist", "lbl_lot_number", "lbl_hist.995");
        public static DataTable GetLossDatData(IFXTransaction tx) => FetchByCchuSubquery(tx, "loss_dat", "loss_lot_number", "loss_dat.995");
        public static DataTable GetLossHistData(IFXTransaction tx) => FetchByCchuSubquery(tx, "loss_hist", "loss_lot_number", "loss_his.995");
        public static DataTable GetMatHistData(IFXTransaction tx) => FetchByCchuSubquery(tx, "mat_hist", "mat_lot_number", "mat_hist.995");
        public static DataTable GetWipMvouEntityData(IFXTransaction tx) => FetchByCchuSubquery(tx, "wip_mvou_entity", "wmvo_lot_number", "mvou_ent.995");
        public static DataTable GetOperTurnData(IFXTransaction tx) => FetchByCchuSubquery(tx, "oper_turn", "turn_lot_number", "op_turn.995");
        public static DataTable GetPunchHistData(IFXTransaction tx) => FetchByCchuSubquery(tx, "punch_hist", "punch_lot_number", "punc_his.995");
        public static DataTable GetQcHistData(IFXTransaction tx) => FetchByCchuSubquery(tx, "qc_hist", "qc_lot_number", "qc_hist.995");
        public static DataTable GetRwltCpHistData(IFXTransaction tx) => FetchByCchuSubquery(tx, "rwlt_cp_hist", "rcp_lot_number", "rcp_hist.995");
        public static DataTable GetRedoDatData(IFXTransaction tx) => FetchByCchuSubquery(tx, "redo_dat", "redo_lot_number", "redo_dat.995");
        public static DataTable GetRedoHistData(IFXTransaction tx) => FetchByCchuSubquery(tx, "redo_hist", "redo_lot_number", "redo_his.995");
        public static DataTable GetRedoU2HistData(IFXTransaction tx) => FetchByCchuSubquery(tx, "redo_u2_hist", "redo_u2_lot_number", "redo_his2.995");
        public static DataTable GetRemarkHistData(IFXTransaction tx) => FetchByCchuSubquery(tx, "remark_hist", "rmk_lot_number", "rmk_hist.995");
        public static DataTable GetRunTimeData(IFXTransaction tx) => FetchByCchuSubquery(tx, "run_time", "run_lot_number", "run_time.995");
        public static DataTable GetSampHistData(IFXTransaction tx) => FetchByCchuSubquery(tx, "samp_hist", "samp_lot_number", "samp_his.995");
        public static DataTable GetShipHistData(IFXTransaction tx) => FetchByCchuSubquery(tx, "ship_hist", "assy_lot_no", "ship_his.995");
        public static DataTable GetWaflotHistData(IFXTransaction tx) => FetchByCchuSubquery(tx, "waflot_hist", "wflt_waflot_no", "whis_waf.995");
        public static DataTable GetWipDelData(IFXTransaction tx) => FetchByCchuSubquery(tx, "wip_del", "wdel_lot_number", "wip_del.995");
        public static DataTable GetWipLbrData(IFXTransaction tx) => FetchByCchuSubquery(tx, "wip_lbr", "wlbr_lot_number", "wip_lbr.995");
        public static DataTable GetWipAlrData(IFXTransaction tx) => FetchByCchuSubquery(tx, "wipalr", "walr_lot_number", "wipalr.995");
        public static DataTable GetWipdltData(IFXTransaction tx) => FetchByCchuSubquery(tx, "wipdlt", "wdlt_lot_number", "wipdlt.995");
        public static DataTable GetWiplhaData(IFXTransaction tx) => FetchByCchuSubquery(tx, "wiplha", "wlha_lot_number", "wiplha.995");
        public static DataTable GetWiplotData(IFXTransaction tx) => FetchByCchuSubquery(tx, "wiplot", "wlot_lot_number", "wiplot.995");
        public static DataTable GetWiplshData(IFXTransaction tx) => FetchByCchuSubquery(tx, "wiplsh", "wlsh_lot_number", "wiplsh.995");
        public static DataTable GetWiplspData(IFXTransaction tx) => FetchByCchuSubquery(tx, "wiplsp", "wlsp_lot_number", "wiplsp.995");
        public static DataTable GetWipltaData(IFXTransaction tx) => FetchByCchuSubquery(tx, "wiplta", "wlta_lot_number", "wiplta.995");
        public static DataTable GetIlbWfChkData(IFXTransaction tx) => FetchByCchuSubquery(tx, "ilb_wf_chk", "ilwc_taplot_no", "ilb_wchk.995");
        public static DataTable GetQcSelLotData(IFXTransaction tx) => FetchByCchuSubquery(tx, "qc_sel_lot", "lot_number", "qc_selot.995");
        public static DataTable GetWfVirSpltData(IFXTransaction tx) => FetchByCchuSubquery(tx, "wf_vir_splt", "wvsp_taplot_no", "wf_vsplt.995");
        public static DataTable GetBillRptData(IFXTransaction tx) => FetchByCchuSubquery(tx, "bill_rpt", "wsm_lot", "bill_rpt.995");
        public static DataTable GetMklblHistData(IFXTransaction tx) => FetchByCchuSubquery(tx, "mklbl_hist", "mklbl_assy_lot", "mklbl_hist.995");
        public static DataTable GetWipEntData(IFXTransaction tx) => FetchByCchuSubquery(tx, "wip_ent", "went_lot_number", "wip_ent.995");
        public static DataTable GetBillSumData(IFXTransaction tx) => FetchByCchuSubquery(tx, "bill_sum", "wsm_lot", "bill_sum.995");
        public static DataTable GetDocHistData(IFXTransaction tx) => FetchByCchuSubquery(tx, "doc_hist", "dhst_lot_number", "doc_hist.995");
        public static DataTable GetLotMatRecData(IFXTransaction tx) => FetchByCchuSubquery(tx, "lot_mat_rec", "lmrc_lot_number", "lot_mat_rec.995");
        public static DataTable GetDyFvSpltData(IFXTransaction tx) => FetchByCchuSubquery(tx, "dy_fv_splt", "ori_assy_lot", "dy_fv_splt.995");
        public static DataTable GetPre3ipCpltData(IFXTransaction tx) => FetchByCchuSubquery(tx, "pre_3ip_cplt", "wlot_lot_number", "pre_3ip_cplt.995");
        public static DataTable GetCassetteHistData(IFXTransaction tx) => FetchByCchuSubquery(tx, "cassette_hist", "wlot_lot_number", "cassette_hist.995");

        /// <summary>
        /// 通用查詢封裝方法 (對應 4GL 的 unload 邏輯)
        /// </summary>
        private static DataTable FetchByCchuSubquery(IFXTransaction tx, string tableName, string lotColumnName, string fileName)
        {
            try
            {
                Console.WriteLine($"unload {tableName} ..... (Target: {fileName})");

                // 使用子查詢優化，避免 4GL 的 foreach 迴圈導致多次資料庫往返
                //string query = $@"
                //SELECT *
                //FROM {tableName}
                //WHERE {lotColumnName} IN (
                //    SELECT wlot_lot_number
                //    FROM cchu
                //)";
                string query = $@"
                SELECT T.*
                FROM {tableName} AS T
                INNER JOIN cchu AS C ON T.{lotColumnName} = C.wlot_lot_number";

                DataTable dt = IfxDataAccess.ExecuteDataTable(tx, query);

                Console.WriteLine($"{tableName} 查詢完成，回傳筆數: {dt?.Rows.Count ?? 0}");
                return dt;
            }
            catch (Exception ex)
            {
                // 對應 4GL 的 if sqlca.sqlcode <> 0 
                Console.WriteLine($"== error == 查詢 {tableName} 時發生錯誤: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// 匯出 ar_exclude 資料
        /// 注意：這裡的邏輯直接 unload 整個表
        /// </summary>
        public static DataTable GetArExcludeData(IFXTransaction tx)
        {
            try
            {
                Console.WriteLine("unload ar_exclude .......");

                string query = @"
                SELECT *
                FROM ar_exclude";

                DataTable dt = IfxDataAccess.ExecuteDataTable(tx, query);

                Console.WriteLine($"ar_exclude 查詢完成，回傳筆數: {dt?.Rows.Count ?? 0}");
                return dt;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"== error == 查詢 ar_exclude 時發生錯誤: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// 匯出 cchu 資料
        /// 注意：這裡的邏輯直接 unload 整個表
        /// </summary>
        public static DataTable GetCchuData(IFXTransaction tx)
        {
            try
            {
                Console.WriteLine("unload cchu .......");

                string query = @"
                SELECT *
                FROM cchu";

                DataTable dt = IfxDataAccess.ExecuteDataTable(tx, query);

                Console.WriteLine($"cchu 查詢完成，回傳筆數: {dt?.Rows.Count ?? 0}");
                return dt;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"== error == 查詢 cchu 時發生錯誤: {ex.Message}");
                return null;
            }
        }
    }
}
