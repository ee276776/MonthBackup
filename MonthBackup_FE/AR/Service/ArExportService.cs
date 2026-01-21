using MonthBackup_FE.AR.Provider;
using MonthBackup_FE.EAR_DEL.Provider;
using MonthBackup_FE.Helper;
using MonthBackup_FE.Service;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;

namespace MonthBackup_FE.AR.Service
{
    /// <summary>
    /// AR 資料匯出服務 - 對應原 ar.4ge 的執行邏輯
    /// </summary>
    public static class ArExportService
    {
        public static void Execute(Dictionary<string, string> parameters, Action<string> logCallback)
        {
            try
            {
                logCallback("========================================");
                logCallback("         AR 資料匯出程式");
                logCallback("========================================\n");

                var startTime = DateTime.Now;
                logCallback("開始時間：" + startTime.ToString("yyyy/MM/dd HH:mm:ss"));
                LogHelper.WriteLog("AR", "開始時間：" + startTime.ToString("yyyy/MM/dd HH:mm:ss"));
                // 取得參數
                string endDate = parameters.ContainsKey("endDate") ? parameters["endDate"] : "";
                logCallback($"參數 endDate: {endDate}\n");

                logCallback("開始匯出資料...\n");
                string FolderNameCurMonth = $"AR_{DateTime.Now.ToString("yyyyMM")}";
                // 這裡需要根據你的實際資料庫連線方式來調整
                // 假設你有 UserDefineTableService 類別
                using (var service = new UserDefineTableService())
                using (var tx = service.BeginTransaction())
                {
                    logCallback("--- 階段 1/54: 執行 del_load 邏輯 ---");
                    DatabaseProvider2.ExecuteDelLoadLogic(tx, endDate, logCallback);
                    logCallback("[V] del_load 完成\n");

                    logCallback("--- 階段 2/54: 匯出 as_pds 資料 ---");
                    DataTable aspds = CommonProvider.GetAspdsData(tx);
                    DataExporter.ExportData(aspds, "as_pds.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] as_pds 匯出完成\n");

                    logCallback("--- 階段 3/54: 匯出 wiplth 資料 ---");
                    WiplthProvider.ExportWiplthData(tx);
                    logCallback("[V] wiplth 匯出完成\n");

                    logCallback("--- 階段 4/54: 匯出 abn_mvou 資料 ---");
                    DataTable abns = CommonProvider.GetAbnMvouHistData(tx);
                    DataExporter.ExportData(abns, "abn_mvou.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] abn_mvou 匯出完成\n");

                    logCallback("--- 階段 5/54: 匯出 bin_hist 資料 ---");
                    DataTable binHist = CommonProvider.GetBinHistData(tx);
                    DataExporter.ExportData(binHist, "bin_hist.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] bin_hist 匯出完成\n");

                    logCallback("--- 階段 6/54: 匯出 bin_hllt 資料 ---");
                    DataTable binHllt = CommonProvider.GetBinHlltData(tx);
                    DataExporter.ExportData(binHllt, "bin_hllt.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] bin_hllt 匯出完成\n");

                    logCallback("--- 階段 7/54: 匯出 fail_dat 資料 ---");
                    DataTable failDat = CommonProvider.GetFailDatData(tx);
                    DataExporter.ExportData(failDat, "fail_dat.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] fail_dat 匯出完成\n");

                    logCallback("--- 階段 8/54: 匯出 fail_hist2 資料 ---");
                    DataTable failHist2 = CommonProvider.GetFailHist2Data(tx);
                    DataExporter.ExportData(failHist2, "fail_h2.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] fail_hist2 匯出完成\n");

                    logCallback("--- 階段 9/54: 匯出 fail_hist 資料 ---");
                    DataTable failHist = CommonProvider.GetFailHistData(tx);
                    DataExporter.ExportData(failHist, "fail_his.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] fail_hist 匯出完成\n");

                    logCallback("--- 階段 10/54: 匯出 hold_hist 資料 ---");
                    DataTable holdHist = CommonProvider.GetHoldHistData(tx);
                    DataExporter.ExportData(holdHist, "hold_his.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] hold_hist 匯出完成\n");

                    logCallback("--- 階段 11/54: 匯出 ink_hist 資料 ---");
                    DataTable inkHist = CommonProvider.GetInkHistData(tx);
                    DataExporter.ExportData(inkHist, "ink_hist.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] ink_hist 匯出完成\n");

                    logCallback("--- 階段 12/54: 匯出 lbl_hist 資料 ---");
                    DataTable lblHist = CommonProvider.GetLblHistData(tx);
                    DataExporter.ExportData(lblHist, "lbl_hist.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] lbl_hist 匯出完成\n");

                    logCallback("--- 階段 13/54: 匯出 loss_dat 資料 ---");
                    DataTable lossDat = CommonProvider.GetLossDatData(tx);
                    DataExporter.ExportData(lossDat, "loss_dat.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] loss_dat 匯出完成\n");

                    logCallback("--- 階段 14/54: 匯出 loss_hist 資料 ---");
                    DataTable lossHist = CommonProvider.GetLossHistData(tx);
                    DataExporter.ExportData(lossHist, "loss_his.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] loss_hist 匯出完成\n");

                    logCallback("--- 階段 15/54: 匯出 mat_hist 資料 ---");
                    DataTable matHist = CommonProvider.GetMatHistData(tx);
                    DataExporter.ExportData(matHist, "mat_hist.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] mat_hist 匯出完成\n");

                    logCallback("--- 階段 16/54: 匯出 redo_hist 資料 ---");
                    DataTable redoHist = CommonProvider.GetRedoHistData(tx);
                    DataExporter.ExportData(redoHist, "redo_his.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] redo_hist 匯出完成\n");

                    logCallback("--- 階段 17/54: 匯出 redo_u2_hist 資料 ---");
                    DataTable redoU2Hist = CommonProvider.GetRedoU2HistData(tx);
                    DataExporter.ExportData(redoU2Hist, "redo_his2.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] redo_u2_hist 匯出完成\n");

                    logCallback("--- 階段 18/54: 匯出 remark_hist 資料 ---");
                    DataTable remarkHist = CommonProvider.GetRemarkHistData(tx);
                    DataExporter.ExportData(remarkHist, "rmk_hist.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] remark_hist 匯出完成\n");

                    logCallback("--- 階段 19/54: 匯出 run_time 資料 ---");
                    DataTable runTime = CommonProvider.GetRunTimeData(tx);
                    DataExporter.ExportData(runTime, "run_time.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] run_time 匯出完成\n");

                    logCallback("--- 階段 20/54: 匯出 samp_hist 資料 ---");
                    DataTable sampHist = CommonProvider.GetSampHistData(tx);
                    DataExporter.ExportData(sampHist, "samp_his.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] samp_hist 匯出完成\n");

                    logCallback("--- 階段 21/54: 匯出 ship_hist 資料 ---");
                    DataTable shipHist = CommonProvider.GetShipHistData(tx);
                    DataExporter.ExportData(shipHist, "ship_his.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] ship_hist 匯出完成\n");

                    logCallback("--- 階段 22/54: 匯出 waflot_hist 資料 ---");
                    DataTable waflotHist = CommonProvider.GetWaflotHistData(tx);
                    DataExporter.ExportData(waflotHist, "whis_waf.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] waflot_hist 匯出完成\n");

                    logCallback("--- 階段 23/54: 匯出 ilb_wf_chk 資料 ---");
                    DataTable ilbWfChk = CommonProvider.GetIlbWfChkData(tx);
                    DataExporter.ExportData(ilbWfChk, "ilb_wchk.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] ilb_wf_chk 匯出完成\n");

                    logCallback("--- 階段 24/54: 匯出 qc_sel_lot 資料 ---");
                    DataTable qcSelLot = CommonProvider.GetQcSelLotData(tx);
                    DataExporter.ExportData(qcSelLot, "qc_selot.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] qc_sel_lot 匯出完成\n");

                    logCallback("--- 階段 25/54: 匯出 wf_vir_splt 資料 ---");
                    DataTable wfVirSplt = CommonProvider.GetWfVirSpltData(tx);
                    DataExporter.ExportData(wfVirSplt, "wf_vsplt.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] wf_vir_splt 匯出完成\n");

                    logCallback("--- 階段 26/54: 匯出 bill_rpt 資料 ---");
                    DataTable billRpt = CommonProvider.GetBillRptData(tx);
                    DataExporter.ExportData(billRpt, "bill_rpt.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] bill_rpt 匯出完成\n");

                    logCallback("--- 階段 27/54: 匯出 mklbl_hist 資料 ---");
                    DataTable mklblHist = CommonProvider.GetMklblHistData(tx);
                    DataExporter.ExportData(mklblHist, "mklbl_hist.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] mklbl_hist 匯出完成\n");

                    logCallback("--- 階段 28/54: 匯出 bill_sum 資料 ---");
                    DataTable billSum = CommonProvider.GetBillSumData(tx);
                    DataExporter.ExportData(billSum, "bill_sum.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] bill_sum 匯出完成\n");

                    logCallback("--- 階段 29/54: 匯出 doc_hist 資料 ---");
                    DataTable docHist = CommonProvider.GetDocHistData(tx);
                    DataExporter.ExportData(docHist, "doc_hist.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] doc_hist 匯出完成\n");

                    logCallback("--- 階段 30/54: 匯出 lot_mat_rec 資料 ---");
                    DataTable lotMatRec = CommonProvider.GetLotMatRecData(tx);
                    DataExporter.ExportData(lotMatRec, "lot_mat_rec.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] lot_mat_rec 匯出完成\n");

                    logCallback("--- 階段 31/54: 匯出 dy_fv_splt 資料 ---");
                    DataTable dyFvSplt = DyFvSpltProvider.GetDyFvSpltDataBySubquery(tx);
                    DataExporter.ExportData(dyFvSplt, "dy_fv_splt.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] dy_fv_splt 匯出完成\n");

                    logCallback("--- 階段 32/54: 匯出 pre_3ip_cplt 資料 ---");
                    DataTable pre3ipCplt = CommonProvider.GetPre3ipCpltData(tx);
                    DataExporter.ExportData(pre3ipCplt, "pre_3ip_cplt.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] pre_3ip_cplt 匯出完成\n");

                    logCallback("--- 階段 33/54: 匯出 cassette_hist 資料 ---");
                    DataTable cassetteHist = CommonProvider.GetCassetteHistData(tx);
                    DataExporter.ExportData(cassetteHist, "cassette_hist.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] cassette_hist 匯出完成\n");

                    logCallback("--- 階段 34/54: 匯出 ar_exclude 資料 ---");
                    DataTable arExclude = CommonProvider.GetArExcludeData(tx);
                    DataExporter.ExportData(arExclude, "ar_exclude.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] ar_exclude 匯出完成\n");

                    logCallback("--- 階段 35/54: 匯出 cchu 資料 ---");
                    DataTable cchu = CommonProvider.GetCchuData(tx);
                    DataExporter.ExportData(cchu, "cchu.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] cchu 匯出完成\n");

                    logCallback("--- 階段 36/54: 匯出 rwlt_cp_hist 資料 ---");
                    DataTable rwltCpHist = CommonProvider.GetRwltCpHistData(tx);
                    DataExporter.ExportData(rwltCpHist, "rcp_hist.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] rwlt_cp_hist 匯出完成\n");

                    logCallback("--- 階段 37/54: 匯出 oper_turn 資料 ---");
                    DataTable operTurn = CommonProvider.GetOperTurnData(tx);
                    DataExporter.ExportData(operTurn, "op_turn.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] oper_turn 匯出完成\n");

                    logCallback("--- 階段 38/54: 匯出 punch_hist 資料 ---");
                    DataTable punchHist = CommonProvider.GetPunchHistData(tx);
                    DataExporter.ExportData(punchHist, "punc_his.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] punch_hist 匯出完成\n");

                    logCallback("--- 階段 39/54: 匯出 wip_lbr 資料 ---");
                    DataTable wipLbr = CommonProvider.GetWipLbrData(tx);
                    DataExporter.ExportData(wipLbr, "wip_lbr.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] wip_lbr 匯出完成\n");

                    logCallback("--- 階段 40/54: 匯出 wipdlt 資料 ---");
                    DataTable wipdlt = CommonProvider.GetWipdltData(tx);
                    DataExporter.ExportData(wipdlt, "wipdlt.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] wipdlt 匯出完成\n");

                    logCallback("--- 階段 41/54: 匯出 wiplsh 資料 ---");
                    DataTable wiplsh = CommonProvider.GetWiplshData(tx);
                    DataExporter.ExportData(wiplsh, "wiplsh.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] wiplsh 匯出完成\n");

                    logCallback("--- 階段 42/54: 匯出 wiplsp 資料 ---");
                    DataTable wiplsp = CommonProvider.GetWiplspData(tx);
                    DataExporter.ExportData(wiplsp, "wiplsp.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] wiplsp 匯出完成\n");

                    logCallback("--- 階段 43/54: 匯出 wiplta 資料 ---");
                    DataTable wiplta = CommonProvider.GetWipltaData(tx);
                    DataExporter.ExportData(wiplta, "wiplta.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] wiplta 匯出完成\n");

                    logCallback("--- 階段 44/54: 匯出 wiplha 資料 ---");
                    DataTable wiplha = CommonProvider.GetWiplhaData(tx);
                    DataExporter.ExportData(wiplha, "wiplha.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] wiplha 匯出完成\n");

                    logCallback("--- 階段 45/54: 匯出 wiplot 資料 ---");
                    DataTable wiplot = CommonProvider.GetWiplotData(tx);
                    DataExporter.ExportData(wiplot, "wiplot.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] wiplot 匯出完成\n");

                    logCallback("--- 階段 46/54: 匯出 wipalr 資料 ---");
                    DataTable wipalr = CommonProvider.GetWipAlrData(tx);
                    DataExporter.ExportData(wipalr, "wipalr.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] wipalr 匯出完成\n");

                    logCallback("--- 階段 47/54: 匯出 wip_del 資料 ---");
                    DataTable wipDel = CommonProvider.GetWipDelData(tx);
                    DataExporter.ExportData(wipDel, "wip_del.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] wip_del 匯出完成\n");

                    logCallback("--- 階段 48/54: 匯出 redo_dat 資料 ---");
                    DataTable redoDat = CommonProvider.GetRedoDatData(tx);
                    DataExporter.ExportData(redoDat, "redo_dat.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] redo_dat 匯出完成\n");

                    logCallback("--- 階段 49/54: 匯出 qc_hist 資料 ---");
                    DataTable qcHist = CommonProvider.GetQcHistData(tx);
                    DataExporter.ExportData(qcHist, "qc_hist.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] qc_hist 匯出完成\n");

                    logCallback("--- 階段 50/54: 匯出 wip_mvou_entity 資料 ---");
                    DataTable wipMvouEntity = CommonProvider.GetWipMvouEntityData(tx);
                    DataExporter.ExportData(wipMvouEntity, "mvou_ent.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] wip_mvou_entity 匯出完成\n");

                    logCallback("--- 階段 51/54: 匯出 wip_mvou_entity 資料 ---");
                    DataTable wipent = CommonProvider.GetWipEntData(tx);
                    DataExporter.ExportData(wipent, "wip_ent.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] wip_ent 匯出完成\n");

                    logCallback("--- 階段 52/54: 匯出 _cchu 資料 ---");
                    DataTable _cchu = CommonProvider.GetCchuData(tx);
                    DataExporter.ExportData(_cchu, "cchu.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] Cchu 匯出完成\n");

                    logCallback("--- 階段 53/54: 匯出 ar_exclude 資料 ---");
                    DataTable ar_exclude = CommonProvider.GetArExcludeData(tx);
                    DataExporter.ExportData(ar_exclude, "ar_exclude.995", FolderNameCurMonth, logCallback);
                    logCallback("[V] ar_exclude 匯出完成\n");

                    logCallback("--- 階段 54/54: 提交交易 (Commit) ---");
                    tx.Commit();
                    tx.Dispose();
                    logCallback("[V] 交易已成功提交\n");
                }

                var endTime = DateTime.Now;
                TimeSpan duration = endTime - startTime;
                logCallback("結束時間：" + endTime.ToString("yyyy/MM/dd HH:mm:ss"));
                logCallback($"歷經時間：{(int)duration.TotalHours:00} 小時 {duration.Minutes:00} 分 {duration.Seconds:00} 秒");
                LogHelper.WriteLog("AR", "結束時間：" + endTime.ToString("yyyy/MM/dd HH:mm:ss"));
                LogHelper.WriteLog("AR", $"歷經時間：{(int)duration.TotalHours:00} 小時 {duration.Minutes:00} 分 {duration.Seconds:00} 秒");
                logCallback("\n========================================");
                logCallback("[V] 資料匯出完成！");
                // logCallback($"[V] cchu 插入筆數: {DatabaseProvider2.cchuCount}");
                logCallback("========================================");
                LogHelper.WriteLog("AR", "[V] AR資料匯出完成！");
                LogHelper.ExecuteRecord("ar.4ge", $"{DateTime.Now.ToString("f", new CultureInfo("zh-TW"))}",$"endDate: {endDate}");
            }
            catch (Exception ex)
            {
                logCallback("\n========================================");
                logCallback($"[X] 程式執行出現錯誤: {ex.Message}");
                logCallback($"[X] 堆疊追蹤:\n{ex.StackTrace}");
                logCallback("========================================");
                LogHelper.WriteLog("AR_DEL", $"[X] 程式執行出現錯誤: {ex.Message}");

                throw;
            }
        }
    }
}