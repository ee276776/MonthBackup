using MonthBackup_FE.AR_DEL.Provider;
using MonthBackup_FE.EAR_DEL.Provider;
using MonthBackup_FE.Helper;
using MonthBackup_FE.Service;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MonthBackup_FE.AR_DEL.Service
{
    public class ArDelExportService
    {
        public static void Execute(Dictionary<string, string> parameters, Action<string> logCallback)
        {
            try
            {
                logCallback("========================================");
                logCallback("         AR 資料刪除程式");
                logCallback("========================================\n");
                logCallback("開始刪除資料...\n");
                var startTime = DateTime.Now;
                logCallback("開始時間：" + startTime.ToString("yyyy/MM/dd HH:mm:ss"));
                LogHelper.WriteLog("AR_DEL", "開始時間：" + startTime.ToString("yyyy/MM/dd HH:mm:ss"));


                using (var service = new UserDefineTableService())
                using (var tx = service.BeginTransaction())
                {
                    logCallback("--- 階段 1/49: 執行 del_load 邏輯 ---");
                    DatabaseProvider.ExecuteDelLoadLogic(tx, parameters["endDate"], logCallback);
                    logCallback("[V] del_load 完成\n");

                    logCallback("--- 階段 2/49: 刪除 as_pds ---");
                    CommonProvider.DeleteAsPds(tx, logCallback);
                    logCallback("[V] as_pds 刪除完成\n");

                    logCallback("--- 階段 3/49: 刪除 wiplth ---");
                    CommonProvider.DeleteWiplth(tx, logCallback);
                    logCallback("[V] wiplth 刪除完成\n");

                    logCallback("--- 階段 4/49: 刪除 wiplsh ---");
                    CommonProvider.DeleteWiplsh(tx, logCallback);
                    logCallback("[V] wiplsh 刪除完成\n");

                    logCallback("--- 階段 5/49: 刪除 wiplha ---");
                    CommonProvider.DeleteWiplha(tx, logCallback);
                    logCallback("[V] wiplha 刪除完成\n");

                    logCallback("--- 階段 6/49: 刪除 wiplsp ---");
                    CommonProvider.DeleteWiplsp(tx, logCallback);
                    logCallback("[V] wiplsp 刪除完成\n");

                    logCallback("--- 階段 7/49: 刪除 wiplta ---");
                    CommonProvider.DeleteWiplta(tx, logCallback);
                    logCallback("[V] wiplta 刪除完成\n");

                    logCallback("--- 階段 8/49: 刪除 wipdlt ---");
                    CommonProvider.DeleteWipdlt(tx, logCallback);
                    logCallback("[V] wipdlt 刪除完成\n");

                    logCallback("--- 階段 9/49: 刪除 wip_lbr ---");
                    CommonProvider.DeleteWipLbr(tx, logCallback);
                    logCallback("[V] wip_lbr 刪除完成\n");

                    logCallback("--- 階段 10/49: 刪除 oper_turn ---");
                    CommonProvider.DeleteOperTurn(tx, logCallback);
                    logCallback("[V] oper_turn 刪除完成\n");

                    logCallback("--- 階段 11/49: 刪除 qc_hist ---");
                    CommonProvider.DeleteQcHist(tx, logCallback);
                    logCallback("[V] qc_hist 刪除完成\n");

                    logCallback("--- 階段 12/49: 刪除 redo_hist ---");
                    CommonProvider.DeleteRedoHist(tx, logCallback);
                    logCallback("[V] redo_hist 刪除完成\n");

                    logCallback("--- 階段 13/49: 刪除 redo_dat ---");
                    CommonProvider.DeleteRedoDat(tx, logCallback);
                    logCallback("[V] redo_dat 刪除完成\n");

                    logCallback("--- 階段 14/49: 刪除 fail_hist ---");
                    CommonProvider.DeleteFailHist(tx, logCallback);
                    logCallback("[V] fail_hist 刪除完成\n");

                    logCallback("--- 階段 15/49: 刪除 samp_hist ---");
                    CommonProvider.DeleteSampHist(tx, logCallback);
                    logCallback("[V] samp_hist 刪除完成\n");

                    logCallback("--- 階段 16/49: 刪除 mat_hist ---");
                    CommonProvider.DeleteMatHist(tx, logCallback);
                    logCallback("[V] mat_hist 刪除完成\n");

                    logCallback("--- 階段 17/49: 刪除 wip_del ---");
                    CommonProvider.DeleteWipDel(tx, logCallback);
                    logCallback("[V] wip_del 刪除完成\n");

                    logCallback("--- 階段 18/49: 刪除 ship_hist ---");
                    CommonProvider.DeleteShipHist(tx, logCallback);
                    logCallback("[V] ship_hist 刪除完成\n");

                    logCallback("--- 階段 19/49: 刪除 wip_mvou_entity ---");
                    CommonProvider.DeleteWipMvouEntity(tx, logCallback);
                    logCallback("[V] wip_mvou_entity 刪除完成\n");

                    logCallback("--- 階段 20/49: 刪除 fail_dat ---");
                    CommonProvider.DeleteFailDat(tx, logCallback);
                    logCallback("[V] fail_dat 刪除完成\n");

                    logCallback("--- 階段 21/49: 刪除 lbl_hist ---");
                    CommonProvider.DeleteLblHist(tx, logCallback);
                    logCallback("[V] lbl_hist 刪除完成\n");

                    logCallback("--- 階段 22/49: 刪除 loss_hist ---");
                    CommonProvider.DeleteLossHist(tx, logCallback);
                    logCallback("[V] loss_hist 刪除完成\n");

                    logCallback("--- 階段 23/49: 刪除 loss_dat ---");
                    CommonProvider.DeleteLossDat(tx, logCallback);
                    logCallback("[V] loss_dat 刪除完成\n");

                    logCallback("--- 階段 24/49: 刪除 abn_mvou_hist ---");
                    CommonProvider.DeleteAbnMvouHist(tx, logCallback);
                    logCallback("[V] abn_mvou_hist 刪除完成\n");

                    logCallback("--- 階段 25/49: 刪除 wip_ent ---");
                    CommonProvider.DeleteWipEnt(tx, logCallback);
                    logCallback("[V] wip_ent 刪除完成\n");

                    logCallback("--- 階段 26/49: 刪除 hold_hist ---");
                    CommonProvider.DeleteHoldHist(tx, logCallback);
                    logCallback("[V] hold_hist 刪除完成\n");

                    logCallback("--- 階段 27/49: 刪除 bin_hist ---");
                    CommonProvider.DeleteBinHist(tx, logCallback);
                    logCallback("[V] bin_hist 刪除完成\n");

                    logCallback("--- 階段 28/49: 刪除 bin_hllt ---");
                    CommonProvider.DeleteBinHllt(tx, logCallback);
                    logCallback("[V] bin_hllt 刪除完成\n");

                    logCallback("--- 階段 29/49: 刪除 waflot_hist ---");
                    CommonProvider.DeleteWaflotHist(tx, logCallback);
                    logCallback("[V] waflot_hist 刪除完成\n");

                    logCallback("--- 階段 30/49: 刪除 waflot_bkup ---");
                    CommonProvider.DeleteWaflotBkup(tx, logCallback);
                    logCallback("[V] waflot_bkup 刪除完成\n");

                    logCallback("--- 階段 31/49: 刪除 punch_hist ---");
                    CommonProvider.DeletePunchHist(tx, logCallback);
                    logCallback("[V] punch_hist 刪除完成\n");

                    logCallback("--- 階段 32/49: 刪除 remark_hist ---");
                    CommonProvider.DeleteRemarkHist(tx, logCallback);
                    logCallback("[V] remark_hist 刪除完成\n");

                    logCallback("--- 階段 33/49: 刪除 ink_hist ---");
                    CommonProvider.DeleteInkHist(tx, logCallback);
                    logCallback("[V] ink_hist 刪除完成\n");

                    logCallback("--- 階段 34/49: 刪除 redo_u2_hist ---");
                    CommonProvider.DeleteRedoU2Hist(tx, logCallback);
                    logCallback("[V] redo_u2_hist 刪除完成\n");

                    logCallback("--- 階段 35/49: 刪除 rwlt_cp_hist ---");
                    CommonProvider.DeleteRwltCpHist(tx, logCallback);
                    logCallback("[V] rwlt_cp_hist 刪除完成\n");

                    logCallback("--- 階段 36/49: 刪除 fail_hist2 ---");
                    CommonProvider.DeleteFailHist2(tx, logCallback);
                    logCallback("[V] fail_hist2 刪除完成\n");

                    logCallback("--- 階段 37/49: 刪除 ilb_wf_chk ---");
                    CommonProvider.DeleteIlbWfChk(tx, logCallback);
                    logCallback("[V] ilb_wf_chk 刪除完成\n");

                    logCallback("--- 階段 38/49: 刪除 qc_sel_lot ---");
                    CommonProvider.DeleteQcSelLot(tx, logCallback);
                    logCallback("[V] qc_sel_lot 刪除完成\n");

                    logCallback("--- 階段 39/49: 刪除 wf_vir_splt ---");
                    CommonProvider.DeleteWfVirSplt(tx, logCallback);
                    logCallback("[V] wf_vir_splt 刪除完成\n");

                    logCallback("--- 階段 40/49: 刪除 wipalr ---");
                    CommonProvider.DeleteWipalr(tx, logCallback);
                    logCallback("[V] wipalr 刪除完成\n");

                    logCallback("--- 階段 41/49: 刪除 bill_rpt ---");
                    CommonProvider.DeleteBillRpt(tx, logCallback);
                    logCallback("[V] bill_rpt 刪除完成\n");

                    logCallback("--- 階段 42/49: 刪除 mklbl_hist ---");
                    CommonProvider.DeleteMklblHist(tx, logCallback);
                    logCallback("[V] mklbl_hist 刪除完成\n");

                    logCallback("--- 階段 43/49: 刪除 doc_hist ---");
                    CommonProvider.DeleteDocHist(tx, logCallback);
                    logCallback("[V] doc_hist 刪除完成\n");

                    logCallback("--- 階段 44/49: 刪除 lot_mat_rec ---");
                    CommonProvider.DeleteLotMatRec(tx, logCallback);
                    logCallback("[V] lot_mat_rec 刪除完成\n");

                    logCallback("--- 階段 45/49: 刪除 pre_3ip_cplt ---");
                    CommonProvider.DeletePre3ipCplt(tx, logCallback);
                    logCallback("[V] pre_3ip_cplt 刪除完成\n");

                    logCallback("--- 階段 46/49: 刪除 cassette_hist ---");
                    CommonProvider.DeleteCassetteHist(tx, logCallback);
                    logCallback("[V] cassette_hist 刪除完成\n");

                    logCallback("--- 階段 47/49: 刪除 run_time ---");
                    CommonProvider.DeleteRunTime(tx, logCallback);
                    logCallback("[V] run_time 刪除完成\n");

                    logCallback("--- 階段 48/49: 刪除 dy_fv_splt ---");
                    DyFvSpltProvider.DeleteDyFvSplt(tx, logCallback);
                    logCallback("[V] dy_fv_splt 刪除完成\n");

                    logCallback("--- 階段 48/49: 刪除 wiplot ---");
                    CommonProvider.DeleteWiplot(tx, logCallback);
                    logCallback("[V] wiplot 刪除完成\n");

                    CommonProvider.FetchCchuData(tx, logCallback);

                    logCallback("--- 階段 49/49: 提交交易 (Commit) ---");
                    tx.Commit();
                    logCallback("[V] 交易已成功提交\n");
                    var endTime = DateTime.Now;
                    TimeSpan duration = endTime - startTime;
                    logCallback("結束時間：" + endTime.ToString("yyyy/MM/dd HH:mm:ss"));
                    logCallback($"歷經時間：{(int)duration.TotalHours:00} 小時 {duration.Minutes:00} 分 {duration.Seconds:00} 秒");
                    LogHelper.WriteLog("AR_DEL", "結束時間：" + endTime.ToString("yyyy/MM/dd HH:mm:ss"));
                    LogHelper.WriteLog("AR_DEL", $"歷經時間：{(int)duration.TotalHours:00} 小時 {duration.Minutes:00} 分 {duration.Seconds:00} 秒");
                }

                logCallback("\n========================================");
                logCallback($"[V] 資料刪除完成！");
                logCallback($"[V] cchu 插入筆數: {DatabaseProvider.cchuCount}");
                logCallback("========================================");
                LogHelper.WriteLog("AR_DEL", GlobalSettings.IsDeleteMode ? "[V] AR 資料刪除完成！" : "[V] AR 資料匯出完成！");
                LogHelper.ExecuteRecord("ar_del.4ge", $"{DateTime.Now.ToString("f", new CultureInfo("zh-TW"))}", $"endDate: {parameters["endDate"]}");
            }
            catch (Exception ex)
            {
                logCallback("\n========================================");
                logCallback($"[X] 程式執行出現錯誤: {ex.Message}");
                logCallback($"[X] 堆疊追蹤:\n{ex.StackTrace}");
                logCallback("========================================");
                Environment.ExitCode = 1;
                LogHelper.WriteLog("AR_DEL", $"[X] 程式執行出現錯誤: {ex.Message}");


            }
            finally
            {
    
            }
        }
    }
}
