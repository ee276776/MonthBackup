using System;
using System.Data;
using CPISData.Data;
using MonthBackup_FE.Helper;

namespace MonthBackup_FE.PRODASSYS_SPC_AR.Service
{
    /// <summary>
    /// 對應 prodassys_spc_ar.4gl 的 FUNCTION backup_data()
    /// </summary>
    public static class ProdassysSpcArService
    {
        private const int BATCH_SIZE =200000;

        /// <summary>
        /// 將查詢結果分批匯出到檔案（每2000筆匯出一次）
        /// </summary>
        private static void ExportTable(
            IFXTransaction tx,
            string tableName,
            string whereClause,
            string fileName,
            int toDate,
            ref short errCnt,
            string errorTag,
            Action<string> logCallback)
        {
            try
            {
                int offset = 0;
                int totalExported = 0;
                bool hasMoreData = true;

                while (hasMoreData)
                {
                    // 使用 SKIP 和 FIRST 來分批查詢
                    string selectSql = $"SELECT SKIP {offset} FIRST {BATCH_SIZE} * FROM {tableName} WHERE {whereClause}";

                    DataTable dt = IfxDataAccess.ExecuteDataTable(tx, selectSql);
                    DataExporter.ExportData_Append(null, fileName, $"PRODASSYS_SPC_AR_{DateTime.Now.ToString("yyyyMM")}", logCallback);
                    if (dt.Rows.Count > 0)
                    {
                        // 匯出這批資料（append 模式）
                        DataExporter.ExportData_Append(dt, fileName, $"PRODASSYS_SPC_AR_{DateTime.Now.ToString("yyyyMM")}", logCallback);

                        totalExported += dt.Rows.Count;
                        logCallback($"   已匯出 {totalExported} 筆資料...");

                        // 如果這批資料少於 BATCH_SIZE，表示已經是最後一批
                        if (dt.Rows.Count < BATCH_SIZE)
                        {
                            hasMoreData = false;
                        }
                        else
                        {
                            offset += BATCH_SIZE;
                        }
                    }
                    else
                    {
                        hasMoreData = false;
                    }
                }

                logCallback($"   完成！總共匯出 {totalExported} 筆資料");
            }
            catch (Exception ex)
            {
                errCnt++;
                logCallback("error---[" + errorTag + "]> " + ex.Message);
                LogHelper.WriteLog("PRODASSYS_SPC_AR",
                    "error---[" + errorTag + "]> " + ex.Message);
            }
        }

        /// <summary>
        /// 對應 FUNCTION backup_data()
        /// toDate 對應 v_to_date (integer yyyyMMdd)
        /// </summary>
        public static void BackupData(
            IFXTransaction tx,
            int toDate,
            ref short errCnt,
            Action<string> logCallback)
        {
            // 4GL 中的 DB 是 eit，且表來自 eit@prodassys:xxx
            // 這邊假設目前 connection 已連到 eit，select 用「eit@prodassys:table」寫死

            // 1. spc_ds
            logCallback("1. BACKUP APG : spc_ds");
            ExportTable(
                tx,
                "eit@prodassys:spc_ds",
                $"sds_keyin_date < {toDate}",
                "spc_ds.995",
                toDate,
                ref errCnt,
                "BACKUP 2F : spc_ds",
                logCallback);

            // 2. spc_da
            logCallback("2. BACKUP APG : spc_da");
            ExportTable(
                tx,
                "eit@prodassys:spc_da",
                $"sda_keyin_date < {toDate}",
                "spc_da.995",
                toDate,
                ref errCnt,
                "BACKUP 2F : spc_da",
                logCallback);

            // 3. spc_wb
            logCallback("3. BACKUP APG : spc_wb");
            ExportTable(
                tx,
                "eit@prodassys:spc_wb",
                $"swb_keyin_date < {toDate}",
                "spc_wb.995",
                toDate,
                ref errCnt,
                "BACKUP 2F : spc_wb",
                logCallback);

            // 4. spc_md
            logCallback("4. BACKUP APG : spc_md");
            ExportTable(
                tx,
                "eit@prodassys:spc_md",
                $"smd_keyin_date < {toDate}",
                "spc_md.995",
                toDate,
                ref errCnt,
                "BACKUP 2F : spc_md",
                logCallback);

            // 5. spc_wg
            logCallback("5. BACKUP APG : spc_wg");
            ExportTable(
                tx,
                "eit@prodassys:spc_wg",
                $"swg_keyin_date < {toDate}",
                "spc_wg.995",
                toDate,
                ref errCnt,
                "BACKUP 2F : spc_wg",
                logCallback);

            // 6. spc_sp
            logCallback("6. BACKUP APG : spc_sp");
            ExportTable(
                tx,
                "eit@prodassys:spc_sp",
                $"ssp_keyin_date < {toDate}",
                "spc_sp.995",
                toDate,
                ref errCnt,
                "BACKUP 2F : spc_sp",
                logCallback);

            // 7. spc_tf
            logCallback("7. BACKUP APG : spc_tf");
            ExportTable(
                tx,
                "eit@prodassys:spc_tf",
                $"stf_keyin_date < {toDate}",
                "spc_tf.995",
                toDate,
                ref errCnt,
                "BACKUP 2F : spc_tf",
                logCallback);

            // 8. spc_di
            logCallback("8. BACKUP APG : spc_di");
            ExportTable(
                tx,
                "eit@prodassys:spc_di",
                $"sdi_keyin_date < {toDate}",
                "spc_di.995",
                toDate,
                ref errCnt,
                "BACKUP 2F : spc_di",
                logCallback);

            // 9. spc_bp
            logCallback("9. BACKUP APG : spc_bp");
            ExportTable(
                tx,
                "eit@prodassys:spc_bp",
                $"sbp_keyin_date < {toDate}",
                "spc_bp.995",
                toDate,
                ref errCnt,
                "BACKUP 2F : spc_bp",
                logCallback);

            // 10. spc_lp
            logCallback("10. BACKUP APG : spc_lp");
            ExportTable(
                tx,
                "eit@prodassys:spc_lp",
                $"slp_keyin_date < {toDate}",
                "spc_lp.995",
                toDate,
                ref errCnt,
                "BACKUP 2F : spc_lp",
                logCallback);

            // 11. spc_pmc
            logCallback("11. BACKUP APG : spc_pmc");
            ExportTable(
                tx,
                "eit@prodassys:spc_pmc",
                $"spmc_keyin_date < {toDate}",
                "spc_pmc.995",
                toDate,
                ref errCnt,
                "BACKUP 2F : spc_pmc",
                logCallback);

            // 12. spc_wr
            logCallback("12. BACKUP APG : spc_wr");
            ExportTable(
                tx,
                "eit@prodassys:spc_wr",
                $"swr_keyin_date < {toDate}",
                "spc_wr.995",
                toDate,
                ref errCnt,
                "BACKUP 2F : spc_wr",
                logCallback);

            // 13. spc_ls
            logCallback("13. BACKUP APG : spc_ls");
            ExportTable(
                tx,
                "eit@prodassys:spc_ls",
                $"sls_keyin_date < {toDate}",
                "spc_ls.995",
                toDate,
                ref errCnt,
                "BACKUP 2F : spc_ls",
                logCallback);

            // 14. spc_pda
            logCallback("14. BACKUP APG : spc_pda");
            ExportTable(
                tx,
                "eit@prodassys:spc_pda",
                $"spda_keyin_date < {toDate}",
                "spc_pda.995",
                toDate,
                ref errCnt,
                "BACKUP 2F : spc_pda",
                logCallback);

            // 15. spc_criteria_ent
            logCallback("15. BACKUP APG : spc_criteria_ent");
            ExportTable(
                tx,
                "eit@prodassys:spc_criteria_ent",
                $"keyin_date < {toDate}",
                "spc_criteria_ent.995",
                toDate,
                ref errCnt,
                "BACKUP 2F : spc_criteria_ent",
                logCallback);

            // 16. spc_criteria_lot
            logCallback("16. BACKUP APG : spc_criteria_lot");
            ExportTable(
                tx,
                "eit@prodassys:spc_criteria_lot",
                $"keyin_date < {toDate}",
                "spc_criteria_lot.995",
                toDate,
                ref errCnt,
                "BACKUP 2F : spc_criteria_lot",
                logCallback);

            // 17. spc_edc
            logCallback("17. BACKUP APG : spc_edc");
            ExportTable(
                tx,
                "eit@prodassys:spc_edc",
                $"ec_date < {toDate}",
                "spc_edc.995",
                toDate,
                ref errCnt,
                "BACKUP 2F : spc_edc",
                logCallback);
        }
    }
}