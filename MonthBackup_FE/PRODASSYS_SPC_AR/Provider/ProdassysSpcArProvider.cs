using System;

namespace MonthBackup_FE.PRODASSYS_SPC_AR.Provider
{
    /// <summary>
    /// 對應 prodassys_spc_ar.4gl GLOBAL & 日期計算
    /// </summary>
    public static class ProdassysSpcArProvider
    {
        /// <summary>
        /// 4GL v_run_date (傳入 yyyy-mm 或 TODAY yyyy-mm)
        /// </summary>
        public static string RunDateRaw { get; set; }

        /// <summary>
        /// 對應 from_yyyymm，例：20040901 (int)
        /// </summary>
        public static int FromDateInt { get; set; }

        /// <summary>
        /// 對應 to_yyyymm，例：20041001 (int)
        /// </summary>
        public static int ToDateInt { get; set; }

        /// <summary>
        /// 對應 v_yyyymm (datetime year to month)
        /// </summary>
        public static DateTime VYyyymm { get; set; }

        /// <summary>
        /// 對應 v_6m_from
        /// </summary>
        public static DateTime V6mFrom { get; set; }

        /// <summary>
        /// 對應 v_6m_to
        /// </summary>
        public static DateTime V6mTo { get; set; }

        /// <summary>
        /// 根據 4GL MAIN 的邏輯計算 v_yyyymm, v_6m_from, v_6m_to, v_from_date, v_to_date
        /// - 若 runDateRaw 為空 → 用今天的 yyyy-MM
        /// - 否則用傳入的 yyyy-MM
        /// </summary>
        public static void InitDates(string runDateRaw)
        {
            if (string.IsNullOrEmpty(runDateRaw))
            {
                // num_args() = 0 → TODAY USING "yyyy-mm"
                RunDateRaw = DateTime.Today.ToString("yyyy-MM");
            }
            else
            {
                string s = runDateRaw.Trim();

                // 去掉中間的分隔符 '-' 或 '/' 來判斷長度
                string digitsOnly = s.Replace("-", "").Replace("/", "");

                if (digitsOnly.Length == 6)
                {
                    // yyyyMM → 轉成 yyyy-MM
                    // 例: 202501 → 2025-01
                    string yyyy = digitsOnly.Substring(0, 4);
                    string mm = digitsOnly.Substring(4, 2);
                    RunDateRaw = yyyy + "-" + mm;
                }
                else if (digitsOnly.Length == 8)
                {
                    // yyyyMMdd → 轉成 yyyy-MM
                    // 例: 20250115 → 2025-01
                    string yyyy = digitsOnly.Substring(0, 4);
                    string mm = digitsOnly.Substring(4, 2);
                    RunDateRaw = yyyy + "-" + mm;
                }
                else if (s.Length == 7 && s[4] == '-')
                {
                    // 已經是 yyyy-MM
                    RunDateRaw = s;
                }
                else
                {
                    throw new ArgumentException("v_run_date 格式不正確，請輸入 yyyyMM / yyyy-MM / yyyyMMdd", "runDateRaw");
                }
            }

            // v_yyyymm = v_run_date (datetime year to month)
            // 我們用該月一號代表
            DateTime yyyymm;
            if (!DateTime.TryParseExact(RunDateRaw + "-01", "yyyy-MM-dd",
                    System.Globalization.CultureInfo.InvariantCulture,
                    System.Globalization.DateTimeStyles.None,
                    out yyyymm))
            {
                throw new ArgumentException("v_run_date 格式錯誤，應為 yyyy-mm: " + RunDateRaw, "runDateRaw");
            }

            VYyyymm = yyyymm;

            // 4GL:
            // v_6m_from = v_yyyymm - 6 months
            // v_6m_to   = v_yyyymm - 5 months
            V6mFrom = VYyyymm.AddMonths(-6);
            V6mTo = VYyyymm.AddMonths(-5);

            // from_yyyymm = v_6m_from[1,4] + v_6m_from[6,7] + "01"
            // to_yyyymm   = v_6m_to[1,4] + v_6m_to[6,7] + "01"
            // 然後 cast 為 integer
            string fromYyyymmdd = V6mFrom.ToString("yyyyMM") + "01";
            string toYyyymmdd = V6mTo.ToString("yyyyMM") + "01";

            int fromInt;
            int toInt;

            if (!int.TryParse(fromYyyymmdd, out fromInt))
                throw new Exception("無法轉換 from_yyyymm 為整數: " + fromYyyymmdd);
            if (!int.TryParse(toYyyymmdd, out toInt))
                throw new Exception("無法轉換 to_yyyymm 為整數: " + toYyyymmdd);

            FromDateInt = fromInt;
            ToDateInt = toInt;
        }
    }
}