using CPISData.Data;
using IBM.Data.Informix;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MonthBackup_FE.EAR.Provider
{
    /// <summary>
    /// 資料庫提供者：管理全域參數、連線字串、entity 清單
    /// </summary>
    public static class DatabaseProvider
    {
        // 資料庫連線字串
        public static string ConnectionString = ConnectionStrings.FromName("comets");

        // 全域參數
        public static int BegDate { get; set; }
        public static int EndDate { get; set; }
        public static int MaxAllowedDate { get; private set; }

        // 儲存 ntcent_cur 查詢結果的 entity 集合
        private static List<string> _entityList = null;

        /// <summary>
        /// 靜態建構子：初始化最大允許日期
        /// </summary>
        static DatabaseProvider()
        {
            CalculateMaxAllowedDate();
        }

        /// <summary>
        /// Entity 清單屬性 (Lazy Loading)
        /// </summary>
        public static List<string> EntityList
        {
            get
            {
                if (_entityList == null)
                {
                    LoadEntityList();
                }
                return _entityList;
            }
        }

        /// <summary>
        /// 計算最大允許日期
        /// if MONTH(TODAY) <= 3 THEN let i = YEAR(TODAY) * 10000 + MONTH(TODAY) * 100 + 1 - 9100
        /// else let i = YEAR(TODAY) * 10000 + MONTH(TODAY) * 100 + 1 - 300
        /// </summary>
        private static void CalculateMaxAllowedDate()
        {
            DateTime today = DateTime.Today;
            int year = today.Year;
            int month = today.Month;

            if (month <= 3)
            {
                MaxAllowedDate = year * 10000 + month * 100 + 1 - 9100;
            }
            else
            {
                MaxAllowedDate = year * 10000 + month * 100 + 1 - 300;
            }
        }

        /// <summary>
        /// 驗證日期是否合法
        /// </summary>
        public static bool ValidateDate(int date, out string errorMessage)
        {
            if (date >= MaxAllowedDate)
            {
                errorMessage = $"日期 {date} 必須小於 {MaxAllowedDate}，請檢查輸入日期！";
                return false;
            }
            errorMessage = string.Empty;
            return true;
        }

        /// <summary>
        /// 從資料庫載入 entity 清單
        /// 對應 4GL: declare ntcent_cur cursor for select nent_entity from ntcent where nent_facility = "ASSY"
        /// </summary>
        private static void LoadEntityList()
        {
            _entityList = new List<string>();

            string sql = @"SELECT nent_entity 
                          FROM ntcent 
                          WHERE nent_facility = 'ASSY'";

            using (var connection = new IfxConnection(ConnectionString))
            {
                connection.Open();

                // SET ISOLATION TO DIRTY READ
                using (var cmd = new IfxCommand("SET ISOLATION TO DIRTY READ", connection))
                {
                    cmd.ExecuteNonQuery();
                }

                using (var command = new IfxCommand(sql, connection))
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string entity = reader.GetString(0).Trim();
                        _entityList.Add(entity);
                    }
                }
            }

            Console.WriteLine($"已載入 {_entityList.Count} 個 entity");
        }

        /// <summary>
        /// 重新載入 entity 清單
        /// </summary>
        public static void ReloadEntityList()
        {
            _entityList = null;
        }

        /// <summary>
        /// 將整數日期轉換為 DateTime
        /// 對應 4GL: LET j = i / 10000, LET k = (i - j * 10000) / 100, LET p_curr_date = MDY(k, i MOD 100, j)
        /// </summary>
        public static DateTime ConvertIntToDate(int dateInt)
        {
            int year = dateInt / 10000;
            int month = (dateInt - year * 10000) / 100;
            int day = dateInt % 100;
            return new DateTime(year, month, day);
        }
    }
}
