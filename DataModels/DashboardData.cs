namespace Deloitte.MnANextGenAnalytics.WebAPI.DataModels
{
    public class DashboardData
    {
        public int EngagementID { get; set; }
        public string EngagementName { get; set; }
        public string Offering_Portfolio { get; set; }
        public string wbscode { get; set; }
        public string Engagement_Status { get; set; }
        public string ADLS_Folder { get; set; }
        public bool Engagement_Buy_Side { get; set; }
        public int statuschangeduration { get; set; }
    }
}
