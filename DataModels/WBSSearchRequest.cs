namespace Deloitte.MnANextGenAnalytics.WebAPI.DataModels
{
    public class WBSSearchRequest
    {
        public List<Order> order { get; set; }
        public string searchText { get; set; }
        public string accssToken { get; set; }
        public int page { get; set; }
        public int pageSize { get; set; }
    }
    public class Order
    {
        public string orderBy { get; set; }
        public string orderDir { get; set; }
    }
}
