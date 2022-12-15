namespace Deloitte.MnANextGenAnalytics.WebAPI.DataModels
{
    public class ProfileBatchResponse
    {
        public List<Responses> responses { get; set; }
    }
    public class Responses
    {
        public string id { get; set; }
        public int status { get; set; }
        public Headers headers { get; set; }
        public string body { get; set; }
    }
    public class Headers
    {
        public string CacheControl { get; set; }
        public string XContentTypeOptions { get; set; }
        public string ContentType { get; set; }
        public string ETag { get; set; }
    }
}
