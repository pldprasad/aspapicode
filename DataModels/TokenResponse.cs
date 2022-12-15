namespace Deloitte.MnANextGenAnalytics.WebAPI.DataModels
{
    public class TokenResponse
    {
        public string token_type { get; set; }
        public string scope { get; set; }
        public string access_token { get; set; }
    }
}
