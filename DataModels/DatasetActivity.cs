namespace Deloitte.MnANextGenAnalytics.WebAPI.DataModels
{
    public class DatasetActivity
    {
        public int Id { get; set; }

        public int Engagement_Id { get; set; }

        public string Filename { get; set; }

        public string Message { get; set; }

        public string CreatedUser { get; set; }
        
        public DateTime CreatedDate { get; set; }


    }
}
