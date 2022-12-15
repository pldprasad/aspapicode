namespace Deloitte.MnANextGenAnalytics.WebAPI.DataModels
{
    public class Dataset
    {
        public string dataset_name { get; set; }
        public string tablename { get; set; }
        public bool uploaded_to_databricks { get; set; }
    }
}
