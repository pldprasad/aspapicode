namespace Deloitte.MnANextGenAnalytics.WebAPI.DataModels
{
    public class EngagementPortfolio
    {
        public int? Id { get; set; }
        public string? Name { get; set; }
        public string? CreatedUser { get; set; }

        public DateTime? CreatedDate { get; set; } 
        public string? ModifiedUser { get; set; }
        public DateTime? ModifiedDate { get; set; }
        public bool? IsActive { get; set; }


    }

    public class EngagementDetails
    {
       public int? id { get; set; }
       public  string? createddate { get; set; }
       
        public string? displayDate { get; set; }
        public string submittedUserId { get; set; }
        public  string? submittedby { get; set; }
       public  string? engagementname { get; set; }
       public  int? offeringPortfolio { get; set; }
       public  string? wbscode { get; set; }
       public string? adlsfoldername { get; set; }
       public  string? adlsfolderurl { get; set; }
       public  List<string>? datasets { get; set; }
       public  bool? engagementisbuyside { get; set; }
       public  List<Approver>? ppmdapprovers { get; set; }
       public  List<Approver>? engagementteams { get; set; }
       public  List<ClientTeam>? clientTeams { get; set; }
       public  string? status { get; set; }
       public  bool? editpermission { get; set; }
       public int? currentStatus { get; set; }
       public int? previousStatus { get; set; }

    }
    public class Approver
    {
       public  string? displayName { get; set; }
       public  string? id { get; set; }
       public  string? imgUrl { get; set; }
       public  string? givenName { get; set; }
       public  string? surName { get; set; }
       public  bool? readPermission { get; set; }
       public  bool? writePermission { get; set; }
    }
    public class ClientTeam
    {
       public string? firstName { get; set; }
       public string? lastName { get; set; }
       public string? email { get; set; }

    }
}
