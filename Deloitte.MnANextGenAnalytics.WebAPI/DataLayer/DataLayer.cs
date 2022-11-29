using Deloitte.MnANextGenAnalytics.WebAPI.DataModels;
using System.Data;
using System.Data.Odbc;

namespace Deloitte.MnANextGenAnalytics.WebAPI.DataLayer
{
    public class DataLayer
    {
        private readonly IConfiguration _config;
        private readonly ILogger _logger;

        public DataLayer(IConfiguration config,ILogger logger)
        {
            _config = config;
            _logger = logger;
        }
        public  string GetODBCConnection()
        {            
            var Driver = _config.GetValue<string>("DatabricksConnection:Driver");
            var DSN = _config.GetValue<string>("DatabricksConnection:DSN");
            var Host = _config.GetValue<string>("DatabricksConnection:Host");
            var Port = _config.GetValue<string>("DatabricksConnection:Port");
            var SSL = _config.GetValue<string>("DatabricksConnection:SSL");
            var ThriftTransport = _config.GetValue<string>("DatabricksConnection:ThriftTransport");
            var AuthMech = _config.GetValue<string>("DatabricksConnection:AuthMech");
            var UID = _config.GetValue<string>("DatabricksConnection:UID");
            var PWD = _config.GetValue<string>("DatabricksConnection:PWD");
            var HTTPPath = _config.GetValue<string>("DatabricksConnection:HTTPPath");
            OdbcConnectionStringBuilder odbcConnectionStringBuilder = new OdbcConnectionStringBuilder
            {
                Driver = Driver,
                Dsn = DSN

            };
            odbcConnectionStringBuilder.Add("Host", Host);
            odbcConnectionStringBuilder.Add("Port", Port);
            odbcConnectionStringBuilder.Add("SSL", SSL);
            odbcConnectionStringBuilder.Add("ThriftTransport", ThriftTransport);
            odbcConnectionStringBuilder.Add("AuthMech", AuthMech);
            odbcConnectionStringBuilder.Add("UID", UID);
            odbcConnectionStringBuilder.Add("PWD", PWD);
            odbcConnectionStringBuilder.Add("HTTPPath", HTTPPath);

            return odbcConnectionStringBuilder.ConnectionString;
        }

        public int UpdateEngagementDetails(EngagementDetails data)
        {
            try
            {
                //_logger.LogDebug($"Debug {data.id}");
                //_logger.LogInformation($"Information {data.id}");
                //_logger.LogWarning($"Warning {data.id}");
                //_logger.LogError($"Error {data.id}");
                //_logger.LogCritical($"Critical {data.id}");
                string connectionString = GetODBCConnection();
                string query1 = "";
                string date = "getdate()";
                int rowAffected = 0;
                int userId =0;
                OdbcCommand? command = null;
                OdbcDataReader? reader = null;

                using (OdbcConnection connection = new OdbcConnection(connectionString))
                {
                    connection.Open();
                    //adding the data for testing
                    //updating the engagement details details                    
                     query1 = string.Format("Update mna.engagement_details set Engagement_Name = '{0}'," +
                     "Offering_Portfolio_Id='{1}'," +
                    "WBS_Code='{2}'," +
                    "Is_Engagement_Buy_Side={3}," +
                    "Current_state={4}," +
                    "IsActive={5}," +
                    "CreatedUser='{6}'," +
                    "CreatedDate={7}," +
                    "ModifiedUser='{8}'," +
                    "ModifiedDate={9}," +
                    "Previous_state={10}"+
                    " Where Id={11}", data.engagementname, data.offeringPortfolio, data.wbscode, data.engagementisbuyside, data.currentStatus, true, data.submittedby, date, data.submittedby, date, data.previousStatus,data.id);

                     command = new OdbcCommand(query1, connection);
                     rowAffected = command.ExecuteNonQuery();                    
                    //deleting the all users data for the update purpose
                    query1= String.Format("delete from mna.user_roles where Engagement_Id= {0}",data.id);
                    command= new OdbcCommand(query1, connection);
                    rowAffected= command.ExecuteNonQuery();

                    //adding the data to users table
                    if ((data.ppmdapprovers != null) && (data.ppmdapprovers.Any()))
                    {
                        foreach (var ppmdapprover in data.ppmdapprovers)
                        {

                            query1 = String.Format("select count(*) from mna.users where EmailId = '{0}'", ppmdapprover.id);
                            command = new OdbcCommand(query1, connection);

                            int count = 0;
                            count = Convert.ToInt32(command.ExecuteScalar());

                            if (count == 0)
                            {
                                query1 = String.Format("insert into mna.users(Alias," +
                                "Last_Name," +
                                "First_Name," +
                                "IsPPMD," +
                                "EmailID," +
                                "Domain," +
                                "Office," +
                                "IsActive," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "Title) values('{0}','{1}','{2}',{3},'{4}','{5}','{6}','{7}','{8}',{9},'{10}',{11},'{12}')", ppmdapprover.displayName, ppmdapprover.displayName, ppmdapprover.displayName, true, ppmdapprover.id, "Domain", "Office", true, data.submittedby, date, data.submittedby, date, "title");

                                command = new OdbcCommand(query1, connection);
                                rowAffected = command.ExecuteNonQuery();
                            }

                            //Adding user to user role table (user and engagement details)

                            query1 = string.Format("select id from mna.users where EmailID='{0}'", ppmdapprover.id);
                            command = new OdbcCommand(query1, connection);
                            reader = command.ExecuteReader();
                            userId = 0;
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    userId = reader.GetInt32(0);
                                }
                            }

                            if (userId > 0)
                            {
                                query1 = String.Format("insert into mna.user_roles(UserId," +
                                "Engagement_Id," +
                                "Role_Id," +
                                "Can_Write," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "IsActive) values({0},{1},{2},{3},'{4}',{5},'{6}',{7},{8})", userId, data.id, 1, ppmdapprover.writePermission, data.submittedby, date, data.submittedby, date, true);

                                command = new OdbcCommand(query1, connection);

                                rowAffected = command.ExecuteNonQuery();

                            }
                        }



                    }

                    //adding team member to user table
                    if ((data.engagementteams != null) && (data.engagementteams.Any()))
                    {
                        foreach (var engagementteam in data.engagementteams)
                        {

                            query1 = String.Format("select count(*) from mna.users where EmailId = '{0}'", engagementteam.id);
                            command = new OdbcCommand(query1, connection);

                            int count = 0;
                            count = Convert.ToInt32(command.ExecuteScalar());

                            if (count == 0)
                            {
                                query1 = String.Format("insert into mna.users(Alias," +
                                "Last_Name," +
                                "First_Name," +
                                "IsPPMD," +
                                "EmailID," +
                                "Domain," +
                                "Office," +
                                "IsActive," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "Title) values('{0}','{1}','{2}',{3},'{4}','{5}','{6}','{7}','{8}',{9},'{10}',{11},'{12}')", engagementteam.displayName, engagementteam.displayName, engagementteam.displayName, false, engagementteam.id, "Domain", "Office", true, data.submittedby, date, data.submittedby, date, "title");

                                command = new OdbcCommand(query1, connection);
                                rowAffected = command.ExecuteNonQuery();
                            }

                            //Adding user to user role table (user and engagement details)

                            query1 = string.Format("select id from mna.users where EmailID='{0}'", engagementteam.id);
                            command = new OdbcCommand(query1, connection);
                            reader = command.ExecuteReader();
                            userId = 0;
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    userId = reader.GetInt32(0);
                                }
                            }

                            if (userId > 0)
                            {
                                query1 = String.Format("insert into mna.user_roles(UserId," +
                                "Engagement_Id," +
                                "Role_Id," +
                                "Can_Write," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "IsActive) values({0},{1},{2},{3},'{4}',{5},'{6}',{7},{8})", userId, data.id, 2, engagementteam.writePermission, data.submittedby, date, data.submittedby, date, true);

                                command = new OdbcCommand(query1, connection);

                                rowAffected = command.ExecuteNonQuery();

                            }
                        }



                    }

                    // adding client menmber to the table
                    if ((data.clientTeams != null) && (data.clientTeams.Any()))
                    {
                        foreach (var clienntteam in data.clientTeams)
                        {

                            query1 = String.Format("select count(*) from mna.users where EmailId = '{0}'", clienntteam.email);
                            command = new OdbcCommand(query1, connection);

                            int count = 0;
                            count = Convert.ToInt32(command.ExecuteScalar());
                            string fullName = "" + clienntteam.firstName + " " + clienntteam.lastName;

                            if (count == 0)
                            {
                                query1 = String.Format("insert into mna.users(Alias," +
                                "Last_Name," +
                                "First_Name," +
                                "IsPPMD," +
                                "EmailID," +
                                "Domain," +
                                "Office," +
                                "IsActive," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "Title) values('{0}','{1}','{2}',{3},'{4}','{5}','{6}','{7}','{8}',{9},'{10}',{11},'{12}')", fullName, clienntteam.lastName, clienntteam.firstName, false, clienntteam.email, "Domain", "Office", true, data.submittedby, date, data.submittedby, date, "title");

                                command = new OdbcCommand(query1, connection);
                                rowAffected = command.ExecuteNonQuery();
                            }

                            //Adding user to user role table (user and engagement details)

                            query1 = string.Format("select id from mna.users where EmailID='{0}'", clienntteam.email);
                            command = new OdbcCommand(query1, connection);
                            reader = command.ExecuteReader();
                            userId = 0;
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    userId = reader.GetInt32(0);
                                }
                            }

                            if (userId > 0)
                            {
                                query1 = String.Format("insert into mna.user_roles(UserId," +
                                "Engagement_Id," +
                                "Role_Id," +
                                "Can_Write," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "IsActive) values({0},{1},{2},{3},'{4}',{5},'{6}',{7},{8})", userId, data.id, 3, false, data.submittedby, date, data.submittedby, date, true);

                                command = new OdbcCommand(query1, connection);

                                rowAffected = command.ExecuteNonQuery();

                            }
                        }



                    }




                    return (int)data.id;
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e.Message);
                return -1;
            }
        
        }

        public int SaveEngagementDetails(EngagementDetails data)
        {
            string connectionString = GetODBCConnection();
            string resp = "";
            string query1 = "";
            string date = "getdate()";
            int latestEngagementId = 0;
            int userId = 0;
            int rowAffected = 0;
            
            //adding the engagement details to table
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {          
                connection.Open();
                try
                {
                    query1 = string.Format("INSERT into mna.engagement_details(Engagement_Name," +
                    "Offering_Portfolio_Id," +
                    "WBS_Code," +
                    "Is_Engagement_Buy_Side," +
                    "Current_state," +                    
                    "IsActive," +
                    "CreatedUser," +
                    "CreatedDate," +
                    "ModifiedUser," +
                    "ModifiedDate," +
                    "Previous_state) values('{0}',{1},'{2}',{3},{4},{5},'{6}',{7},'{8}',{9},{10})", data.engagementname, data.offeringPortfolio, data.wbscode, data.engagementisbuyside, data.currentStatus, true, data.submittedby, date, data.submittedby, date, data.previousStatus);

                    OdbcCommand command = new OdbcCommand(query1, connection);
                    rowAffected = command.ExecuteNonQuery();
                    
                    query1 = "select  Id  from mna.engagement_details  order by Id desc limit 1";
                    command = new OdbcCommand(query1, connection);
                    OdbcDataReader reader = command.ExecuteReader();

                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            latestEngagementId = reader.GetInt32(0);
                        }
                    }

                    //adding ppmd  to users table
                    if ((data.ppmdapprovers != null) && (data.ppmdapprovers.Any()))
                    {
                        foreach (var ppmdapprover in data.ppmdapprovers)
                        {

                            query1 = String.Format("select count(*) from mna.users where EmailId = '{0}'", ppmdapprover.id);
                            command = new OdbcCommand(query1, connection);

                            int count = 0;
                            count = Convert.ToInt32(command.ExecuteScalar());

                            if (count == 0)
                            {
                                query1 = String.Format("insert into mna.users(Alias," +
                                "Last_Name," +
                                "First_Name," +
                                "IsPPMD," +
                                "EmailID," +
                                "Domain," +
                                "Office," +
                                "IsActive," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "Title) values('{0}','{1}','{2}',{3},'{4}','{5}','{6}','{7}','{8}',{9},'{10}',{11},'{12}')", ppmdapprover.displayName, ppmdapprover.displayName, ppmdapprover.displayName, true, ppmdapprover.id, "Domain", "Office", true, data.submittedby, date, data.submittedby, date,"title");

                                command = new OdbcCommand(query1, connection);
                                rowAffected= command.ExecuteNonQuery();
                            }

                            //Adding user to user role table (user and engagement details)

                            query1 = string.Format("select id from mna.users where EmailID='{0}'", ppmdapprover.id);
                            command = new OdbcCommand(query1, connection);
                            reader = command.ExecuteReader();
                            userId = 0;
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    userId = reader.GetInt32(0);
                                }
                            }

                            if (userId > 0)
                            {
                                query1 = String.Format("insert into mna.user_roles(UserId," +
                                "Engagement_Id," +
                                "Role_Id," +
                                "Can_Write," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "IsActive) values({0},{1},{2},{3},'{4}',{5},'{6}',{7},{8})",userId,latestEngagementId, 1, ppmdapprover.writePermission, data.submittedby, date, data.submittedby, date, true);

                                command = new OdbcCommand(query1, connection);

                                rowAffected = command.ExecuteNonQuery();

                            }
                        }



                    }

                    //adding team member to user table
                    if ((data.engagementteams != null) && (data.engagementteams.Any()))
                    {
                        foreach (var engagementteam in data.engagementteams)
                        {

                            query1 = String.Format("select count(*) from mna.users where EmailId = '{0}'", engagementteam.id);
                            command = new OdbcCommand(query1, connection);

                            int count = 0;
                            count = Convert.ToInt32(command.ExecuteScalar());

                            if (count == 0)
                            {
                                query1 = String.Format("insert into mna.users(Alias," +
                                "Last_Name," +
                                "First_Name," +
                                "IsPPMD," +
                                "EmailID," +
                                "Domain," +
                                "Office," +
                                "IsActive," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "Title) values('{0}','{1}','{2}',{3},'{4}','{5}','{6}','{7}','{8}',{9},'{10}',{11},'{12}')", engagementteam.displayName, engagementteam.displayName, engagementteam.displayName, false, engagementteam.id, "Domain", "Office", true, data.submittedby, date, data.submittedby, date, "title");

                                command = new OdbcCommand(query1, connection);
                                rowAffected = command.ExecuteNonQuery();
                            }

                            //Adding user to user role table (user and engagement details)

                            query1 = string.Format("select id from mna.users where EmailID='{0}'", engagementteam.id);
                            command = new OdbcCommand(query1, connection);
                            reader = command.ExecuteReader();
                            userId = 0;
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    userId = reader.GetInt32(0);
                                }
                            }

                            if (userId > 0)
                            {
                                query1 = String.Format("insert into mna.user_roles(UserId," +
                                "Engagement_Id," +
                                "Role_Id," +
                                "Can_Write," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "IsActive) values({0},{1},{2},{3},'{4}',{5},'{6}',{7},{8})", userId, latestEngagementId, 2, engagementteam.writePermission, data.submittedby, date, data.submittedby, date, true);

                                command = new OdbcCommand(query1, connection);

                                rowAffected = command.ExecuteNonQuery();

                            }
                        }



                    }

                    // adding client menmber to the table
                    if ((data.clientTeams != null) && (data.clientTeams.Any()))
                    {
                        foreach (var clienntteam in data.clientTeams)
                        {

                            query1 = String.Format("select count(*) from mna.users where EmailId = '{0}'", clienntteam.email);
                            command = new OdbcCommand(query1, connection);

                            int count = 0;
                            count = Convert.ToInt32(command.ExecuteScalar());
                            string fullName = "" + clienntteam.firstName + " " + clienntteam.lastName; 

                            if (count == 0)
                            {
                                query1 = String.Format("insert into mna.users(Alias," +
                                "Last_Name," +
                                "First_Name," +
                                "IsPPMD," +
                                "EmailID," +
                                "Domain," +
                                "Office," +
                                "IsActive," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "Title) values('{0}','{1}','{2}',{3},'{4}','{5}','{6}','{7}','{8}',{9},'{10}',{11},'{12}')", fullName, clienntteam.lastName, clienntteam.firstName, false, clienntteam.email, "Domain", "Office", true, data.submittedby, date, data.submittedby, date, "title");

                                command = new OdbcCommand(query1, connection);
                                rowAffected = command.ExecuteNonQuery();
                            }

                            //Adding user to user role table (user and engagement details)

                            query1 = string.Format("select id from mna.users where EmailID='{0}'", clienntteam.email);
                            command = new OdbcCommand(query1, connection);
                            reader = command.ExecuteReader();
                            userId = 0;
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    userId = reader.GetInt32(0);
                                }
                            }

                            if (userId > 0)
                            {
                                query1 = String.Format("insert into mna.user_roles(UserId," +
                                "Engagement_Id," +
                                "Role_Id," +
                                "Can_Write," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "IsActive) values({0},{1},{2},{3},'{4}',{5},'{6}',{7},{8})", userId, latestEngagementId, 3, false, data.submittedby, date, data.submittedby, date, true);

                                command = new OdbcCommand(query1, connection);

                                rowAffected = command.ExecuteNonQuery();

                            }
                        }



                    }

                    return latestEngagementId;

                }
                catch (Exception e)
                {
                    _logger.LogError(e.Message);
                    return -1;
                }
                //query2=
            }
        }
        public  List<EngagementPortfolio> GetOfferingPortfolio()
        {
            List<EngagementPortfolio> PortfolioList = new List<EngagementPortfolio>();

            string connectionString = GetODBCConnection();

            string resp = "";
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
               sqlQuery= "select * from mna.offering_portfolio";
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    OdbcDataReader reader = command.ExecuteReader();
                    DataTable dt = new DataTable();
                    dt.Load(reader);
                    PortfolioList = (from DataRow dr in dt.Rows
                                   select new EngagementPortfolio()
                                   {
                                       Id = Convert.ToInt32(dr["Id"]),
                                       Name = dr["Name"].ToString(),
                                       CreatedUser = dr["CreatedUser"].ToString(),
                                       CreatedDate = Convert.ToDateTime(dr["CreatedDate"]),
                                       ModifiedUser = dr["ModifiedUser"].ToString(),
                                       ModifiedDate = Convert.ToDateTime(dr["ModifiedDate"]),
                                       IsActive=Convert.ToBoolean(dr["IsActive"])
                                   }
                             ).ToList();


                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                    resp = ex.Message;
                }
            }
            return PortfolioList;
        }
        public string getReturnForReworkComment(int? engagementId)
        {
            string comment = string.Empty;
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = String.Format("select comment from mna.return_rework_comments where Engagement_Id  = {0} order by Id desc", engagementId);
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    OdbcDataReader reader = command.ExecuteReader();
                    DataTable dt = new DataTable();
                    dt.Load(reader);
                    DataRow dr = dt.Rows[0];
                    comment = dr[0].ToString();

                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                    return string.Empty;
                }
                return comment;
            }
        }
        public int SaveFolderDetails(string FolderURL,int? EngagementID)
        {
            int records = 0;
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = "Insert into mna.adls_details(Engagement_id,Folder_URL,CreatedUser,CreatedDate,ModifiedUser,ModifiedDate,IsActive)" +
                    "values("+EngagementID+","+"'"+FolderURL+ "','pvenkatasatyanara@deloitte.com',getdate(),'pvenkatasatyanara@deloitte.com',getdate(),true)";
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    records = command.ExecuteNonQuery();
                    
                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                }
            }
            return records;

        }

        public  SGGroupDetails GetSGGroup()
        {
            
            string connectionString = GetODBCConnection();
            SGGroupDetails sgGroupDetails = new SGGroupDetails();

            string resp = "";
            string sqlQuery;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = "select  * from mna.sggroupdetails where Is_inuse=false order by Id limit 1";
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    OdbcDataReader reader = command.ExecuteReader();
                    DataTable dt = new DataTable();
                    dt.Load(reader);
                    foreach(DataRow dr in dt.Rows)
                    {
                        sgGroupDetails.Id = Convert.ToInt32(dr["Id"]);
                        sgGroupDetails.GroupName = dr["GroupName"].ToString();
                        sgGroupDetails.GroupId = dr["GroupID"].ToString();
                        sgGroupDetails.Is_Inuse = Convert.ToBoolean(dr["Is_Inuse"]);
                        sgGroupDetails.Engagement_Id = Convert.ToInt32(dr["Engagement_Id"]);
                        sgGroupDetails.CreatedDate = Convert.ToDateTime(dr["CreatedDate"]);
                        sgGroupDetails.ModifiedUser = dr["ModifiedUser"].ToString();
                        sgGroupDetails.ModifiedDate = Convert.ToDateTime(dr["ModifiedDate"]);
                        sgGroupDetails.IsActive = Convert.ToBoolean(dr["IsActive"]);
                    }
                    

                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                    resp = ex.Message;
                }
                return sgGroupDetails;
            }
        }

        public int UpdateSGGroupDetails(int? EngagementID,string Groupname)
        {
            int records = 0;
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = "update mna.sggroupdetails set Is_Inuse=true,Engagement_Id="+EngagementID+" where GroupName='"+ Groupname + "'";
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    records = command.ExecuteNonQuery();

                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                }
            }
            return records;

        }
        public bool CheckEngagementAccess(int EngagementId, string EmailId)
        {
           
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            int userid = 0;
            //string accessCheckQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = String.Format("select ur.userid " + 
                            " from mna.user_roles ur inner join mna.users u on u.id = ur.userid" +
                            " inner join mna.roles r on r.id = ur.role_id" +
                            " where u.emailId = '{0}' and ur.Engagement_id = {1} and ur.Role_Id in (1,2)", EmailId, EngagementId);
                //accessCheckQuery = String.Format("select count(ur.Can_Write) from mna.user_roles ur inner join mna.users u on u.Id = ur.UserId where u.EmailID = '{0}' AND ur.Engagement_Id = {1}", EmailId, EngagementId);
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                //OdbcCommand accessCheckCommand = new OdbcCommand(accessCheckQuery, connection);
                connection.Open();
                try
                {
                    OdbcDataReader reader = command.ExecuteReader();
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            userid = reader.GetInt32(0);
                        }
                    }
                    if (userid > 0)
                    {
                        return true;
                    }
                    else
                    {
                        return false;
                    }


                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                    return false;
                }
            }
        }
        public List<SecurityMatrix> GetSecurityMatrix()
        {
            List<SecurityMatrix> securityMatrixList = new List<SecurityMatrix>();
            string connectionString = GetODBCConnection();
            string sqlQuery = "";            
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = ("select Engagement_Status_Id,PPMDApprovers,EngagementTeams from mna.securitymatrix"); 
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);                
                connection.Open();
                try
                {
                    OdbcDataReader reader = command.ExecuteReader();
                    DataTable dt = new DataTable();
                    dt.Load(reader);
                    foreach (DataRow dr in dt.Rows)
                    { 
                        SecurityMatrix securityMatrix = new SecurityMatrix();
                        securityMatrix.engagementStatusId = Convert.ToInt32(dr["Engagement_Status_Id"]);
                        securityMatrix.ppmdApprovers = Convert.ToBoolean(dr["PPMDApprovers"]);
                        securityMatrix.engagementTeams = Convert.ToBoolean(dr["EngagementTeams"]);
                        securityMatrixList.Add(securityMatrix);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                    return null;
                }
                return securityMatrixList;
            }
        }
        public EngagementData GetEngagementData(int EngagementId)
        {
            EngagementData engagementData = new EngagementData();
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            string EngagementQuery = "";
            string DatasetQuery = "";
            string filename = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {

                sqlQuery = "select users.Id,users.First_Name,users.Last_Name,users.EmailID,user_roles.Role_Id,roles.Role_name,user_roles.Can_Write from mna.users users inner join mna.user_roles user_roles on users.Id=user_roles.UserId" +
                            " inner join mna.roles roles on roles.Id = user_roles.Role_Id"+
                            " where user_roles.Engagement_Id ="+EngagementId;
                EngagementQuery = "select ed.Engagement_Name,ed.Offering_Portfolio_Id,ed.WBS_Code,ed.CreatedDate,ed.CreatedUser,ed.Is_Engagement_Buy_Side,ed.Current_state,ed.Previous_state,ad.folder_url" +
                                  " from mna.engagement_details ed left join mna.adls_details ad on ed.id=ad.engagement_id" + " where ed.Id="+EngagementId;
                DatasetQuery = string.Format("select dd.dataset_name from mna.dataset_details dd where dd.engagement_id={0}", EngagementId);

                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                OdbcCommand Engagementcommand = new OdbcCommand(EngagementQuery, connection);
                OdbcCommand datasetCommand = new OdbcCommand(DatasetQuery, connection);
                connection.Open();
                try
                {
                   
                    List<UserModelApprover> PPMDApprovers = new List<UserModelApprover>();
                    List<UserModelApprover> EngagementMembers = new List<UserModelApprover>();
                    List<string> Datasets = new List<string>();
                    List<UserModel> ClientTeam = new List<UserModel>();
                    OdbcDataReader reader = command.ExecuteReader();
                    OdbcDataReader Engagementreader = Engagementcommand.ExecuteReader();
                    OdbcDataReader datasetreader = datasetCommand.ExecuteReader();
                    DataTable dt = new DataTable();
                    DataTable EngDT = new DataTable();
                    DataTable dsDT = new DataTable();
                    EngDT.Load(Engagementreader);
                    dt.Load(reader);
                    dsDT.Load(datasetreader);
                  
                    
                    foreach (DataRow dr in dt.Rows)
                    {
                        UserModel model = new UserModel();
                        UserModelApprover modelApprover = new UserModelApprover();
                        if (dr["Role_name"].ToString()== "PPMD Approver")
                        {
                            //modelApprover.UserId = Convert.ToInt32(dr["Id"]);
                            modelApprover.givenName = dr["First_Name"].ToString();
                            modelApprover.displayName = dr["Last_Name"].ToString();
                            modelApprover.id = dr["EmailID"].ToString();
                            modelApprover.readPermission = Convert.ToBoolean(dr["Can_Write"])?false:true;
                            modelApprover.writePermission = Convert.ToBoolean(dr["Can_Write"]);
                            PPMDApprovers.Add(modelApprover);
                        }
                        else if (dr["Role_name"].ToString() == "Engagement Member")
                        {
                            //modelApprover.UserId = Convert.ToInt32(dr["Id"]);
                            modelApprover.givenName = dr["First_Name"].ToString();
                            modelApprover.displayName = dr["Last_Name"].ToString();
                            modelApprover.id = dr["EmailID"].ToString();
                            modelApprover.readPermission = Convert.ToBoolean(dr["Can_Write"]) ? false : true;
                            modelApprover.writePermission = Convert.ToBoolean(dr["Can_Write"]);
                            EngagementMembers.Add(modelApprover);
                        }
                        else if (dr["Role_name"].ToString() == "Client Team")
                        {
                            model.UserId = Convert.ToInt32(dr["Id"]);
                            model.FirstName = dr["First_Name"].ToString();
                            model.LastName = dr["Last_Name"].ToString();
                            model.Email = dr["EmailID"].ToString();
                            model.canWrite = Convert.ToBoolean(dr["Can_Write"]);
                            ClientTeam.Add(model);
                        }
                    }
                    foreach (DataRow dr in dsDT.Rows)
                    {
                        filename = dr["dataset_name"].ToString();
                        Datasets.Add(filename);
                    }
                    foreach (DataRow dr in EngDT.Rows)
                    {
                        engagementData.id = EngagementId;
                        engagementData.createdDate = Convert.ToDateTime(dr["CreatedDate"]);
                        engagementData.submittedBy = dr["CreatedUser"].ToString();
                        engagementData.engagementName = dr["Engagement_Name"].ToString();
                        engagementData.offeringPortfolio = Convert.ToInt32(dr["Offering_Portfolio_Id"]);
                        engagementData.wbsCode = dr["WBS_Code"].ToString();
                        engagementData.adlsFolderUrl = dr["folder_url"].ToString();
                        engagementData.adlsFolderName = engagementData.engagementName + "-" + engagementData.wbsCode;
                        engagementData.engagementisBuySide = Convert.ToBoolean(dr["Is_Engagement_Buy_Side"]);
                        engagementData.currentStatus = Convert.ToInt32(dr["Current_state"]);
                        engagementData.previousStatus = Convert.ToInt32(dr["Previous_state"]);
                        engagementData.userhasWritePermission = true;
                        engagementData.tableauURL = "";
                        engagementData.dataSets = Datasets;
                        engagementData.ppmdApprovers = PPMDApprovers;
                        engagementData.engagementTeams = EngagementMembers;
                        engagementData.clientTeams = ClientTeam;

                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                }
                return engagementData;
            }
        }

        public List<DashboardData> GetAllEngagements(string EmailId)
        {
            List<DashboardData> DashboardData = new List<DashboardData>();
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = "select distinct ED.id,ED.Engagement_Name,OP.Name,ED.WBS_Code, ES.State_Name,'' as ADLS_Folder,ED.Is_Engagement_Buy_Side" +
                            " from mna.engagement_details ED inner join mna.offering_portfolio OP on ED.Offering_Portfolio_Id = OP.Id" +
                            " inner join mna.engagement_state ES on ED.Current_state = ES.Id" +
                            " inner join mna.user_roles UR on ur.Engagement_Id = ED.Id" +
                            " inner join mna.users u on U.Id = UR.UserId" +
                            " where u.EmailID = '" + EmailId + "'" +
                            " and UR.Role_Id in (1, 2)" +
                            " order by ED.ID";
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {


                    OdbcDataReader reader = command.ExecuteReader();
                    DataTable dt = new DataTable();
                    dt.Load(reader);

                    foreach (DataRow dr in dt.Rows)
                    {
                        DashboardData dashboardData = new DashboardData();

                        dashboardData.EngagementID = Convert.ToInt32(dr["id"]);
                        dashboardData.EngagementName = dr["Engagement_Name"].ToString();
                        dashboardData.Offering_Portfolio = dr["Name"].ToString();
                        dashboardData.wbscode = dr["WBS_Code"].ToString();
                        dashboardData.Engagement_Status = dr["State_Name"].ToString();
                        dashboardData.ADLS_Folder = dr["ADLS_Folder"].ToString();
                        dashboardData.Engagement_Buy_Side = Convert.ToBoolean(dr["Is_Engagement_Buy_Side"]);
                        dashboardData.statuschangeduration = 0;
                        DashboardData.Add(dashboardData);

                    }

                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);

                }
                return DashboardData;
            }
        }

        public bool AddComments(AddComment addComment)
        {

            bool status = false;
            string connectionString = GetODBCConnection();

            string resp = "";
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = string.Format("insert into mna.Return_Rework_Comments(Engagement_Id,Comment) values({0},'{1}')", addComment.Engagement_Id, addComment.Comment);
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    command.ExecuteNonQuery();
                    status = true;

                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                    resp = ex.Message;
                }
            }
            return status;
        }

        public void AddDatasets(List<string> FilesList,int EngagementId,string username)
        {
            string connectionString = GetODBCConnection();
            string ADLSId_query = "";
            string Dataset_query = "";
            string InsertQuery = "";
            string UpdateQuery = "";
            string filename = "";
            List<string> SavedDataset = new List<string>();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                ADLSId_query = string.Format("select ad.Id from mna.adls_details ad where ad.engagement_id={0}",EngagementId);
                
                OdbcCommand ADLS_command = new OdbcCommand(ADLSId_query, connection);
                connection.Open();
                try
                {
                    OdbcDataReader reader=ADLS_command.ExecuteReader();
                    DataTable dataTable=new DataTable();
                    dataTable.Load(reader);
                    DataRow drow=dataTable.Rows[0];
                    int ADLS_Id = Convert.ToInt32(drow[0]);

                    Dataset_query = string.Format("select dd.dataset_name from mna.dataset_details dd where dd.engagement_id={0}", EngagementId);
                    OdbcCommand dataset_command = new OdbcCommand(Dataset_query, connection);
                    OdbcDataReader datasetreader = dataset_command.ExecuteReader();
                    DataTable dddataTable = new DataTable();
                    dddataTable.Load(datasetreader);
                    foreach(DataRow dr in dddataTable.Rows)
                    {
                        filename = dr["dataset_name"].ToString();
                        SavedDataset.Add(filename);
                    }
                    var firstNotSecond = FilesList.Except(SavedDataset).ToList();
                    var secondNotFirst = SavedDataset.Except(FilesList).ToList();

                    
                    foreach(string file in firstNotSecond)
                    {
                        InsertQuery = String.Format("insert into mna.dataset_details(Engagement_Id," +
                                "ADLS_Id," +
                                "Dataset_Name," +
                                "IsActive," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate" +
                                ") values({0},{1},'{2}',{3},'{4}',{5},'{6}',{7})", EngagementId, ADLS_Id, file, true,username, "getdate()", username, "getdate()");
                        OdbcCommand insertCommand = new OdbcCommand(InsertQuery, connection);
                        insertCommand.ExecuteNonQuery();

                    }
                    foreach (string file in secondNotFirst)
                    {
                        UpdateQuery = String.Format("update mna.dataset_details set IsActive=false where EngagementId={0} and ADLS_Id={1} and Dataset_Name='{2}'",
                            EngagementId, ADLS_Id, file);
                        OdbcCommand updateCommand = new OdbcCommand(UpdateQuery, connection);
                        updateCommand.ExecuteNonQuery();

                    }

                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                }
            }
        }
        public string CreateSchema(string schemaname)
        {
            int records = 0;
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = "CREATE SCHEMA IF NOT EXISTS " + schemaname;
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    records = command.ExecuteNonQuery();

                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                }
            }
            return schemaname;

        }
        public void LogError(string errormessage)
        {
            string connectionString = GetODBCConnection();

            string resp = "";
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = string.Format("insert into mna.errorlog(Error,CreatedDate) values('{0}',{1})", errormessage, "getdate()");
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    command.ExecuteNonQuery();
                    

                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                    resp = ex.Message;
                }
            }
            
        }

        public void AddJobInfo(int EngagementId,string filename,string jobid,string jobrunid,string number_in_job,string status)
        {
            string connectionString = GetODBCConnection();

            string resp = "";
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = string.Format("insert into mna.jobinfo(EngagementId,Filename,JobId,JobrunId,number_in_job,Status,CreatedDate) values({0},'{1}','{2}','{3}','{4}','{5}',{6})", EngagementId,filename,jobid,jobrunid,number_in_job,status, "getdate()");
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    command.ExecuteNonQuery();


                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                    resp = ex.Message;
                }
            }

        }
    }
}
