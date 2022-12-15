using Microsoft.AspNetCore.Http;
using Azure.Storage.Blobs;
using Microsoft.AspNetCore.Mvc;
using System.Net.Mime;
using System.Text;
using Newtonsoft.Json;
using Deloitte.MnANextGenAnalytics.WebAPI.DataModels;

namespace Deloitte.MnANextGenAnalytics.WebAPI.Controllers
{
    [CustomAuthorization]
    [Route("api/[controller]")]
    [ApiController]
    
    public class DatabricksController : ControllerBase
    {
        private static IConfiguration _config;
        public DataLayer.DataLayer dataLayer;
        private static ILogger _logger;
        public DatabricksController(IConfiguration config, ILogger<DatabricksController> logger)
        {
            _config = config;
            _logger = logger;
            dataLayer = new DataLayer.DataLayer(_config,_logger);

        }
        [HttpPost("CreateTable")]
        public async Task<string> CreateTable(string EngagementName, string foldername, string FileName,int EngagementId)
        {
            string runid = "";
            try
            {
                string formattedEngagement = RemoveSpecialCharacters(EngagementName);
                string schemaname = dataLayer.CreateSchema(formattedEngagement);
                string tablename=createNotebook(foldername, FileName, schemaname);
                string JobResponse = await CreateJob(schemaname+"-"+FileName);
                string RunJobResponse=await RunJob(JobResponse);
                var JobResponseJson = JsonConvert.DeserializeObject<Job>(JobResponse);
                var RunJobResponseJson = JsonConvert.DeserializeObject<Job_Run>(RunJobResponse);
                dataLayer.AddJobInfo(EngagementId, tablename, JobResponseJson.job_id, RunJobResponseJson.run_id, RunJobResponseJson.number_in_job, "Initiated");
               
                return RunJobResponseJson.run_id;
            }
            catch (Exception ex)
            {
                _logger.LogError("CreateTable : " + ex.Message);
                return runid;
            }

        }
        [HttpPost("UnionTable")]
        public async Task<string> UnionTable(int EngagementId,string basefile,string engagementname, string foldername, FilesList[] filesList)
        {
            string runid = "";
            try
            {

                string[] unionfiles = new string[filesList.Length];
                int i = 0;
                foreach (FilesList file in filesList)
                {
                    unionfiles[i] = file.dataset_name.ToString();
                    i++;
                }
                string formattedEngagement = RemoveSpecialCharacters(engagementname);
                string tablename = dataLayer.GetTablename(EngagementId, basefile);
                tablename = CreateUnionTableWorkbook(basefile, unionfiles, engagementname, tablename, foldername,EngagementId);
                string FileName = System.IO.Path.GetFileNameWithoutExtension(basefile) + "union";
                string JobResponse = await CreateJob(formattedEngagement+"-"+FileName);
                string RunJobResponse = await RunJob(JobResponse);
                var JobResponseJson = JsonConvert.DeserializeObject<Job>(JobResponse);
                var RunJobResponseJson = JsonConvert.DeserializeObject<Job_Run>(RunJobResponse);
                dataLayer.AddJobInfo(EngagementId, tablename, JobResponseJson.job_id, RunJobResponseJson.run_id, RunJobResponseJson.number_in_job, "Initiated");

                return RunJobResponseJson.run_id;
            }
            catch (Exception ex)
            {
                _logger.LogError("UnionTable : " + ex.Message);
                return runid;
            }

        }

        [HttpPost("JoinTable")]
        public async Task<string> JoinTable(int EngagementId, string basefile, string engagementname, string foldername,string destinationfile, string sourcecolumn,string destinationcolumn)
        {
            string runid = "";
            try
            {
                string formattedEngagement = RemoveSpecialCharacters(engagementname);
                string tablename = dataLayer.GetTablename(EngagementId, basefile);
                tablename = CreateJoinTableWorkbook(EngagementId, basefile, destinationfile, engagementname, tablename, foldername,sourcecolumn,destinationcolumn);
                string FileName = System.IO.Path.GetFileNameWithoutExtension(basefile) + "Join";
                string JobResponse = await CreateJob(formattedEngagement+"-"+FileName);
                string RunJobResponse = await RunJob(JobResponse);
                var JobResponseJson = JsonConvert.DeserializeObject<Job>(JobResponse);
                var RunJobResponseJson = JsonConvert.DeserializeObject<Job_Run>(RunJobResponse);
                dataLayer.AddJobInfo(EngagementId, tablename, JobResponseJson.job_id, RunJobResponseJson.run_id, RunJobResponseJson.number_in_job, "Initiated");

                return RunJobResponseJson.run_id;
            }
            catch (Exception ex)
            {
                _logger.LogError("UnionTable : " + ex.Message);
                return runid;
            }

        }

        [HttpPost("PurgeTable")]
        public async Task<string> PurgeTable(int EngagementId, string basefile, string engagementname, string foldername, string destinationfile)
        {
            string runid = "";
            try
            {
                string formattedEngagement = RemoveSpecialCharacters(engagementname);
                string tablename = dataLayer.GetTablename(EngagementId, basefile);
                tablename = CreatePurgeTableWorkbook(basefile, destinationfile, engagementname, tablename, foldername);
                string FileName = System.IO.Path.GetFileNameWithoutExtension(basefile) + "Purge";
                string JobResponse = await CreateJob(formattedEngagement+"-"+FileName);
                string RunJobResponse = await RunJob(JobResponse);
                var JobResponseJson = JsonConvert.DeserializeObject<Job>(JobResponse);
                var RunJobResponseJson = JsonConvert.DeserializeObject<Job_Run>(RunJobResponse);
                dataLayer.AddJobInfo(EngagementId, tablename, JobResponseJson.job_id, RunJobResponseJson.run_id, RunJobResponseJson.number_in_job, "Initiated");

                return RunJobResponseJson.run_id;
            }
            catch (Exception ex)
            {
                _logger.LogError("PurgeTable : " + ex.Message);
                return runid;
            }

        }

        [HttpPost("CreateCache")]
        public string CreateCache(string fileName, string engagementName,int engagementId)
        {
            string status = "Failed";
            try
            {
                string trim = RemoveSpecialCharacters(Path.GetFileNameWithoutExtension(fileName).Replace(" ", ""));
                string formattedEngagement = RemoveSpecialCharacters(engagementName);
                string tablename= formattedEngagement + "." + "Tbl" + trim;
                
                dataLayer.UpdateDatasetDetails(engagementId, fileName, tablename);
                string serviceaccount = dataLayer.GetServiceAccount(engagementId);
                dataLayer.SetSchemaPermissions(formattedEngagement, serviceaccount);
                dataLayer.SetTablePermissions(tablename, serviceaccount);
                dataLayer.UpdateServiceAccountDetails(engagementId, serviceaccount);
                status = dataLayer.CreateCache(tablename);
                status = "{\"value\":\"" + status + "\"}";
                return status;
            }
            catch(Exception ex)
            {
                _logger.LogError(ex.Message);
                status = "{\"value\":\"" + status + "\"}";
                return status;
            }
        }
        [HttpGet("GetJobStatus")]
        public async Task<string> GetJobStatus(string runid)
        {
            string resp = "";
            try
            {
                var BaseAPI = _config.GetValue<string>("DatabricksAPI:DatabricksBaseAPI");
                var Jobstatus = _config.GetValue<string>("DatabricksAPI:JobStatus");
                var accessToken = _config.GetValue<string>("DatabricksAPI:AccessToken");

               
                using (var httpClient = new HttpClient())
                {
                    httpClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken);
                    using (var response = await httpClient.GetAsync(BaseAPI + Jobstatus+ "?run_id="+runid))
                    {
                        string apiResponse = await response.Content.ReadAsStringAsync();
                        resp = apiResponse;
                    }
                }
                return resp;
            }
            catch (Exception ex)
            {
                _logger.LogError("GetJobStatus : " + ex.Message);
                return resp;
            }

        }

        [HttpGet("GetHeader")]
        public List<string> GetHeader(string foldername, string FileName)
        {
            List<string> Header_Data = new List<string>();
            try
            {

                string ConnectionString = _config.GetValue<string>("Blobstorage:ConnectionString");
                string containername = _config.GetValue<string>("Blobstorage:containername");
                string filepath = foldername + "\\" + FileName;
                BlobServiceClient blobServiceClient = new BlobServiceClient(ConnectionString);
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containername);
                BlobClient blob = containerClient.GetBlobClient(filepath);
                Stream blobstream = blob.OpenRead();
                StreamReader blobStreamReader = new StreamReader(blobstream);
                string header = blobStreamReader.ReadLine();
                string delimitter = GetDelimitter(header);
                string[] columns= header.Split(delimitter);
                Header_Data=columns.ToList();
                return Header_Data;
            }
            catch (Exception ex)
            {
                _logger.LogError("GetHeaderData : " + ex.Message);
                return Header_Data;
            }
        }
       
        [HttpPost("ValidateUnion")]
        public bool ValidateUnion(string foldername, string basefile, FilesList[] filesList)
        {
            List<string> Header_Data = new List<string>();
            List<string> exceptlist=new List<string>();
            List<string> baseheader = new List<string>();
            List<List<string>> headers = new List<List<string>>();
            string[] filenames=new string[filesList.Length];
            int i = 0;
            foreach (FilesList file in filesList)
            {
                filenames[i] = file.dataset_name.ToString();
                i++;
            }
            try
            {

                string ConnectionString = _config.GetValue<string>("Blobstorage:ConnectionString");
                string containername = _config.GetValue<string>("Blobstorage:containername");
                BlobServiceClient blobServiceClient = new BlobServiceClient(ConnectionString);
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containername);

                {
                    string filepath = foldername + "\\" + basefile;
                    BlobClient blob = containerClient.GetBlobClient(filepath);
                    Stream blobstream = blob.OpenRead();
                    StreamReader blobStreamReader = new StreamReader(blobstream);
                    string header = blobStreamReader.ReadLine();
                    string delimitter = GetDelimitter(header);
                    string[] columns = header.Split(delimitter);
                    baseheader = columns.ToList();
                    
                }


                foreach (string FileName in filenames)
                {
                    string filepath = foldername + "\\" + FileName;
                    
                    BlobClient blob = containerClient.GetBlobClient(filepath);
                    Stream blobstream = blob.OpenRead();
                    StreamReader blobStreamReader = new StreamReader(blobstream);
                    string header = blobStreamReader.ReadLine();
                    string delimitter = GetDelimitter(header);
                    string[] columns = header.Split(delimitter);
                    Header_Data = columns.ToList();
                    headers.Add(Header_Data);
                }

                foreach(List<string> Header in headers)
                {
                    exceptlist=baseheader.Except(Header).ToList();
                    if(exceptlist.Count > 0)
                    {
                        return false;
                    }
                }
                return true;
                
            }
            catch (Exception ex)
            {
                _logger.LogError("GetHeaderData : " + ex.Message);
                return false;
            }
        }

        [HttpPost("ValidatePurge")]
        public bool ValidatePurge(string foldername, string basefile, string destinationfile)
        {
            List<string> Header_Data = new List<string>();
            List<string> exceptlist = new List<string>();
            List<string> baseheader = new List<string>();
           
            try
            {

                string ConnectionString = _config.GetValue<string>("Blobstorage:ConnectionString");
                string containername = _config.GetValue<string>("Blobstorage:containername");
                BlobServiceClient blobServiceClient = new BlobServiceClient(ConnectionString);
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containername);

                {
                    string filepath = foldername + "\\" + basefile;
                    BlobClient blob = containerClient.GetBlobClient(filepath);
                    Stream blobstream = blob.OpenRead();
                    StreamReader blobStreamReader = new StreamReader(blobstream);
                    string header = blobStreamReader.ReadLine();
                    string delimitter = GetDelimitter(header);
                    string[] columns = header.Split(delimitter);
                    baseheader = columns.ToList();

                }


                
                {
                    string filepath = foldername + "\\" + destinationfile;

                    BlobClient blob = containerClient.GetBlobClient(filepath);
                    Stream blobstream = blob.OpenRead();
                    StreamReader blobStreamReader = new StreamReader(blobstream);
                    string header = blobStreamReader.ReadLine();
                    string delimitter = GetDelimitter(header);
                    string[] columns = header.Split(delimitter);
                    Header_Data = columns.ToList();
                    
                }

               
                    exceptlist = baseheader.Except(Header_Data).ToList();
                    if (exceptlist.Count > 0)
                    {
                        return false;
                    }
                
                return true;

            }
            catch (Exception ex)
            {
                _logger.LogError("GetHeaderData : " + ex.Message);
                return false;
            }
        }

       

        [HttpDelete("DropTable")]
        public async Task<string> DropTable(int EngagementId, string basefile)
        {
            string resp = "";
            try
            {
                string tablename = dataLayer.GetTablename(EngagementId, basefile);
                resp=dataLayer.DropTable(tablename);
                dataLayer.DeleteTableRecord(tablename);
                resp = "{\"value\":\"" + resp + "\"}";
                return resp;
            }
            catch(Exception ex)
            {
                _logger.LogError("DropTable : " + ex.Message);
                resp = "failure";
                resp = "{\"value\":\"" + resp + "\"}";
                return resp;
            }
            
        }

        [HttpDelete("DeleteFolder")]
        public async Task<string> DeleteFolder(string basefile, string foldername)
        {
            string resp = "";
            try
            {
                string ConnectionString = _config.GetValue<string>("Blobstorage:ConnectionString");
                string containername = _config.GetValue<string>("Blobstorage:containername");
                string Filepath = foldername + "\\" + basefile;
                string storageaccount = _config.GetValue<string>("Blobstorage:StorageAccount");
                string AccountKey = _config.GetValue<string>("Blobstorage:AccountKey");
                BlobServiceClient blobServiceClient = new BlobServiceClient(ConnectionString);
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containername);
                BlobClient blobClient = containerClient.GetBlobClient(Filepath);
                bool success=blobClient.DeleteIfExists();
                resp=success.ToString();
                resp = "{\"value\":\"" + resp + "\"}";
                return resp;
            }
            catch (Exception ex)
            {
                _logger.LogError("DeleteFolder : " + ex.Message);
                resp = "failure";
                resp = "{\"value\":\"" + resp + "\"}";
                return resp;
            }
            
        }

        [HttpDelete("CloseoutEngagement")]
        public async Task<string> CloseoutEngagement(int EngagementId, string engagementname)
        {
            string resp = "";
            try
            {
                string result = DeleteFolder(EngagementId);
                string formattedEngagement = RemoveSpecialCharacters(engagementname);
                resp = dataLayer.DropSchema(formattedEngagement);
                resp = dataLayer.UpdateSGGroupDetails(EngagementId);
                resp = dataLayer.UpdateServiceAccountDetails(EngagementId);

                resp = result.ToString();
                resp = "{\"value\":\"" + resp + "\"}";
                return resp;
            }
            catch (Exception ex)
            {
                _logger.LogError("CloseoutEngagement : " + ex.Message);
                resp = "failure";
                resp = "{\"value\":\"" + resp + "\"}";
                return resp;
            }

        }

        [HttpPost("AddDatasetLog")]
        public async void AddDatasetLog(int EngagementId,string filename,string message,string username)
        {
            try
            {
                dataLayer.AddDatasetLog(EngagementId, filename, message, username);
            }
            catch (Exception ex)
            {
                _logger.LogError("Dataset Activitylog " + ex.Message);
                
            }
        }

        [HttpGet("GetDatasetLog")]
        public List<DatasetActivity> GetDatasetLog(int EngagementId, string filename)
        {
            try
            {
                List<DatasetActivity> activities=dataLayer.GetDatasetLog(EngagementId, filename);
                return activities;
            }
            catch (Exception ex)
            {
                _logger.LogError("Dataset Activitylog " + ex.Message);
                return null;

            }
        }


        [NonAction]
        public string createNotebook(string foldername, string FileName, string schemaname)
        {
            string tablename = "";
            try
            {
                string notebookData = "";
                string trim = RemoveSpecialCharacters(Path.GetFileNameWithoutExtension(FileName).Replace(" ", ""));
                string mountpoint = _config.GetValue<string>("DatabricksConnection:Mountpoint");
                string externalTable = _config.GetValue<string>("DatabricksAPI:ExternalTablesPath");
                List<string> Header_Data = GetHeaderData(foldername, FileName);
                string delimitter = GetDelimitter(Header_Data.FirstOrDefault());

                notebookData = notebookData + "from pyspark.sql.functions import expr,col,to_date" + "\n\n";
                notebookData = notebookData + "spark.conf.set(\"spark.databricks.delta.formatCheck.enabled\", False)" + "\n\n";
                notebookData = notebookData + "file_location = \"" + mountpoint + foldername + "/" + FileName + "\"" + "\n\n";
                notebookData = notebookData + "df = spark.read.format(\"csv\").option(\"inferSchema\", \"true\").option(\"header\",\"true\")" +
                                            ".option(\"delimiter\", \"" + delimitter + "\").load(file_location)" + "\n\n";

                string query = PrepareQuery(Header_Data, delimitter,"df","df1");
                notebookData = notebookData + query + "\n\n";
                string ExternalTable = "df1.write.format(\"parquet\").mode(\"overwrite\").option(\"path\",\"" + externalTable + schemaname + "-" + "Tbl" + trim + "\").saveAsTable(\"" + schemaname + "." + "Tbl" + trim + "\")";
                //string ExternalTable = "df1.write.format(\"parquet\").mode(\"overwrite\").saveAsTable(\"" + schemaname + "." + "Tbl" + trim + "\")";
                notebookData = notebookData + ExternalTable;
                string workbookname = createWorkBook(notebookData, FileName);
                string encoded = GetEncodedText(workbookname);
                UploadWorkbook(encoded, FileName, schemaname);
                tablename = schemaname + "." + "Tbl" + trim;
                return tablename;
            }
            catch (Exception ex)
            {
                _logger.LogError("createNotebook : " + ex.Message);
                return tablename;
            }

        }
        [NonAction]
        public string DeleteFolder(int EngagementId)
        {
            try
            {
                
                string ConnectionString = _config.GetValue<string>("Blobstorage:ConnectionString");
                string containername = _config.GetValue<string>("Blobstorage:containername");
                string Filepath = dataLayer.GetFoldername(EngagementId);
                string storageaccount = _config.GetValue<string>("Blobstorage:StorageAccount");
                string AccountKey = _config.GetValue<string>("Blobstorage:AccountKey");
                BlobServiceClient blobServiceClient = new BlobServiceClient(ConnectionString);
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containername);
                BlobClient blobClient = containerClient.GetBlobClient(Filepath);
                List<string> files = Getfiles(Filepath);
                foreach(string file in files)
                {
                    DeleteFolder(file, Filepath);
                }
                bool success = blobClient.DeleteIfExists();
                return success.ToString();
            }
            catch(Exception ex)
            {
                _logger.LogError("DeleteFolder : " + ex.Message);
                return "failure";
            }
        }
        [NonAction]
        public List<string> Getfiles(string foldername)
        {
            string Foldername = foldername;
            string ConnectionString = _config.GetValue<string>("Blobstorage:ConnectionString");
            string containername = _config.GetValue<string>("Blobstorage:containername");
            BlobServiceClient blobServiceClient = new BlobServiceClient(ConnectionString);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containername);
            var blobs = containerClient.GetBlobs();
           List<string> files= blobs.Where(x => x.Name.Contains(Foldername + "/")).Select(x => x.Name.Replace(Foldername + "/", "")).ToList();
            return files;
        }

        [NonAction]
        public List<string> GetHeaderData(string foldername, string FileName)
        {
            List<string> Header_Data = new List<string>();
            try
            {

                string ConnectionString = _config.GetValue<string>("Blobstorage:ConnectionString");
                string containername = _config.GetValue<string>("Blobstorage:containername");
                string filepath = foldername + "\\" + FileName;
                BlobServiceClient blobServiceClient = new BlobServiceClient(ConnectionString);
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containername);
                BlobClient blob = containerClient.GetBlobClient(filepath);
                Stream blobstream = blob.OpenRead();
                StreamReader blobStreamReader = new StreamReader(blobstream);
                string header = blobStreamReader.ReadLine();
                string data = blobStreamReader.ReadLine();
                Header_Data.Add(header);
                Header_Data.Add(data);
                return Header_Data;
            }
            catch (Exception ex)
            {
                _logger.LogError("GetHeaderData : " + ex.Message);
                return Header_Data;
            }
        }

        [NonAction]
        public async void UploadWorkbook(string bytestream, string Filename,string Engagementname)
        {
            try
            {
                var BaseAPI = _config.GetValue<string>("DatabricksAPI:DatabricksBaseAPI");
                var WorkbookUpload = _config.GetValue<string>("DatabricksAPI:Workbookupload");
                var accessToken = _config.GetValue<string>("DatabricksAPI:AccessToken");
                var workbookpath = _config.GetValue<string>("DatabricksAPI:Workbookpath");
                using (var httpClient = new HttpClient())
                {
                    var content = new MultipartFormDataContent();
                    content.Add(new StringContent(workbookpath + Engagementname +"-"+ System.IO.Path.GetFileNameWithoutExtension(Filename) + ".py"), "path");
                    content.Add(new StringContent("SOURCE"), "format");
                    content.Add(new StringContent("PYTHON"), "language");
                    content.Add(new StringContent(bytestream), "content");
                    content.Add(new StringContent("true"), "overwrite");

                    httpClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken);
                    using (var response = await httpClient.PostAsync(BaseAPI + WorkbookUpload, content))
                    {
                        string apiResponse = await response.Content.ReadAsStringAsync();

                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("UploadWorkbook : " + ex.Message);
            }
        }
       
        [NonAction]
        public async Task<string> RunJob(string jobidjson)
        {
            try
            {
                var BaseAPI = _config.GetValue<string>("DatabricksAPI:DatabricksBaseAPI");
                var RunJob = _config.GetValue<string>("DatabricksAPI:RunJob");
                var accessToken = _config.GetValue<string>("DatabricksAPI:AccessToken");

                string body = jobidjson;
                string resp = "";
                using (var httpClient = new HttpClient())
                {
                    httpClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken);
                    var request = new HttpRequestMessage
                    {
                        Method = HttpMethod.Post,
                        RequestUri = new Uri(BaseAPI + RunJob),
                        Content = new StringContent(body, Encoding.UTF8, MediaTypeNames.Application.Json),

                    };
                    using (var response = await httpClient.SendAsync(request))
                    {
                        string apiResponse = await response.Content.ReadAsStringAsync();
                        resp = apiResponse;
                    }
                }
                return resp;
            }
            catch (Exception ex)
            {
                _logger.LogError("RunJob : " + ex.Message);
                return string.Empty;
            }
        }
       
        [NonAction]
        public async Task<string> CreateJob(string filename)
        {
            string resp = "";
            try
            {
                var BaseAPI = _config.GetValue<string>("DatabricksAPI:DatabricksBaseAPI");
                var CreateJob = _config.GetValue<string>("DatabricksAPI:CreateJob");
                var accessToken = _config.GetValue<string>("DatabricksAPI:AccessToken");
                var workbookpath = _config.GetValue<string>("DatabricksAPI:Workbookpath");

                string body = "{" +
                                "\"name\": \"Job for " + filename + "\"," +
                                "\"tags\": {" +
                                "\"cost-center\": \"API\"," +
                                "\"team\": \"Adhoc jobs\"" +
                                "}," +
                                "\"tasks\": [" +
                                "{" +
                                "\"task_key\": \"job_for_list_path\"," +
                                "\"notebook_task\": {" +
                                "\"notebook_path\": \""+workbookpath+ System.IO.Path.GetFileNameWithoutExtension(filename) + ".py" + "\"," +
                                "\"source\": \"WORKSPACE\"" +
                                "}," +
                                "\"job_cluster_key\": \"job_for_list_path_cluster\"," +
                                "\"timeout_seconds\": 0," +
                                "\"email_notifications\": { }" +
                                "}" +
                                "]," +
                                "\"job_clusters\": [" +
                                "{" +
                                "\"job_cluster_key\": \"job_for_list_path_cluster\"," +
                                "\"new_cluster\": {" +
                                "\"spark_version\": \"10.4.x-scala2.12\"," +
                                "\"spark_conf\": {" +
                                "\"spark.databricks.delta.preview.enabled\": \"true\"" +
                                "}," +
                                "\"azure_attributes\": {" +
                                "\"first_on_demand\": 1," +
                                "\"availability\": \"ON_DEMAND_AZURE\"," +
                                "\"spot_bid_max_price\": -1" +
                                "}," +
                                "\"node_type_id\": \"Standard_DS3_v2\"," +
                                "\"spark_env_vars\": {" +
                                "\"PYSPARK_PYTHON\": \"/databricks/python3/bin/python3\"" +
                                "}," +
                                "\"enable_elastic_disk\": true," +
                                "\"data_security_mode\": \"LEGACY_SINGLE_USER_STANDARD\"," +
                                "\"runtime_engine\": \"STANDARD\"," +
                                "\"num_workers\": 8" +
                                "}" +
                                "}" +
                                "]," +
                                "\"format\": \"MULTI_TASK\"" +
                                "}";


                using (var httpClient = new HttpClient())
                {
                    httpClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + accessToken);
                    var request = new HttpRequestMessage
                    {
                        Method = HttpMethod.Post,
                        RequestUri = new Uri(BaseAPI + CreateJob),
                        Content = new StringContent(body, Encoding.UTF8, MediaTypeNames.Application.Json),

                    };
                    using (var response = await httpClient.SendAsync(request))
                    {
                        string apiResponse = await response.Content.ReadAsStringAsync();
                        resp = apiResponse;
                    }
                }
                return resp;
            }
            catch (Exception ex)
            {
                _logger.LogError("CreateJob : " + ex.Message);
                return resp;
            }
        }
       
        [NonAction]
        public string GetDelimitter(string header)
        {
            try
            {
                string delimitter = "";
                if (header.Contains('|'))
                {
                    delimitter = "|";
                }
                else if (header.Contains(','))
                {
                    delimitter = ",";
                }
                else if (header.Contains(';'))
                {
                    delimitter = ";";
                }
                return delimitter;
            }
            catch (Exception ex)
            {
                _logger.LogError("GetDelimitter : " + ex.Message);
                return "|";
            }
        }

        [NonAction]
        public string PrepareQuery(List<string> Data, string Delimitter,string sourceDF,string destinationDF)
        {
            try
            {
                string query = string.Empty;
                string header = Data[0];
                string tabledata = Data[1];
                string columns = "";

                string[] columnNames = header.Split(Delimitter);
                string[] columnValues = tabledata.Split(Delimitter);
                string[] datatype = new string[columnValues.Length];

                int counter = 0;
                foreach (string value in columnValues)
                {
                    datatype[counter] = ParseString(value);
                    counter++;
                }
                query = query +destinationDF +"="+sourceDF+".select(";
                for (int i = 0; i < columnNames.Length; i++)
                {
                    if (datatype[i] != "DateTime")
                    {
                        columns = columns + "col(\"" + columnNames[i] + "\")";
                    }
                    else
                    {
                        columns = columns + "to_date(col(\"" + columnNames[i] + "\"),\"M/dd/yyyy\").alias(\"" + columnNames[i] + "\")";
                    }
                    if (i < columnNames.Length - 1)
                    {
                        columns = columns + ",";
                    }
                }
                query = query + columns + ")";


                return query;
            }
            catch (Exception ex)
            {
                _logger.LogError("PrepareQuery : " + ex.Message);
                return string.Empty;
            }
        }

        [NonAction]
        public string createWorkBook(string text, string Filename)
        {
            try
            {
                string workbookname = Filename;
                var plainTextBytes = System.Text.Encoding.UTF8.GetBytes(text);
                var logWriter = new System.IO.StreamWriter(workbookname);
                logWriter.BaseStream.Write(plainTextBytes);
                logWriter.Dispose();
                return workbookname;
            }
            catch (Exception ex)
            {
                _logger.LogError("createWorkBook : " + ex.Message);
                return string.Empty;
            }

        }
       
        [NonAction]
        public string GetEncodedText(string notebookname)
        {
            try
            {
                string text = System.IO.File.ReadAllText(notebookname);
                var plainTextBytes = System.Text.Encoding.UTF8.GetBytes(text);
                string encoded = System.Convert.ToBase64String(plainTextBytes);
                return encoded;
            }
            catch (Exception ex)
            {
                _logger.LogError("GetEncodedText : " + ex.Message);
                return string.Empty;
            }
        }
       
        [NonAction]
        private string ParseString(string str)
        {
            try
            {
                bool boolValue;
                Int32 intValue;
                Int64 bigintValue;
                double doubleValue;
                DateTime dateValue;

                // Place checks higher in if-else statement to give higher priority to type.

                if (bool.TryParse(str, out boolValue))
                    return "bool";
                else if (Int32.TryParse(str, out intValue))
                    return "int";
                else if (Int64.TryParse(str, out bigintValue))
                    return "int";
                else if (double.TryParse(str, out doubleValue))
                    return "double";
                else if (DateTime.TryParse(str, out dateValue))
                    return "DateTime";
                else return "string";
            }
            catch (Exception ex)
            {
                _logger.LogError("ParseString : " + ex.Message);
                return string.Empty;
            }

        }
       
        [NonAction]
        public static string RemoveSpecialCharacters(string str)
        {
            StringBuilder sb = new StringBuilder();
            foreach (char c in str)
            {
                if ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_')
                {
                    sb.Append(c);
                }
            }
            return sb.ToString();
        }

        [NonAction]

        public string CreateUnionTableWorkbook(string basefile, string[] filesList, string Engagementname, string tablename, string foldername,int EngagementId)
        {

            try
            {
                int i = 0, j = 0, q = 0, r = 0, l = 0, m = 0;
                int p = filesList.Length;
                List<string> Header_Data = new List<string>();
                string delimitter = "";
                List<string> table_df = new List<string>();
                List<string> Dest_Tables = new List<string>();
                List<string> file_location = new List<string>();
                List<string> data_frames = new List<string>();
                List<string> destination_dataframes = new List<string>();
                List<string> files_list= new List<string>();
                for (int k = 0; k < filesList.Length; k++)
                {
                    string desttablename = dataLayer.GetTablename(EngagementId, filesList[k]);
                    if (desttablename != "")
                    {
                        Dest_Tables.Add(desttablename);
                        table_df.Add("table_df" + r);
                        r++;
                    }
                    else
                    {
                        file_location.Add("file_location_" + l);
                        data_frames.Add("df" + l);
                        destination_dataframes.Add("dest_df" + l);
                        files_list.Add(filesList[k]);
                        l++;
                    }

                }


                string[] filelocation = file_location.ToArray();
                string[] dataframes = data_frames.ToArray();
                string[] destinationdataframes = destination_dataframes.ToArray();
                string[] tabledf = table_df.ToArray();
                string[] filesArray=files_list.ToArray();
                string destination_concat = "";
                string tables_concat = "";


                string notebookData = "";
                string mountpoint = _config.GetValue<string>("DatabricksConnection:Mountpoint");
                string externalTable = _config.GetValue<string>("DatabricksAPI:ExternalTablesPath");
                notebookData = notebookData + "%python" + "\n";
                notebookData = notebookData + "from functools import reduce" + "\n";
                notebookData = notebookData + "from pyspark.sql import DataFrame" + "\n";
                notebookData = notebookData + "from pyspark.sql.types import ArrayType,IntegerType,DateType,BooleanType" + "\n\n";
                notebookData = notebookData + "from pyspark.sql.functions import expr,col,to_date" + "\n\n";
                notebookData = notebookData + "spark.conf.set(\"spark.databricks.delta.formatCheck.enabled\", False)" + "\n\n";

                notebookData = notebookData + "spark.sql(\"REFRESH TABLE " + tablename + "\")" + "\n\n";

                notebookData = notebookData + "source_df=spark.read.table(\"" + tablename + "\")" + "\n\n";


                foreach (string table_name in Dest_Tables)
                {
                    notebookData = notebookData + tabledf[m] + " =spark.read.table(\"" + table_name + "\")" + "\n\n";
                    tables_concat = tables_concat + tabledf[m] + ",";
                    m++;
                }

                foreach (string file in filesArray)
                {
                    notebookData = notebookData + filelocation[i] + " = \"" + mountpoint + foldername + "/" + file + "\"" + "\n\n";
                    i++;
                }


                foreach (string file in filesArray)
                {
                    Header_Data = GetHeaderData(foldername, file);
                    delimitter = GetDelimitter(Header_Data.FirstOrDefault());
                    notebookData = notebookData + dataframes[j] + " = spark.read.format(\"csv\").option(\"inferSchema\", \"true\").option(\"header\",\"true\")" +
                                             ".option(\"delimiter\", \"" + delimitter + "\").load(" + filelocation[j] + ")" + "\n\n";
                    j++;
                }


                foreach (string file in filesArray)
                {
                    Header_Data = GetHeaderData(foldername, file);
                    delimitter = GetDelimitter(Header_Data.FirstOrDefault());
                    string query = PrepareQuery(Header_Data, delimitter, dataframes[q], destinationdataframes[q]);
                    notebookData = notebookData + query + "\n\n";
                    destination_concat = destination_concat + destinationdataframes[q] + ",";
                    q++;
                }

                if (tables_concat.Length > 0)
                {
                    tables_concat = ","+tables_concat.Remove(tables_concat.Length - 1, 1);
                }
                if (destination_concat.Length > 0)
                {
                    destination_concat = "," + destination_concat.Remove(destination_concat.Length - 1, 1);
                }
                string uniondf = tables_concat+destination_concat;
                

                notebookData = notebookData + "dfs= [source_df" + uniondf + "]" + "\n";
                notebookData = notebookData + "dffinal = reduce(DataFrame.unionByName, dfs)"+"\n";

                notebookData = notebookData + "spark.sql(\"DROP TABLE IF EXISTS " + tablename + "\")" + "\n\n";


                string schemaname = tablename.Replace('.', '-');
                string ExternalTable = "dffinal.write.format(\"parquet\").mode(\"overwrite\").option(\"path\",\"" + externalTable + schemaname + "\").saveAsTable(\"" + tablename + "\")";
                //string ExternalTable = "dffinal.write.format(\"parquet\").mode(\"overwrite\").saveAsTable(\"" + tablename + "\")";
                notebookData = notebookData + ExternalTable;
                string workbookname = createWorkBook(notebookData, basefile);
                string encoded = GetEncodedText(workbookname);
                string Filename = System.IO.Path.GetFileNameWithoutExtension(basefile) + "union";
                schemaname = tablename.Split('.')[0].ToString();
                UploadWorkbook(encoded, Filename, schemaname);
                return tablename;
            }
            catch (Exception ex)
            {
                _logger.LogError("createNotebook : " + ex.Message);
                return tablename;
            }
        }

        [NonAction]

        public string CreateJoinTableWorkbook(int EngagementId,string basefile, string destinationfile, string Engagementname, string tablename, string foldername,string sourcecolumn,string destinationcolumn)
        {

            try
            {
               
                string notebookData = "";
                string desttablename = dataLayer.GetTablename(EngagementId, destinationfile);
                string mountpoint = _config.GetValue<string>("DatabricksConnection:Mountpoint");
                string externalTable = _config.GetValue<string>("DatabricksAPI:ExternalTablesPath");
                notebookData = notebookData + "%python" + "\n";
                notebookData = notebookData + "from pyspark.sql.types import ArrayType,IntegerType,DateType,BooleanType" + "\n\n";
                notebookData = notebookData + "from pyspark.sql.functions import expr,col,to_date" + "\n\n";
                notebookData = notebookData + "spark.conf.set(\"spark.databricks.delta.formatCheck.enabled\", False)" + "\n\n";

                

                notebookData = notebookData + "spark.sql(\"REFRESH TABLE " + tablename + "\")" + "\n\n";

                notebookData = notebookData + "source_df=spark.read.table(\"" + tablename + "\")"+"\n\n";

                if (desttablename != "")
                {
                    notebookData = notebookData + "dftemp=spark.read.table(\"" + desttablename + "\")" + "\n\n";
                }

                else
                {
                    notebookData = notebookData + "file_location" + " = \"" + mountpoint + foldername + "/" + destinationfile + "\"" + "\n\n";

                    List<string> Header_Data = GetHeaderData(foldername, destinationfile);
                    string delimitter = GetDelimitter(Header_Data.FirstOrDefault());
                    notebookData = notebookData + "df = spark.read.format(\"csv\").option(\"inferSchema\", \"true\").option(\"header\",\"true\")" +
                                             ".option(\"delimiter\", \"" + delimitter + "\").load(file_location)" + "\n\n";


                    Header_Data = GetHeaderData(foldername, destinationfile);
                    delimitter = GetDelimitter(Header_Data.FirstOrDefault());
                    string tempquery = PrepareQuery(Header_Data, delimitter, "df", "dftemp");
                    notebookData = notebookData + tempquery + "\n\n";
                }

                
                

                notebookData = notebookData + "dffinal = source_df.join(dftemp.withColumnRenamed('"+destinationcolumn+"','"+sourcecolumn+"'),['"+sourcecolumn+ "'])" + "\n\n";

                notebookData = notebookData + "spark.sql(\"DROP TABLE IF EXISTS "+tablename+"\")" + "\n\n";

                
                string schemaname = tablename.Replace('.', '-');
                string ExternalTable = "dffinal.write.format(\"parquet\").mode(\"overwrite\").option(\"path\",\"" + externalTable + schemaname + "\").saveAsTable(\"" + tablename + "\")";
                notebookData = notebookData + ExternalTable;
                string workbookname = createWorkBook(notebookData, basefile);
                string encoded = GetEncodedText(workbookname);
                string Filename = System.IO.Path.GetFileNameWithoutExtension(basefile) + "Join";
                schemaname = tablename.Split('.')[0].ToString();
                UploadWorkbook(encoded, Filename, schemaname);
                return tablename;
            }
            catch (Exception ex)
            {
                _logger.LogError("createNotebook : " + ex.Message);
                return tablename;
            }
        }

        [NonAction]

        public string CreatePurgeTableWorkbook(string basefile, string destinationfile, string Engagementname, string tablename, string foldername)
        {

            try
            {

                string notebookData = "";
                string mountpoint = _config.GetValue<string>("DatabricksConnection:Mountpoint");
                string externalTable = _config.GetValue<string>("DatabricksAPI:ExternalTablesPath");
                List<string> Header_Data = GetHeaderData(foldername, destinationfile);
                string delimitter = GetDelimitter(Header_Data.FirstOrDefault());

                notebookData = notebookData + "%python" + "\n";
                notebookData = notebookData + "from pyspark.sql.types import ArrayType,IntegerType,DateType,BooleanType" + "\n\n";
                notebookData = notebookData + "from pyspark.sql.functions import expr,col,to_date" + "\n\n";
                notebookData = notebookData + "spark.conf.set(\"spark.databricks.delta.formatCheck.enabled\", False)" + "\n\n";

                notebookData = notebookData + "file_location" + " = \"" + mountpoint + foldername + "/" + destinationfile + "\"" + "\n\n";

                //notebookData = notebookData + "spark.sql(\"DELETE FROM " + tablename + "\")" + "\n\n";

           
                
                notebookData = notebookData + "df = spark.read.format(\"csv\").option(\"inferSchema\", \"true\").option(\"header\",\"true\")" +
                                         ".option(\"delimiter\", \"" + delimitter + "\").load(file_location)" + "\n\n";


                Header_Data = GetHeaderData(foldername, destinationfile);
                delimitter = GetDelimitter(Header_Data.FirstOrDefault());
                string tempquery = PrepareQuery(Header_Data, delimitter, "df", "df1");
                notebookData = notebookData + tempquery + "\n\n";

                

                string schemaname = tablename.Replace('.', '-');
                string ExternalTable = "df1.write.format(\"parquet\").mode(\"overwrite\").option(\"path\",\"" + externalTable + schemaname + "\").saveAsTable(\"" + tablename + "\")";
                notebookData = notebookData + ExternalTable;
                string workbookname = createWorkBook(notebookData, basefile);
                string encoded = GetEncodedText(workbookname);
                string Filename = System.IO.Path.GetFileNameWithoutExtension(basefile) + "Purge";
                schemaname = tablename.Split('.')[0].ToString();
                UploadWorkbook(encoded, Filename, schemaname);
                return tablename;


            }
            catch (Exception ex)
            {
                _logger.LogError("createNotebook : " + ex.Message);
                return tablename;
            }
        }

    }
  
    
}
