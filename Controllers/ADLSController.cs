using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Deloitte.MnANextGenAnalytics.WebAPI.DataModels;
using Microsoft.AspNetCore.Mvc;


namespace Deloitte.MnANextGenAnalytics.WebAPI.Controllers
{ 
    public class ADLSController : ControllerBase
    {
        private static IConfiguration _config;
        public DataLayer.DataLayer dataLayer;
        private static ILogger _logger;

        public ADLSController(IConfiguration config,ILogger<ADLSController> logger)
        {
            logger.LogError("CreateFolder : 0");
            _config = config;
            _logger = logger;
            dataLayer = new DataLayer.DataLayer(_config,_logger);
        }
        
        public string CreateFolder(string EngagementName, string WBSCode,int? EngagementID)
        {
            try
            {
                _logger.LogError("CreateFolder : 1");
                string FolderURL = "";
                string ConnectionString = _config.GetValue<string>("Blobstorage:ConnectionString");
                string containername = _config.GetValue<string>("Blobstorage:containername");
                string localFilePath = "Dummy.txt";
                string Foldername = EngagementName + "-" + WBSCode;
                string Filepath = Foldername + "\\" + localFilePath;
                string storageaccount = _config.GetValue<string>("Blobstorage:StorageAccount");
                string AccountKey = _config.GetValue<string>("Blobstorage:AccountKey");
                _logger.LogError("CreateFolder : 2");
                createfile("", localFilePath);
                _logger.LogError("CreateFolder : 3");
                BlobServiceClient blobServiceClient = new BlobServiceClient(ConnectionString);
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containername);
                BlobClient blobClient = containerClient.GetBlobClient(Filepath);

                FileStream uploadFileStream = new FileStream(localFilePath, FileMode.Open);
                _logger.LogError("CreateFolder : 4");
                blobClient.Upload(uploadFileStream);
                _logger.LogError("CreateFolder : 5");

                uploadFileStream.Close();
                _logger.LogError("CreateFolder : 6");
                blobClient.DeleteIfExists();
                _logger.LogError("CreateFolder : 7");
                FolderURL = PopulateFolderURL(EngagementName, WBSCode);
                _logger.LogError("CreateFolder : 8");

                string groupname = SetPermissions(storageaccount, AccountKey, containername, Foldername);
                _logger.LogError("CreateFolder : 9");

                //Save the FolderURL in DB
                string FolderName = EngagementName + "-" + WBSCode;
                _logger.LogError("CreateFolder : 10");
                dataLayer.UpdateSGGroupDetails(EngagementID, groupname);
                _logger.LogError("CreateFolder : 11");
                dataLayer.SaveFolderDetails(FolderURL, EngagementID,FolderName);
                _logger.LogError("CreateFolder : 12");
                return FolderURL;
            }
            catch(Exception ex)
            {
                _logger.LogError("CreateFolder : 13");
                _logger.LogError("CreateFolder : " + ex.Message);
                dataLayer.LogError("CreateFolder : " + ex.Message);
                return string.Empty;
            }
        }

        
        public string SetPermissions(string storageaccount,string AccountKey,string containername,string foldername)
        {
            try
            {
                DataLakeServiceClient dataLakeServiceClient = GetDataLakeServiceClient(storageaccount, AccountKey);
                DataLakeFileSystemClient dataLakeFileSystemClient = GetFileSystem(dataLakeServiceClient, containername);
                ManageContainerACLs(dataLakeFileSystemClient);
                string groupname = ManageFolderACLs(dataLakeFileSystemClient, foldername);
                return groupname;
            }
            catch (Exception ex)
            {
                _logger.LogError("SetPermissions : " + ex.Message);
                dataLayer.LogError("SetPermissions : " + ex.Message);
                return string.Empty;
            }
        }
        public  string PopulateFolderURL(string EngagementName,string WBSCode)
        {
            try
            {
                string storageaccount = _config.GetValue<string>("Blobstorage:StorageAccount");
                string containername = _config.GetValue<string>("Blobstorage:containername");
                string FolderURL = "storageexplorer://?v=2&tenantId=36da45f1-dd2c-4d1f-af13-5abe46b99921&type=fileSystemPath&path=" + EngagementName + "-" + WBSCode + "%2F&container=" + containername + "&serviceEndpoint=https%3A%2F%2F" + storageaccount + ".dfs.core.windows.net%2F";
                return FolderURL;
            }
            catch (Exception ex)
            {
                _logger.LogError("PopulateFolderURL : " + ex.Message);
                dataLayer.LogError("PopulateFolderURL : " + ex.Message);
                return string.Empty;
            }

        }
        public static DataLakeServiceClient GetDataLakeServiceClient(string accountName, string accountKey)
        {
            try
            {
                StorageSharedKeyCredential sharedKeyCredential =
                    new StorageSharedKeyCredential(accountName, accountKey);

                string dfsUri = "https://" + accountName + ".dfs.core.windows.net";

                DataLakeServiceClient dataLakeServiceClient = new DataLakeServiceClient
                    (new Uri(dfsUri), sharedKeyCredential);
                return dataLakeServiceClient;
            }
            catch (Exception ex)
            {
                _logger.LogError("GetDataLakeServiceClient : " + ex.Message);
                return null;
            }
        }
        public static DataLakeFileSystemClient GetFileSystem(DataLakeServiceClient serviceClient, string FileSystemName)
        {
            try
            {
                return serviceClient.GetFileSystemClient(FileSystemName);
            }
            catch(Exception ex)
            {
                _logger.LogError("GetFileSystem : " + ex.Message);
                return null;
            }
        }
        public static string ManageContainerACLs(DataLakeFileSystemClient fileSystemClient)
        {
            try
            {
                DataLayer.DataLayer dataLayer = new DataLayer.DataLayer(_config,_logger);
                DataLakeDirectoryClient directoryClient =
                  fileSystemClient.GetDirectoryClient("");

                PathAccessControl directoryAccessControl = directoryClient.GetAccessControl();
                SGGroupDetails sgGroupDetails = dataLayer.GetSGGroup();
                string accessstring = "group:" + sgGroupDetails.GroupId + ":r-x,default:group:" + sgGroupDetails.GroupId + ":r-x";
                IList<PathAccessControlItem> accessControlList
             = PathAccessControlExtensions.ParseAccessControlList
             (accessstring);

                directoryClient.SetAccessControlList(accessControlList);

                return sgGroupDetails.GroupName;
            }
            catch (Exception ex)
            {
                _logger.LogError("ManageContainerACLs : " + ex.Message);
                return string.Empty;
            }
        }
        public static string ManageFolderACLs(DataLakeFileSystemClient fileSystemClient,string Foldername)
        {
            try
            {
                DataLayer.DataLayer dataLayer = new DataLayer.DataLayer(_config,_logger);
                DataLakeDirectoryClient directoryClient =
                  fileSystemClient.GetDirectoryClient(Foldername);

                PathAccessControl directoryAccessControl = directoryClient.GetAccessControl();
                SGGroupDetails sgGroupDetails = dataLayer.GetSGGroup();
                string accessstring = "group:" + sgGroupDetails.GroupId + ":rwx,default:group:" + sgGroupDetails.GroupId + ":rwx";
                IList<PathAccessControlItem> accessControlList
             = PathAccessControlExtensions.ParseAccessControlList
             (accessstring);

                directoryClient.SetAccessControlList(accessControlList);
                return sgGroupDetails.GroupName;
            }
            catch (Exception ex)
            {
                _logger.LogError("ManageFolderACLs : " + ex.Message);
                return string.Empty;
            }

        }
        public string createfile(string text, string Filename)
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
                _logger.LogError("createfile : " + ex.Message);
                return string.Empty;
            }

        }
    }
}
