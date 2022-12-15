using Deloitte.MnANextGenAnalytics.WebAPI.DataModels;
using ICSharpCode.SharpZipLib.Zip;
using Microsoft.AspNetCore.Mvc;
using MigraDoc.DocumentObjectModel;
using MigraDoc.DocumentObjectModel.Tables;
using MigraDoc.Rendering;
using PdfSharp.Drawing;
using PdfSharp.Pdf;

namespace Deloitte.MnANextGenAnalytics.WebAPI.Controllers
{
    [CustomAuthorization]
    [Route("api/[controller]")]
    [ApiController]
    public class CloseEngagementController : ControllerBase
    {
        private static IConfiguration _config;
        public DataLayer.DataLayer dataLayer;
        private static ILogger _logger;
   
        public CloseEngagementController(IConfiguration config, ILogger<CloseEngagementController> logger)
        {
            _config = config;
            _logger = logger;
            dataLayer = new DataLayer.DataLayer(config, logger);
        }

        [HttpGet("DownloadZIP")]
        public FileResult GeneratePDF(int EngagementId)
        {

            EngagementData engagementData = dataLayer.GetEngagementData(EngagementId);
            List<Audit> auditDetails = dataLayer.GetAuditDetails(EngagementId);
            var fileName = engagementData.engagementName + ".zip";

            PdfDocument pdf = new PdfDocument();

            PdfPage pdfPage = pdf.AddPage();
            XRect rect = new XRect(0, 0, 250, 140);
            XGraphics graph = XGraphics.FromPdfPage(pdfPage);

            System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

            XFont font = new XFont("Verdana", 20, XFontStyle.Regular);

            int yPoint = 0;


            Document doc = new Document();
            Section section = doc.AddSection();

            // Create the item table
            Table table = section.AddTable();
            table.Borders.Color = Colors.Gray;
            table.Borders.Width = 0.2;
            table.Borders.Left.Width = 0.5;
            table.Borders.Right.Width = 0.5;
            table.Rows.LeftIndent = 0;

            // Before you can add a row, you must define the columns
            Column column = table.AddColumn("4cm");
            column.Format.Alignment = ParagraphAlignment.Center;

            column = table.AddColumn("4cm");
            column.Format.Alignment = ParagraphAlignment.Right;

            column = table.AddColumn("4cm");
            column.Format.Alignment = ParagraphAlignment.Center;

            column = table.AddColumn("4cm");
            column.Format.Alignment = ParagraphAlignment.Right;


            //Row-2
            Row row = table.AddRow();
            row.Cells[0].AddParagraph("Engagement Details");
            row.Cells[0].MergeRight = 3;
            row.Cells[0].Format.Alignment = ParagraphAlignment.Center;
            row.Format.Font.Bold = true;
            row.HeadingFormat = true;
            //Row-1
            row = table.AddRow();
            row.HeadingFormat = true;
            row.Format.Alignment = ParagraphAlignment.Center;
            row.Format.Font.Bold = false;
            row.Shading.Color = Colors.LightGray;
            row.Cells[0].AddParagraph("Created");
            row.Cells[0].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[1].AddParagraph("Closed");
            row.Cells[1].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[2].AddParagraph("Submitted");
            row.Cells[2].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[3].AddParagraph("Status");
            row.Cells[3].Format.Alignment = ParagraphAlignment.Left;
            

            row = table.AddRow();
            row.HeadingFormat = true;
            row.Format.Alignment = ParagraphAlignment.Center;
            row.Format.Font.Bold = false;
            row.Cells[0].AddParagraph(Convert.ToString(engagementData.createdDate));
            row.Cells[0].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[1].AddParagraph(engagementData.submittedBy);
            row.Cells[1].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[2].AddParagraph(Convert.ToString(engagementData.modifiedDate));
            row.Cells[2].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[3].AddParagraph(engagementData.stateName);
            row.Cells[3].Format.Alignment = ParagraphAlignment.Left;
          
            //Row-1
            //XPen lineRed = new XPen(XColors.Gray, 1);
            //graph.DrawLine(lineRed, 0, pdfPage.Height / 2, pdfPage.Width, pdfPage.Height / 2);
            row = table.AddRow();
            row.Cells[0].MergeRight = 3;
            //Row-2
            row = table.AddRow();
            row.HeadingFormat = true;
            row.Format.Alignment = ParagraphAlignment.Center;
            row.Format.Font.Bold = false;
            row.Shading.Color = Colors.LightGray;
            row.Cells[0].AddParagraph("Engagement Name");
            row.Cells[0].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[1].AddParagraph("Offering Portfolio");
            row.Cells[1].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[2].AddParagraph("WBS Code");
            row.Cells[2].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[3].AddParagraph("Engagement is Buy-side");
            row.Cells[3].Format.Alignment = ParagraphAlignment.Left;

            row = table.AddRow();
            row.HeadingFormat = true;
            row.Format.Alignment = ParagraphAlignment.Center;
            row.Format.Font.Bold = false;
            row.Cells[0].AddParagraph(engagementData.engagementName);
            row.Cells[0].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[1].AddParagraph(Convert.ToString(engagementData.offeringPortfolioName));
            row.Cells[1].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[2].AddParagraph(engagementData.wbsCode);
            row.Cells[2].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[3].AddParagraph(Convert.ToString(engagementData.engagementisBuySide));
            row.Cells[3].Format.Alignment = ParagraphAlignment.Left;
            //Row-2
            row = table.AddRow();
            row.Cells[0].MergeRight = 3;

            //Row-1
            row = table.AddRow();
            row.HeadingFormat = true;
            row.Format.Alignment = ParagraphAlignment.Center;
            row.Format.Font.Bold = false;
            row.Shading.Color = Colors.LightGray;
            row.Cells[0].AddParagraph("ADLS Folder URL");
            row.Cells[0].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[1].AddParagraph("Data Set(s)");
            row.Cells[1].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[1].MergeRight = 2;

            row = table.AddRow();
            row.HeadingFormat = true;
            row.Format.Alignment = ParagraphAlignment.Center;
            row.Format.Font.Bold = false;
            row.Cells[0].AddParagraph(Convert.ToString(engagementData.adlsFolderName));
            row.Cells[0].Format.Alignment = ParagraphAlignment.Left;

            foreach (var item in engagementData.dataSets.Select((value, i) => new { i, value }))
            {
                var value = item.value;
                var index = item.i;
                row.Cells[1].AddParagraph(index + 1 + " ) " + item.value.dataset_name);
            }

            row.Cells[1].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[1].MergeRight = 2;
            //Row-1
            row = table.AddRow();
            row.Cells[0].MergeRight = 3;

            //Row-1
            row = table.AddRow();
            row.HeadingFormat = true;
            row.Format.Alignment = ParagraphAlignment.Center;
            row.Format.Font.Bold = false;
            row.Shading.Color = Colors.LightGray;
            row.Cells[0].AddParagraph("PPMD Approver(s)");
            row.Cells[0].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[1].AddParagraph("No Data Access PPMD(s)");
            row.Cells[1].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[2].AddParagraph("Data Access PPMD(s)");
            row.Cells[2].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[2].MergeRight = 1;

            row = table.AddRow();
            row.HeadingFormat = true;
            row.Format.Alignment = ParagraphAlignment.Center;
            row.Format.Font.Bold = false;
            foreach (var item in engagementData.ppmdApprovers.Select((value, i) => new { i, value }))
            {
                var value = item.value;
                var index = item.i;
                row.Cells[0].AddParagraph(index + 1 + " ) " + item.value.displayName);
            }

            row.Cells[0].Format.Alignment = ParagraphAlignment.Left;

            foreach (var item in engagementData.ppmdApprovers.Select((value, i) => new { i, value }))
            {
                var value = item.value;
                var index = item.i;
                row.Cells[1].AddParagraph(index + 1 + " ) " + item.value.displayName);
            }
            row.Cells[1].Format.Alignment = ParagraphAlignment.Left;

            foreach (var item in engagementData.ppmdApprovers.Select((value, i) => new { i, value }))
            {
                var value = item.value;
                var index = item.i;
                row.Cells[2].AddParagraph(index + 1 + " ) " + item.value.displayName);
            }
            row.Cells[2].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[2].MergeRight = 1;
            //Row-1
            row = table.AddRow();
            row.Cells[0].MergeRight = 3;


            //Row-1
            row = table.AddRow();
            row.HeadingFormat = true;
            row.Format.Alignment = ParagraphAlignment.Center;
            row.Format.Font.Bold = false;
            row.Shading.Color = Colors.LightGray;
            row.Cells[0].AddParagraph("Team Member(s)");
            row.Cells[0].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[1].AddParagraph("No Data Access Member(s)");
            row.Cells[1].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[2].AddParagraph("Data Access Member(s)");
            row.Cells[2].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[2].MergeRight = 1;

            row = table.AddRow();
            row.HeadingFormat = true;
            row.Format.Alignment = ParagraphAlignment.Center;
            row.Format.Font.Bold = false;

            foreach (var item in engagementData.engagementTeams.Select((value, i) => new { i, value }))
            {
                var value = item.value;
                var index = item.i;
                row.Cells[0].AddParagraph(index + 1 + " ) " + item.value.displayName);
            }
            row.Cells[0].Format.Alignment = ParagraphAlignment.Left;

            foreach (var item in engagementData.engagementTeams.Select((value, i) => new { i, value }))
            {
                var value = item.value;
                var index = item.i;
                row.Cells[1].AddParagraph(index + 1 + " ) " + item.value.displayName);
            }
            row.Cells[1].Format.Alignment = ParagraphAlignment.Left;

            foreach (var item in engagementData.engagementTeams.Select((value, i) => new { i, value }))
            {
                var value = item.value;
                var index = item.i;
                row.Cells[2].AddParagraph(index + 1 + " ) " + item.value.displayName);
            }
            row.Cells[2].Format.Alignment = ParagraphAlignment.Left;
            row.Cells[2].MergeRight = 1;
            //Row-1
            row = table.AddRow();
            row.Cells[0].MergeRight = 3;

            if(engagementData.clientTeams.Count > 0)
            {
                //Row-2
                row = table.AddRow();
                row.HeadingFormat = true;
                row.Format.Alignment = ParagraphAlignment.Center;
                row.Format.Font.Bold = false;
                row.Shading.Color = Colors.LightGray;
                row.Cells[0].AddParagraph("Added Client(s):");
                row.Cells[0].Format.Alignment = ParagraphAlignment.Left;
                row.Cells[0].MergeRight = 3;

                row = table.AddRow();
                row.HeadingFormat = true;
                row.Format.Alignment = ParagraphAlignment.Center;
                row.Format.Font.Bold = false;
                foreach (var item in engagementData.clientTeams.Select((value, i) => new { i, value }))
                {
                    var value = item.value;
                    var index = item.i;

                    row.Cells[0].AddParagraph(index + 1 + " ) " + item.value.LastName + "," + item.value.FirstName);
                    row.Cells[0].Format.Alignment = ParagraphAlignment.Left;
                }
                row.Cells[0].MergeRight = 3;
            }
            

            //Row-1
            row = table.AddRow();
            row.Cells[0].MergeRight = 3;
            // Audit log

            if(auditDetails.Count > 0)
            {
                //Row-2
                row = table.AddRow();
                row.Cells[0].AddParagraph("Audit Details");
                row.Cells[0].MergeRight = 3;
                row.Cells[0].Format.Alignment = ParagraphAlignment.Center;
                row.Format.Font.Bold = true;
                row.HeadingFormat = true;

                foreach (var item in auditDetails.Select((value, i) => new { i, value }))
                {
                    var value = item.value;
                    var index = item.i;

                    //Row-2
                    row = table.AddRow();
                    row.HeadingFormat = true;
                    row.Format.Alignment = ParagraphAlignment.Center;
                    row.Format.Font.Bold = false;
                    row.Shading.Color = Colors.LightGray;
                    row.Cells[0].AddParagraph("Audit Type");
                    row.Cells[0].Format.Alignment = ParagraphAlignment.Left;
                    row.Cells[1].AddParagraph("Old Value");
                    row.Cells[1].Format.Alignment = ParagraphAlignment.Left;
                    row.Cells[2].AddParagraph("New Value");
                    row.Cells[2].Format.Alignment = ParagraphAlignment.Left;
                    row.Cells[3].AddParagraph("CreatedUser");
                    row.Cells[3].Format.Alignment = ParagraphAlignment.Left;


                    row = table.AddRow();
                    row.HeadingFormat = true;
                    row.Format.Alignment = ParagraphAlignment.Center;
                    row.Format.Font.Bold = false;
                    row.Cells[0].AddParagraph(item.value.auditType);
                    row.Cells[0].Format.Alignment = ParagraphAlignment.Left;
                    row.Cells[1].AddParagraph(item.value.oldValue);
                    row.Cells[1].Format.Alignment = ParagraphAlignment.Left;
                    row.Cells[2].AddParagraph(item.value.newValue);
                    row.Cells[2].Format.Alignment = ParagraphAlignment.Left;
                    row.Cells[3].AddParagraph(Convert.ToString(item.value.createdDate));
                    row.Cells[3].Format.Alignment = ParagraphAlignment.Left;

                    //Row-2
                    row = table.AddRow();
                    row.HeadingFormat = true;
                    row.Format.Alignment = ParagraphAlignment.Center;
                    row.Format.Font.Bold = false;
                    row.Shading.Color = Colors.LightGray;
                    row.Cells[0].AddParagraph("ModifiedUser");
                    row.Cells[0].Format.Alignment = ParagraphAlignment.Left;
                    row.Cells[1].AddParagraph("Modified Date");
                    row.Cells[1].Format.Alignment = ParagraphAlignment.Left;
                    row.Cells[2].AddParagraph("Status");
                    row.Cells[2].Format.Alignment = ParagraphAlignment.Left;
                    row.Cells[2].MergeRight = 1;

                    row = table.AddRow();
                    row.HeadingFormat = true;
                    row.Format.Alignment = ParagraphAlignment.Center;
                    row.Format.Font.Bold = false;
                    row.Cells[0].AddParagraph(item.value.modifiedUser);
                    row.Cells[0].Format.Alignment = ParagraphAlignment.Left;
                    row.Cells[1].AddParagraph(Convert.ToString(item.value.modifiedDate));
                    row.Cells[1].Format.Alignment = ParagraphAlignment.Left;
                    row.Cells[2].AddParagraph(Convert.ToString(item.value.isActive));
                    row.Cells[2].Format.Alignment = ParagraphAlignment.Left;
                    row.Cells[2].MergeRight = 1;
                    //Row-2
                    row = table.AddRow();
                    row.Cells[0].MergeRight = 3;
                }
            }
            else
            {
                row = table.AddRow();
                row.Cells[0].AddParagraph("No audit log found");
                row.Cells[0].MergeRight = 3;
                row.Cells[0].Format.Alignment = ParagraphAlignment.Center;
                row.Format.Font.Bold = false;
                row.HeadingFormat = true;
            }
                    
            table.SetEdge(0, 0, 4, 2, Edge.Box, BorderStyle.Single, 0.75, Color.Empty);

            // Create a renderer and prepare (=layout) the document
            MigraDoc.Rendering.DocumentRenderer docRenderer = new DocumentRenderer(doc);
            docRenderer.PrepareDocument();

            // Render the paragraph. You can render tables or shapes the same way.
            docRenderer.RenderObject(graph, XUnit.FromCentimeter(3), XUnit.FromCentimeter(3), "40cm", table);

            string pdfFilename = engagementData.engagementName + ".pdf";

            pdf.Save(pdfFilename);

            var fileResult = DownLoadZip(pdfFilename);
            return File(fileResult, "application/zip", fileName);

        }
        private byte[] DownLoadZip(string pdfFilename)
        {

            var tempOutput = @"MyZip.zip";

            using (ZipOutputStream IzipOutputStream = new ZipOutputStream(System.IO.File.Create(tempOutput)))
            {
                IzipOutputStream.SetLevel(9);
                byte[] buffer = new byte[6000];
                var imageList = new List<string>();

                imageList.Add(pdfFilename);

                for (int i = 0; i < imageList.Count; i++)
                {
                    ZipEntry entry = new ZipEntry(Path.GetFileName(imageList[i]));
                    entry.DateTime = DateTime.Now;
                    entry.IsUnicodeText = true;
                    IzipOutputStream.PutNextEntry(entry);

                    using (FileStream oFileStream = System.IO.File.OpenRead(imageList[i]))
                    {
                        int sourceBytes;
                        do
                        {
                            sourceBytes = oFileStream.Read(buffer, 0, buffer.Length);
                            IzipOutputStream.Write(buffer, 0, sourceBytes);
                        } while (sourceBytes > 0);
                    }
                }
                IzipOutputStream.Finish();
                IzipOutputStream.Flush();
                IzipOutputStream.Close();
            }

            byte[] finalResult = System.IO.File.ReadAllBytes(tempOutput);
            if (System.IO.File.Exists(tempOutput))
            {
                System.IO.File.Delete(tempOutput);
            }
            if (System.IO.File.Exists(pdfFilename))
            {
                System.IO.File.Delete(pdfFilename);
            }
            if (finalResult == null || !finalResult.Any())
            {
                throw new Exception(String.Format("Nothing found"));

            }
            return finalResult;
        }

    }
}
