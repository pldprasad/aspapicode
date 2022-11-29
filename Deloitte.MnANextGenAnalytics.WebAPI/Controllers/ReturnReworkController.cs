using Deloitte.MnANextGenAnalytics.WebAPI.DataModels;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace Deloitte.MnANextGenAnalytics.WebAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ReturnReworkController : ControllerBase
    {
        private static IConfiguration _config;
        public DataLayer.DataLayer dataLayer;
        private static ILogger _logger;
        public ReturnReworkController(IConfiguration config,ILogger<ReturnReworkController> logger)
        {
            _config = config;
            _logger = logger;
        }

        [HttpGet("{id}")]
        public IActionResult GetreturnForReworkDetails(int id)
        {
            dataLayer = new DataLayer.DataLayer(_config,_logger);
            EngagementData engagementData = dataLayer.GetEngagementData(id);             

            return Ok(engagementData);

        }

        [HttpPost("addComment")]
        public IActionResult addComment(AddComment addComment)
        {
            dataLayer = new DataLayer.DataLayer(_config,_logger);
            var status = dataLayer.AddComments(addComment);

            return Ok(status);

        }

    }
}
