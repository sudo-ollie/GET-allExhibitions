using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using Newtonsoft.Json;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace GET_allExhibitions
{
    public class Function
    {
        public async Task<APIGatewayHttpApiV2ProxyResponse> FunctionHandler(APIGatewayHttpApiV2ProxyRequest request, ILambdaContext context)
        {
            var log = context.Logger;
            log.LogInformation($"Request received: {JsonConvert.SerializeObject(request, Formatting.Indented)}");

            try
            {
                log.LogInformation($"Trying Scan");
                List<Dictionary<string, AttributeValue>> results;
                try
                {
                    results = await ScanTable();
                    log.LogInformation($"Scan Finished - Results Found : {results.Count}");

                    if (results != null)
                    {
                        List<object> transformedResults = new List<object>();

                        foreach (var result in results)
                        {
                            List<object> exhibitContent = new List<object>();

                            //  Safely access 'ExhibitContent' & check that its type list + not null
                            if (result.TryGetValue("ExhibitContent", out AttributeValue exhibitContentValue) && exhibitContentValue.L != null)
                            {
                                foreach (var content in exhibitContentValue.L)
                                {
                                    if (content.M != null)
                                    {
                                        exhibitContent.Add(new
                                        {
                                            //  Ternary for each object field - safely accessing it > if true assign the value / if false assign default null value
                                            //  content.Map.safeAccess > save to variable ? access value : set to default value
                                            CreationDate = content.M.TryGetValue("CreationDate", out var creationDate) ? creationDate.N : null,
                                            ItemClassification = content.M.TryGetValue("ItemClassification", out var itemClassification) ? itemClassification.S : null,
                                            ItemObjectLink = content.M.TryGetValue("ItemObjectLink", out var itemObjectLink) ? itemObjectLink.S : null,
                                            ItemDepartment = content.M.TryGetValue("ItemDepartment", out var itemDepartment) ? itemDepartment.S : null,
                                            ItemTitle = content.M.TryGetValue("ItemTitle", out var itemTitle) ? itemTitle.S : null,
                                            ArtistBirthplace = content.M.TryGetValue("ArtistBirthplace", out var artistBirthplace) ? artistBirthplace.S : null,
                                            ArtistName = content.M.TryGetValue("ArtistName", out var artistName) ? artistName.S : null,
                                            ItemTechnique = content.M.TryGetValue("ItemTechnique", out var itemTechnique) ? itemTechnique.S : null,
                                            ItemCentury = content.M.TryGetValue("ItemCentury", out var itemCentury) ? itemCentury.S : null,
                                            ItemCreditline = content.M.TryGetValue("ItemCreditline", out var itemCreditline) ? itemCreditline.S : null,
                                            ItemID = content.M.TryGetValue("ItemID", out var itemID) ? itemID.N : null
                                        });
                                    }
                                }
                            }

                            transformedResults.Add(new
                            {
                                //  Same as above but for the 'outer' object
                                ExhibitionID = result.TryGetValue("ExhibitionID", out var exhibitionID) ? exhibitionID.N : null,
                                ExhibitionName = result.TryGetValue("ExhibitionName", out var exhibitionName) ? exhibitionName.S : null,
                                ExhibitionLength = result.TryGetValue("ExhibitionLength", out var exhibitionLength) ? exhibitionLength.N : null,
                                ExhibitionImage = result.TryGetValue("ExhibitionImage", out var exhibitionImage) ? exhibitionImage.S : null,
                                ExhibitionPublic = result.TryGetValue("ExhibitionPublic", out var exhibitionPublic) ? exhibitionPublic.N ?? exhibitionPublic.S : null,
                                ExhibitContent = exhibitContent
                            });
                        }
                        //  Exit Path - Items Found
                        log.LogInformation($"Query completed. Items Found : {JsonConvert.SerializeObject(transformedResults)}");
                        return new APIGatewayHttpApiV2ProxyResponse
                        {
                            StatusCode = 200,
                            Body = JsonConvert.SerializeObject(new { exhibitions = transformedResults })
                        };
                    }
                        //  Exit Path - No Items Found
                        log.LogInformation("Query Completed - No Items Found.");
                        return new APIGatewayHttpApiV2ProxyResponse
                        {
                            StatusCode = 200,
                            Body = JsonConvert.SerializeObject(new { exhibitions = new List<object>() })
                        };
                }
                //  Exit Path - Scan Error
                catch (Exception ex)
                {
                    log.LogError($"Scan Failed - Error : {ex.Message}");
                    return new APIGatewayHttpApiV2ProxyResponse
                    {
                        StatusCode = 500,
                        Body = JsonConvert.SerializeObject(new { message = "A Scan Error Occurred : ", error = ex.Message })
                    };
                }

            }
            //  Base Error Path
            catch (Exception ex)
            {
                log.LogError($"An Error Occurred : {ex.Message}");
                return new APIGatewayHttpApiV2ProxyResponse
                {
                    StatusCode = 500,
                    Body = JsonConvert.SerializeObject(new { message = "An Error Occurred : ", error = ex.Message })
                };
            }
        }

        //  DB Scan Function
        private async Task<List<Dictionary<string, AttributeValue>>> ScanTable()
        {
            var dynamoDbClient = new AmazonDynamoDBClient();

            try
            {
                var scanRequest = new ScanRequest
                {
                    TableName = "PublicExhibitions"
                };

                var scanResponse = await dynamoDbClient.ScanAsync(scanRequest);
                return scanResponse.Items;
            }
            catch (Exception ex)
            {
                throw new Exception($"An Error Occurred Scanning The DB : {ex.Message}", ex);
            }
        }
    }
}