import ballerina/http;
import ballerinax/googleapis.sheets as sheets;
import ballerina/lang.runtime;

const BASE_URL = "https://api.central.ballerina.io/2.0/registry/";
configurable string clientId = ? ;
configurable string clientSecret = ? ;
configurable string refreshUrl = sheets:REFRESH_URL ;
configurable string refreshToken = ? ;
configurable string spreadSheetID = ? ;
configurable string worksheetName = ? ;

public function main() returns error? {
    
    int offset = 0;
    int queryLimit = 10;

    sheets:ConnectionConfig spreadsheetConfig = {
        auth: {
            clientId,
            clientSecret,
            refreshUrl,
            refreshToken
        }
    };
    sheets:Client spreadsheetClient = check new (spreadsheetConfig);
    http:Client httpClient = check new (BASE_URL);

    boolean hasMoreValues = true;

    while hasMoreValues {
         hasMoreValues = check extractConnectorInfo(offset, queryLimit, httpClient, spreadsheetClient);
         offset = offset + queryLimit;
         runtime:sleep(5.0);
    }
}

function extractConnectorInfo(int offset, int queryLimit, http:Client httpClient, sheets:Client spreadsheetClient) returns boolean|error {
    json connectorResponse = check httpClient->get(string `connectors?offset=${offset}&limit=${queryLimit}&org=ballerinax`);
    json connectors = check connectorResponse.connectors;
    json[] connectorList = check connectors.ensureType(); 
        if connectorList.length() == 0 {
        return false;
    }
    foreach json connectorInfo in connectorList {
        string displayName = check connectorInfo.displayName;
        string moduleName = check connectorInfo.moduleName;
        json keywords = check connectorInfo.package.keywords;
        string keywordsAsString = keywords.toJsonString();
        int pullCount =  check connectorInfo.package.pullCount;
        string[] gSheetData = [displayName, moduleName, keywordsAsString, pullCount.toString()];
        check spreadsheetClient-> appendRowToSheet(spreadSheetID, worksheetName, gSheetData);
    }
    return true;
}
