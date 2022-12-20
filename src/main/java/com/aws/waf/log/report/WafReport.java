package com.aws.waf.log.report;

import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;

//import org.apache.poi.xs

import java.util.List;

public class WafReport {

    public static String TABLE_NAME = "default.waf_logs";
    public static String OUTPUT_BUCKET = "s3://harvest-waf-log-report/output";
    public static final long SLEEP_AMOUNT_IN_MS = 1000;

    XSSFWorkbook workbook = new XSSFWorkbook();
    XSSFSheet requestSheet = workbook.getSheetAt(0);
    XSSFSheet ruleSheet = workbook.getSheetAt(1);
    XSSFSheet ipSheet = workbook.getSheetAt(2);

    AthenaClient athenaClient;

    public void makeReport(String year){

        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
                "AKIA57WB3Q6KX5ZD5GGZ",
                "K9yCYmLxtuMK7JgGBlFSSylO8VTItq2HYwNGEn84");
        athenaClient = AthenaClient.builder()
                .region(Region.CN_NORTH_1)
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();
        try{
            System.out.println(makeYearReport(year));
        }catch (InterruptedException e){
            e.printStackTrace();
        }

        athenaClient.close();

    }

    public String makeYearReport(String year) throws InterruptedException {
        String startDate = year + "-01-01";
        String endDate = year + "-12-31";
        String sql = "select count(*) from " + TABLE_NAME + " where (date(from_unixtime(timestamp/1000)) between date \'" +
                startDate + "\' and date \'" + endDate + "\')";
        String queryExecutionId = submitAthenaQuery(sql);
        waitForQueryToComplete(queryExecutionId);
        processResultRows(queryExecutionId);

        for (int i=1; i<13; i++){
            startDate = year + "-" + StringUtil.leftFill(String.valueOf(i), "0", 2) + "-01";
            if (i==1 || i==3 || i==5 || i==7 || i==8 || i==10 || i== 12){
                endDate = "31";
            }else if (i==2){
                endDate = "28";
            }else if (i==4 || i==6 || i==9 || i==11){
                endDate = "30";
            }
            endDate = year + "-" + StringUtil.leftFill(String.valueOf(i), "0", 2) + "-" + endDate;

            sql = "select count(*) from " + TABLE_NAME + " where (date(from_unixtime(timestamp/1000)) between date \'" +
                    startDate + "\' and date \'" + endDate + "\')";
            System.out.println(i + ":" + sql);
            queryExecutionId = submitAthenaQuery(sql);
            waitForQueryToComplete(queryExecutionId);
            processResultRows(queryExecutionId);

            requestSheet.getRow(1).getCell(1).setCellValue(1);

        }

        return "";
    }

    public String submitAthenaQuery(String sql) {
        try {
            // The QueryExecutionContext allows us to set the database.
            QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder()
                    .database(TABLE_NAME)
                    .build();

            // The result configuration specifies where the results of the query should go.
            ResultConfiguration resultConfiguration = ResultConfiguration.builder()
                    .outputLocation(OUTPUT_BUCKET)
                    .build();

            StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
                    .queryString(sql)
                    .queryExecutionContext(queryExecutionContext)
                    .resultConfiguration(resultConfiguration)
                    .build();

            StartQueryExecutionResponse startQueryExecutionResponse = athenaClient.startQueryExecution(startQueryExecutionRequest);
            return startQueryExecutionResponse.queryExecutionId();

        } catch (AthenaException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return "";
    }

    // Wait for an Amazon Athena query to complete, fail or to be cancelled.
    public void waitForQueryToComplete(String queryExecutionId) throws InterruptedException {
        GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
                .queryExecutionId(queryExecutionId)
                .build();

        GetQueryExecutionResponse getQueryExecutionResponse;
        boolean isQueryStillRunning = true;
        while (isQueryStillRunning) {
            getQueryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest);
            String queryState = getQueryExecutionResponse.queryExecution().status().state().toString();
            if (queryState.equals(QueryExecutionState.FAILED.toString())) {
                throw new RuntimeException("The Amazon Athena query failed to run with error message: " + getQueryExecutionResponse
                        .queryExecution().status().stateChangeReason());
            } else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
                throw new RuntimeException("The Amazon Athena query was cancelled.");
            } else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
                isQueryStillRunning = false;
            } else {
                // Sleep an amount of time before retrying again.
                Thread.sleep(SLEEP_AMOUNT_IN_MS);
            }
            System.out.println("The current status is: " + queryState);
        }
    }

    // This code retrieves the results of a query
    public void processResultRows(String queryExecutionId) {
        try {

            // Max Results can be set but if its not set,
            // it will choose the maximum page size.
            GetQueryResultsRequest getQueryResultsRequest = GetQueryResultsRequest.builder()
                    .queryExecutionId(queryExecutionId)
                    .build();

            GetQueryResultsIterable getQueryResultsResults = athenaClient.getQueryResultsPaginator(getQueryResultsRequest);
            for (GetQueryResultsResponse result : getQueryResultsResults) {
                List<ColumnInfo> columnInfoList = result.resultSet().resultSetMetadata().columnInfo();
                List<Row> results = result.resultSet().rows();
                processRow(results, columnInfoList);
            }

        } catch (AthenaException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void processRow(List<Row> row, List<ColumnInfo> columnInfoList) {
        for (Row myRow : row) {
            List<Datum> allData = myRow.data();
            for (Datum data : allData) {
                System.out.println("The value of the column is "+data.varCharValue());
            }
        }
    }

}
