package com.aws.waf.log.report;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WafReportApplication {

    public static void main(String[] args) {
        SpringApplication.run(WafReportApplication.class, args);

        WafReport wafReport = new WafReport();
        wafReport.makeReport("2022");
    }


}
