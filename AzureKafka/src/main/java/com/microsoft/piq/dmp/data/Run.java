package com.microsoft.piq.dmp.data;

import org.apache.commons.lang3.StringUtils;
import java.io.IOException;
import java.util.UUID;

public class Run {
    public static void main(String[] args) throws IOException {
        // Get the brokers
        String brokers = StringUtils.EMPTY;
        String topicName = StringUtils.EMPTY;
        String groupId = UUID.randomUUID().toString();

        if(args.length < 2) {
            usage();
        } else if (args.length == 2) {
            brokers = args[1];
        } else if (args.length == 3) {
            brokers = args[1];
            topicName = args[2];
        } else {
            brokers = args[1];
            topicName = args[2];
            groupId = args[3];
        }

        switch(args[0].toLowerCase()) {
            case "producer":
                Producer.produce(brokers, topicName);
                break;
            case "consumer":
                Consumer.consume(brokers, groupId, topicName);
                break;
            case "describe":
                AdminClientWrapper.describeTopics(brokers, topicName);
                break;
            case "create":
                AdminClientWrapper.createTopics(brokers, topicName);
                break;
            case "delete":
                AdminClientWrapper.deleteTopics(brokers, topicName);
                break;
            case "list":
                AdminClientWrapper.listTopics(brokers);
                break;
            default:
                usage();
        }
        System.exit(0);
    }
    // Display usage
    public static void usage() {
        System.out.println("Usage:");
        System.out.println("AzureKafka.jar <producer|consumer|describe|create|delete|list> brokerhosts [topicName] [groupid]");
        System.exit(1);
    }
}
