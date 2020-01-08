package org.jboss.pnc.logprocessor.eventduration;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * @author Ales Justin
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {

        Options options = new Options();
        options.addOption("k",true, "Kafka.properties file location.");
        options.addOption("ti",true, "Input topic name.");
        options.addOption("to",true, "Output topic name.");
        options.addOption("td",true, "Durations only topic name.");
        options.addOption("h", false, "Print this help message.");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse( options, args);

        if (cmd.hasOption("h")) {
            printHelp(options);
            return;
        }

        try {
            String kafkaPropertiesPath = getOption(cmd, "k", null);
            validateNotBlank(kafkaPropertiesPath, "Missing path to kafka.properties file.");

            String inputTopicName = getOption(cmd, "ti", null);
            validateNotBlank(inputTopicName, "Missing input topic name.");

            String outputTopicName = getOption(cmd, "to", null);
            validateNotBlank(inputTopicName, "Missing output topic name.");

            String durationsTopicName = getOption(cmd, "td", null);

            Properties kafkaProperties = new Properties();
            kafkaProperties.load(new FileInputStream(kafkaPropertiesPath));

            Application application = new Application(
                    kafkaProperties,
                    inputTopicName,
                    outputTopicName,
                    java.util.Optional.ofNullable(durationsTopicName));
            Runtime.getRuntime().addShutdownHook(new Thread(application::stop, "Shutdown-Thread"));
            application.start();
            log.info("Running ...");

        } catch (Exception e) {
            System.out.println("Invalid configuration: " + e.getMessage());
            System.out.println("");
            printHelp(options);
        }
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(" ", options);
    }

    private static void validateNotBlank(String value, String errorMessage) throws Exception {
        if (value == null || value.isBlank()) {
            throw new Exception(errorMessage);
        }
    }

    private static String getOption(CommandLine cmd, String opt, String defaultValue) {
        if (cmd.hasOption(opt)) {
            return cmd.getOptionValue(opt);
        } else {
            return defaultValue;
        }
    }
}
