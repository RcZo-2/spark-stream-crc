package org.example.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Supplier;

public class ConfigManager {

    private static final Logger logger = LoggerFactory.getLogger(ConfigManager.class);

    private static final Supplier<Config> configSupplier = ConfigManager::loadConfig;

    private static volatile Config configInstance = null;

    private ConfigManager() {
    }

    public static Config getConfig() {
        if (configInstance == null) {
            synchronized (ConfigManager.class) {
                if (configInstance == null) {
                    configInstance = configSupplier.get();
                }
            }
        }
        return configInstance;
    }

    private static Config loadConfig() {
        String environment = Optional.ofNullable(System.getProperty("ENV")).orElse("default");
        String configFile = String.format("config/application-%s.conf", environment);

        Config baseConfig = ConfigFactory.load();
        Config envConfig = ConfigFactory.parseResources(configFile);
        logger.info("Successfully load config");
        return baseConfig.withFallback(envConfig).resolve();
    }
}
