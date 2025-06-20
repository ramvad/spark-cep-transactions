package com.example.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FraudPatternConfig {
    
    private static final Logger LOG = LoggerFactory.getLogger(FraudPatternConfig.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private JsonNode config;
    private String configFilePath;
    private long lastModified;
    
    public FraudPatternConfig(String configFilePath) {
        this.configFilePath = configFilePath;
        loadConfiguration();
    }
    
    /**
     * Load configuration from JSON file with hot-reload capability
     */
    public void loadConfiguration() {
        try {
            File configFile = new File(configFilePath);
            
            // Check if file has been modified (hot-reload)
            if (config != null && configFile.lastModified() <= lastModified) {
                return; // No changes
            }
            
            config = objectMapper.readTree(configFile);
            lastModified = configFile.lastModified();
            
            LOG.info("Fraud pattern configuration loaded from: {}", configFilePath);
            LOG.info("Configuration version: {}", getConfigVersion());
            
        } catch (IOException e) {
            LOG.error("Failed to load fraud pattern configuration", e);
            throw new RuntimeException("Cannot load fraud pattern configuration", e);
        }
    }
    
    /**
     * Get all enabled fraud patterns
     */
    public List<FraudPattern> getEnabledPatterns() {
        List<FraudPattern> patterns = new ArrayList<>();
        
        JsonNode patternsNode = config.path("fraudPatterns").path("patterns");
        for (JsonNode patternNode : patternsNode) {
            if (patternNode.path("enabled").asBoolean(true)) {
                patterns.add(createFraudPattern(patternNode));
            }
        }
        
        // Sort by priority
        patterns.sort(Comparator.comparingInt(FraudPattern::getPriority));
        
        return patterns;
    }
    
    /**
     * Get specific pattern by ID
     */
    public Optional<FraudPattern> getPattern(String patternId) {
        JsonNode patternsNode = config.path("fraudPatterns").path("patterns");
        for (JsonNode patternNode : patternsNode) {
            if (patternId.equals(patternNode.path("id").asText())) {
                return Optional.of(createFraudPattern(patternNode));
            }
        }
        return Optional.empty();
    }
    
    /**
     * Create FraudPattern object from JSON node
     */
    private FraudPattern createFraudPattern(JsonNode patternNode) {
        FraudPattern pattern = new FraudPattern();
        pattern.setId(patternNode.path("id").asText());
        pattern.setName(patternNode.path("name").asText());
        pattern.setDescription(patternNode.path("description").asText());
        pattern.setEnabled(patternNode.path("enabled").asBoolean(true));
        pattern.setPriority(patternNode.path("priority").asInt(1));
        
        // Parse parameters
        Map<String, Object> parameters = new HashMap<>();
        JsonNode parametersNode = patternNode.path("parameters");
        parametersNode.fields().forEachRemaining(entry -> {
            JsonNode value = entry.getValue();
            if (value.isDouble()) {
                parameters.put(entry.getKey(), value.asDouble());
            } else if (value.isInt()) {
                parameters.put(entry.getKey(), value.asInt());
            } else if (value.isBoolean()) {
                parameters.put(entry.getKey(), value.asBoolean());
            } else if (value.isArray()) {
                List<String> list = new ArrayList<>();
                value.forEach(item -> list.add(item.asText()));
                parameters.put(entry.getKey(), list);
            } else {
                parameters.put(entry.getKey(), value.asText());
            }
        });
        pattern.setParameters(parameters);
        
        // Set SQL condition and alert message
        String sqlCondition = patternNode.path("sqlCondition").asText();
        String alertMessage = patternNode.path("alertMessage").asText();
        
        pattern.setSqlCondition(interpolateParameters(sqlCondition, parameters));
        pattern.setAlertMessage(alertMessage);
        
        return pattern;
    }
    
    /**
     * Replace parameter placeholders in SQL conditions
     */
    private String interpolateParameters(String template, Map<String, Object> parameters) {
        String result = template;
        Pattern pattern = Pattern.compile("\\$\\{([^}]+)\\}");
        Matcher matcher = pattern.matcher(template);
        
        while (matcher.find()) {
            String paramName = matcher.group(1);
            Object paramValue = parameters.get(paramName);
            
            if (paramValue != null) {
                String replacement;
                if (paramValue instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<String> list = (List<String>) paramValue;
                    replacement = "(" + String.join(",", 
                        list.stream().map(s -> "'" + s + "'").toArray(String[]::new)) + ")";
                } else {
                    replacement = paramValue.toString();
                }
                result = result.replace("${" + paramName + "}", replacement);
            }
        }
        
        return result;
    }
    
    /**
     * Get alert configuration
     */
    public AlertConfiguration getAlertConfiguration() {
        JsonNode alertConfig = config.path("alertConfiguration");
        AlertConfiguration alertConf = new AlertConfiguration();
        
        alertConf.setEmailNotifications(alertConfig.path("emailNotifications").asBoolean(true));
        alertConf.setSmsNotifications(alertConfig.path("smsNotifications").asBoolean(false));
        alertConf.setWebhookUrl(alertConfig.path("webhookUrl").asText());
        alertConf.setBatchSize(alertConfig.path("batchSize").asInt(100));
        
        return alertConf;
    }
    
    /**
     * Get data configuration
     */
    public Map<String, String> getDataConfiguration() {
        Map<String, String> dataConfig = new HashMap<>();
        JsonNode dataNode = config.path("dataConfiguration");
        
        dataConfig.put("inputPath", dataNode.path("inputPath").asText("transactions.csv"));
        dataConfig.put("outputPath", dataNode.path("outputPath").asText("fraud_alerts"));
        dataConfig.put("checkpointPath", dataNode.path("checkpointPath").asText("checkpoints"));
        dataConfig.put("logLevel", dataNode.path("logLevel").asText("INFO"));
        
        return dataConfig;
    }
    
    public String getConfigVersion() {
        return config.path("fraudPatterns").path("version").asText("unknown");
    }
    
    public String getLastUpdated() {
        return config.path("fraudPatterns").path("lastUpdated").asText();
    }
    
    /**
     * Reload configuration if file has changed
     */
    public boolean checkAndReload() {
        File configFile = new File(configFilePath);
        if (configFile.lastModified() > lastModified) {
            LOG.info("Configuration file changed, reloading...");
            loadConfiguration();
            return true;
        }
        return false;
    }
    
    // Inner classes for configuration objects
    public static class FraudPattern {
        private String id;
        private String name;
        private String description;
        private boolean enabled;
        private int priority;
        private Map<String, Object> parameters = new HashMap<>();
        private String sqlCondition;
        private String alertMessage;
        
        // Getters and setters
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        
        public int getPriority() { return priority; }
        public void setPriority(int priority) { this.priority = priority; }
        
        public Map<String, Object> getParameters() { return parameters; }
        public void setParameters(Map<String, Object> parameters) { this.parameters = parameters; }
        
        public String getSqlCondition() { return sqlCondition; }
        public void setSqlCondition(String sqlCondition) { this.sqlCondition = sqlCondition; }
        
        public String getAlertMessage() { return alertMessage; }
        public void setAlertMessage(String alertMessage) { this.alertMessage = alertMessage; }
        
        @Override
        public String toString() {
            return String.format("FraudPattern{id='%s', name='%s', enabled=%s, priority=%d}", 
                id, name, enabled, priority);
        }
    }
    
    public static class AlertConfiguration {
        private boolean emailNotifications;
        private boolean smsNotifications;
        private String webhookUrl;
        private int batchSize;
        
        // Getters and setters
        public boolean isEmailNotifications() { return emailNotifications; }
        public void setEmailNotifications(boolean emailNotifications) { this.emailNotifications = emailNotifications; }
        
        public boolean isSmsNotifications() { return smsNotifications; }
        public void setSmsNotifications(boolean smsNotifications) { this.smsNotifications = smsNotifications; }
        
        public String getWebhookUrl() { return webhookUrl; }
        public void setWebhookUrl(String webhookUrl) { this.webhookUrl = webhookUrl; }
        
        public int getBatchSize() { return batchSize; }
        public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
    }
}
