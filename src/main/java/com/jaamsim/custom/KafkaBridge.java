package com.jaamsim.custom;

import com.jaamsim.Graphics.DisplayEntity;
import com.jaamsim.basicsim.Entity;
import com.jaamsim.input.StringInput;
import com.jaamsim.input.EntityListInput;
import com.jaamsim.events.ProcessTarget;
import com.jaamsim.states.StateEntity;
import com.jaamsim.ProcessFlow.EntityGenerator;
import com.jaamsim.ProcessFlow.Server;
import com.jaamsim.ProcessFlow.EntitySink;
import com.jaamsim.ProcessFlow.Queue;
import com.jaamsim.ProcessFlow.LinkedComponent;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaBridge extends DisplayEntity {

    // =========================================================================
    // INPUTS
    // =========================================================================

    private final StringInput bootstrapServers;
    private final StringInput topic;
    private final StringInput pollInterval;
    private final StringInput producerTimeout;
    private final StringInput acksConfig;
    private final StringInput retriesConfig;
    private final StringInput lingerMs;
    private final StringInput batchSize;
    private final StringInput maxLogErrors;
    private final StringInput monitorAll;
    private final StringInput serverWorkingState;
    private final StringInput excludeNamePrefix;
    private final StringInput excludeNameSuffix;
    private final EntityListInput<DisplayEntity> entitiesToMonitor;

    // =========================================================================
    // RUNTIME STATE
    // =========================================================================

    private KafkaProducer<String, String> producer;
    private volatile boolean running = false;
    private PollTarget pollTarget;
    private int sendErrorCount = 0;
    private List<DisplayEntity> resolvedEntities;

    // =========================================================================
    // INPUT REGISTRATION
    // =========================================================================

    {
        bootstrapServers = new StringInput("BootstrapServers", KEY_INPUTS, "localhost:9092");
        bootstrapServers.setRequired(true);
        this.addInput(bootstrapServers);

        topic = new StringInput("KafkaTopic", KEY_INPUTS, "jaamsim-events");
        topic.setRequired(true);
        this.addInput(topic);

        pollInterval = new StringInput("PollInterval", KEY_INPUTS, "1.0");
        this.addInput(pollInterval);

        producerTimeout = new StringInput("ProducerTimeout", KEY_INPUTS, "5000");
        this.addInput(producerTimeout);

        monitorAll = new StringInput("MonitorAll", KEY_INPUTS, "TRUE");
        this.addInput(monitorAll);

        entitiesToMonitor = new EntityListInput<>(DisplayEntity.class, "EntitiesToMonitor",
                KEY_INPUTS, new ArrayList<>());
        entitiesToMonitor.setRequired(false);
        this.addInput(entitiesToMonitor);

        acksConfig = new StringInput("Acks", KEY_INPUTS, "1");
        this.addInput(acksConfig);

        retriesConfig = new StringInput("Retries", KEY_INPUTS, "2");
        this.addInput(retriesConfig);

        lingerMs = new StringInput("LingerMs", KEY_INPUTS, "100");
        this.addInput(lingerMs);

        batchSize = new StringInput("BatchSize", KEY_INPUTS, "16384");
        this.addInput(batchSize);

        maxLogErrors = new StringInput("MaxLogErrors", KEY_INPUTS, "10");
        this.addInput(maxLogErrors);

        // These replace hardcoded logic — now fully configurable
        serverWorkingState = new StringInput("ServerWorkingState", KEY_INPUTS, "Working");
        this.addInput(serverWorkingState);

        excludeNamePrefix = new StringInput("ExcludeNamePrefix", KEY_INPUTS, "_");
        this.addInput(excludeNamePrefix);

        excludeNameSuffix = new StringInput("ExcludeNameSuffix", KEY_INPUTS, "Proto");
        this.addInput(excludeNameSuffix);
    }

    // =========================================================================
    // LIFECYCLE
    // =========================================================================

    @Override
    public void earlyInit() {
        super.earlyInit();
        running = false;
        sendErrorCount = 0;
        resolvedEntities = null;
        closeProducer();
    }

    @Override
    public void startUp() {
        super.startUp();

        try {
            producer = createProducer();
        } catch (Exception e) {
            logError("Failed to create Kafka producer: %s", e.getMessage());
            return;
        }

        resolvedEntities = resolveEntitiesToMonitor();
        logInfo("Monitoring %d entities (MonitorAll=%s)", resolvedEntities.size(), monitorAll.getValue());
        for (DisplayEntity ent : resolvedEntities) {
            logInfo("  -> %s [%s]", ent.getName(), ent.getClass().getSimpleName());
        }

        running = true;
        pollTarget = new PollTarget();

        sendSimulationEvent("SIMULATION_STARTED");
        scheduleProcess(getPollIntervalSeconds(), 5, pollTarget);
    }

    // =========================================================================
    // ENTITY RESOLUTION
    // =========================================================================

    private boolean isMonitorAll() {
        String val = monitorAll.getValue();
        return val != null && val.trim().equalsIgnoreCase("TRUE");
    }

    private List<DisplayEntity> resolveEntitiesToMonitor() {
        List<DisplayEntity> result = new ArrayList<>();

        if (isMonitorAll()) {
            result.addAll(discoverEntities(EntityGenerator.class));
            result.addAll(discoverEntities(Server.class));
            result.addAll(discoverEntities(Queue.class));
            result.addAll(discoverEntities(EntitySink.class));

            // Pick up any other LinkedComponent subclasses not already captured
            for (LinkedComponent lc : getJaamSimModel().getClonesOfIterator(LinkedComponent.class)) {
                if (!result.contains(lc) && shouldMonitor(lc)) {
                    result.add(lc);
                }
            }
        } else {
            if (entitiesToMonitor.getValue() != null) {
                for (DisplayEntity ent : entitiesToMonitor.getValue()) {
                    if (ent != null && ent != this) {
                        result.add(ent);
                    }
                }
            }
        }

        return result;
    }

    private <T extends Entity> List<DisplayEntity> discoverEntities(Class<T> clazz) {
        List<DisplayEntity> result = new ArrayList<>();
        for (T ent : getJaamSimModel().getClonesOfIterator(clazz)) {
            if (ent instanceof DisplayEntity && shouldMonitor((DisplayEntity) ent)) {
                result.add((DisplayEntity) ent);
            }
        }
        return result;
    }

    /**
     * Returns false for:
     * - The bridge itself
     * - Entities whose name starts with ExcludeNamePrefix (default: "_")
     * - Entities whose name ends with ExcludeNameSuffix (default: "Proto")
     */
    private boolean shouldMonitor(DisplayEntity ent) {
        if (ent == null || ent == this) return false;

        String name = ent.getName();
        if (name == null || name.isEmpty()) return false;

        String prefix = excludeNamePrefix.getValue();
        String suffix = excludeNameSuffix.getValue();

        if (prefix != null && !prefix.isEmpty() && name.startsWith(prefix)) return false;
        if (suffix != null && !suffix.isEmpty() && name.endsWith(suffix)) return false;

        return true;
    }

    // =========================================================================
    // PRODUCER
    // =========================================================================

    private KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.getValue());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, producerTimeout.getValue());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, producerTimeout.getValue());
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                String.valueOf(Integer.parseInt(producerTimeout.getValue()) * 2));

        props.put(ProducerConfig.ACKS_CONFIG, acksConfig.getValue());
        props.put(ProducerConfig.RETRIES_CONFIG, retriesConfig.getValue());
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs.getValue());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize.getValue());

        return new KafkaProducer<>(props);
    }

    private double getPollIntervalSeconds() {
        try {
            double val = Double.parseDouble(pollInterval.getValue());
            if (val <= 0) {
                logWarn("PollInterval must be positive, defaulting to 1.0");
                return 1.0;
            }
            return val;
        } catch (NumberFormatException e) {
            logWarn("Invalid PollInterval '%s', defaulting to 1.0", pollInterval.getValue());
            return 1.0;
        }
    }

    private int getMaxLogErrors() {
        try {
            return Integer.parseInt(maxLogErrors.getValue());
        } catch (NumberFormatException e) {
            return 10;
        }
    }

    // =========================================================================
    // POLLING
    // =========================================================================

    private class PollTarget extends ProcessTarget {

        @Override
        public String getDescription() {
            return KafkaBridge.this.getName() + "-Poll";
        }

        @Override
        public void process() {
            if (!running || producer == null) return;

            double simTime = getSimTime();
            sendTickEvent(simTime);

            if (resolvedEntities != null) {
                for (DisplayEntity entity : resolvedEntities) {
                    try {
                        String json = buildEntityJson(entity, simTime);
                        sendToKafka(entity.getName(), json);
                    } catch (Exception e) {
                        logWarn("Error building JSON for %s: %s", entity.getName(), e.getMessage());
                    }
                }
            }

            scheduleProcess(getPollIntervalSeconds(), 5, pollTarget);
        }
    }

    // =========================================================================
    // KAFKA SEND
    // =========================================================================

    private final Callback sendCallback = new Callback() {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                sendErrorCount++;
                if (sendErrorCount <= getMaxLogErrors()) {
                    logWarn("Kafka send failed (%d): %s", sendErrorCount, exception.getMessage());
                }
                if (sendErrorCount == getMaxLogErrors()) {
                    logWarn("Max log errors reached (%d). Suppressing further Kafka send errors.", getMaxLogErrors());
                }
            }
        }
    };

    private void sendToKafka(String key, String value) {
        if (producer == null) return;
        try {
            producer.send(new ProducerRecord<>(topic.getValue(), key, value), sendCallback);
        } catch (Exception e) {
            sendErrorCount++;
            if (sendErrorCount <= getMaxLogErrors()) {
                logWarn("Kafka send exception: %s", e.getMessage());
            }
        }
    }

    // =========================================================================
    // JSON BUILDERS
    // =========================================================================

    private String buildEntityJson(DisplayEntity entity, double simTime) {
        StringBuilder sb = new StringBuilder(256);
        sb.append("{");

        appendString(sb, "entity", entity.getName());
        appendString(sb, "class", entity.getClass().getSimpleName());
        appendTime(sb, "simTime", simTime);
        appendLong(sb, "timestamp", System.currentTimeMillis());

        if (entity instanceof EntityGenerator) {
            buildGeneratorJson(sb, (EntityGenerator) entity, simTime);
        } else if (entity instanceof Queue) {
            buildQueueJson(sb, (Queue) entity, simTime);
        } else if (entity instanceof Server) {
            buildServerJson(sb, (Server) entity, simTime);
        } else if (entity instanceof EntitySink) {
            buildSinkJson(sb, (EntitySink) entity, simTime);
        } else if (entity instanceof LinkedComponent) {
            buildLinkedComponentJson(sb, (LinkedComponent) entity, simTime);
        }

        removeTrailingComma(sb);
        sb.append("}");
        return sb.toString();
    }

    private void buildGeneratorJson(StringBuilder sb, EntityGenerator gen, double simTime) {
        appendStateSafe(sb, gen, simTime);
        try {
            long generated = gen.getNumberProcessed(simTime);
            appendLong(sb, "generated", generated);
            if (simTime > 0) {
                appendDecimal(sb, "rate", (double) generated / simTime);
            }
        } catch (Exception e) {
            logMetricError("EntityGenerator.generated", gen.getName(), e);
        }
    }

    private void buildQueueJson(StringBuilder sb, Queue queue, double simTime) {
        try { appendLong(sb, "length", (long) queue.getQueueLength(simTime)); }
        catch (Exception e) { logMetricError("Queue.length", queue.getName(), e); }

        try { appendDecimal(sb, "avgLength", queue.getQueueLengthAverage(simTime)); }
        catch (Exception e) { logMetricError("Queue.avgLength", queue.getName(), e); }

        try { appendLong(sb, "maxLength", (long) queue.getQueueLengthMaximum(simTime)); }
        catch (Exception e) { logMetricError("Queue.maxLength", queue.getName(), e); }

        try { appendLong(sb, "minLength", (long) queue.getQueueLengthMinimum(simTime)); }
        catch (Exception e) { logMetricError("Queue.minLength", queue.getName(), e); }

        try { appendLong(sb, "numberAdded", queue.getNumberAdded(simTime)); }
        catch (Exception e) { logMetricError("Queue.numberAdded", queue.getName(), e); }

        try { appendLong(sb, "numberProcessed", queue.getNumberProcessed(simTime)); }
        catch (Exception e) { logMetricError("Queue.numberProcessed", queue.getName(), e); }

        try { appendLong(sb, "queuePosition", queue.getQueuePosition(simTime)); }
        catch (Exception e) { logMetricError("Queue.queuePosition", queue.getName(), e); }

        try { appendTime(sb, "avgWait", queue.getAverageQueueTime(simTime)); }
        catch (Exception e) { logMetricError("Queue.avgWait", queue.getName(), e); }

        try { appendLong(sb, "numberReneged", queue.getNumberReneged(simTime)); }
        catch (Exception e) { logMetricError("Queue.numberReneged", queue.getName(), e); }
    }

    private void buildServerJson(StringBuilder sb, Server server, double simTime) {
        appendStateSafe(sb, server, simTime);

        try {
            long processed = server.getNumberProcessed(simTime);
            appendLong(sb, "processed", processed);
            if (simTime > 0) {
                appendDecimal(sb, "throughput", (double) processed / simTime);
            }
        } catch (Exception e) {
            logMetricError("Server.processed", server.getName(), e);
        }

        try {
            double totalTime = Math.max(simTime, 0.001);
            double workingTime = 0;

            // Try the configured state name first, then fall back to common alternatives
            String configuredState = serverWorkingState.getValue();
            List<String> statesToTry = new ArrayList<>();
            if (configuredState != null && !configuredState.isEmpty()) {
                statesToTry.add(configuredState);
            }
            // Add fallbacks only if they are not already the configured value
            for (String fallback : Arrays.asList("Working", "working", "Processing", "Busy")) {
                if (!fallback.equals(configuredState)) {
                    statesToTry.add(fallback);
                }
            }

            for (String stateName : statesToTry) {
                try {
                    workingTime = server.getTimeInState(simTime, stateName);
                    if (workingTime > 0) break;
                } catch (Exception ignored) {}
            }

            appendDecimal(sb, "utilization", workingTime / totalTime);
            appendTime(sb, "workingTime", workingTime);
            appendTime(sb, "idleTime", totalTime - workingTime);
        } catch (Exception e) {
            logMetricError("Server.utilization", server.getName(), e);
        }

        try {
            appendLong(sb, "numberInProgress", server.getNumberInProgress(simTime));
        } catch (Exception e) {
            logMetricError("Server.numberInProgress", server.getName(), e);
        }
    }

    private void buildSinkJson(StringBuilder sb, EntitySink sink, double simTime) {
        try {
            long received = sink.getNumberProcessed(simTime);
            appendLong(sb, "received", received);
            if (simTime > 0) {
                appendDecimal(sb, "throughput", (double) received / simTime);
            }
        } catch (Exception e) {
            logMetricError("EntitySink.received", sink.getName(), e);
        }
    }

    private void buildLinkedComponentJson(StringBuilder sb, LinkedComponent lc, double simTime) {
        appendStateSafe(sb, lc, simTime);
        try {
            appendLong(sb, "numberProcessed", lc.getNumberProcessed(simTime));
        } catch (Exception e) {
            logMetricError("LinkedComponent.numberProcessed", lc.getName(), e);
        }
    }

    // =========================================================================
    // STATE HELPER
    // =========================================================================

    private void appendStateSafe(StringBuilder sb, DisplayEntity entity, double simTime) {
        try {
            if (entity instanceof StateEntity) {
                String state = ((StateEntity) entity).getPresentState(simTime);
                if (state != null && !state.isEmpty()) {
                    appendString(sb, "state", state);
                }
            }
        } catch (Exception e) {
            // Entity does not support state tracking — skip silently
        }
    }

    // =========================================================================
    // SIMULATION EVENTS
    // =========================================================================

    private void sendTickEvent(double simTime) {
        StringBuilder sb = new StringBuilder(128);
        sb.append("{");
        appendString(sb, "eventType", "TICK");
        appendTime(sb, "simTime", simTime);
        appendLong(sb, "timestamp", System.currentTimeMillis());
        removeTrailingComma(sb);
        sb.append("}");
        sendToKafka("tick", sb.toString());
    }

    private void sendSimulationEvent(String eventType) {
        StringBuilder sb = new StringBuilder(256);
        sb.append("{");
        appendString(sb, "eventType", eventType);
        appendTime(sb, "simTime", getSimTime());
        appendLong(sb, "timestamp", System.currentTimeMillis());
        appendString(sb, "bridge", getName());

        if (resolvedEntities != null && !resolvedEntities.isEmpty()) {
            sb.append("\"monitoredEntities\":[");
            boolean first = true;
            for (DisplayEntity ent : resolvedEntities) {
                if (!first) sb.append(",");
                sb.append("\"").append(escapeJson(ent.getName())).append("\"");
                first = false;
            }
            sb.append("],");
        }

        removeTrailingComma(sb);
        sb.append("}");
        sendToKafka("simulation", sb.toString());
    }

    // =========================================================================
    // JSON HELPERS
    // =========================================================================

    private void appendString(StringBuilder sb, String key, String value) {
        sb.append("\"").append(key).append("\":\"").append(escapeJson(value)).append("\",");
    }

    private void appendTime(StringBuilder sb, String key, double value) {
        sb.append("\"").append(key).append("\":").append(String.format("%.1f", value)).append(",");
    }

    private void appendDecimal(StringBuilder sb, String key, double value) {
        sb.append("\"").append(key).append("\":").append(String.format("%.4f", value)).append(",");
    }

    private void appendLong(StringBuilder sb, String key, long value) {
        sb.append("\"").append(key).append("\":").append(value).append(",");
    }

    private String escapeJson(String text) {
        if (text == null) return "";
        return text.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private void removeTrailingComma(StringBuilder sb) {
        if (sb.length() > 0 && sb.charAt(sb.length() - 1) == ',') {
            sb.deleteCharAt(sb.length() - 1);
        }
    }

    // =========================================================================
    // LOGGING
    // =========================================================================

    private void logMetricError(String metric, String entityName, Exception e) {
        logWarn("Failed to read %s for %s: %s", metric, entityName, e.getMessage());
    }

    private void logInfo(String format, Object... args) {
        System.out.println("[KafkaBridge:" + getName() + "] INFO " + String.format(format, args));
    }

    private void logWarn(String format, Object... args) {
        System.err.println("[KafkaBridge:" + getName() + "] WARN " + String.format(format, args));
    }

    private void logError(String format, Object... args) {
        System.err.println("[KafkaBridge:" + getName() + "] ERROR " + String.format(format, args));
    }

    // =========================================================================
    // CLEANUP
    // =========================================================================

    private void closeProducer() {
        if (producer != null) {
            try {
                producer.close();
            } catch (Exception ignored) {}
            producer = null;
        }
    }

    @Override
    public void kill() {
        running = false;
        if (producer != null) {
            try {
                sendSimulationEvent("SIMULATION_ENDED");
                producer.flush();
            } catch (Exception ignored) {}
            closeProducer();
        }
        super.kill();
    }
}
