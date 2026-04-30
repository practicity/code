// File: src/main/java/com/jaamsim/custom/KafkaCommandListener.java
package com.jaamsim.custom;

import com.jaamsim.Graphics.DisplayEntity;
import com.jaamsim.BasicObjects.ToggleButton;
import com.jaamsim.basicsim.Entity;
import com.jaamsim.basicsim.JaamSimModel;
import com.jaamsim.input.Input;
import com.jaamsim.input.InputAgent;
import com.jaamsim.input.Keyword;
import com.jaamsim.input.StringInput;
import com.jaamsim.math.Vec3d;
import com.jaamsim.ui.GUIFrame;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import javax.swing.SwingUtilities;

public class KafkaCommandListener extends DisplayEntity {

    // =========================================================================
    // INPUTS
    // =========================================================================

    @Keyword(description = "Kafka bootstrap servers (host:port).",
            exampleList = {"localhost:9092"})
    private final StringInput bootstrapServers;

    @Keyword(description = "Kafka topic to consume commands from.",
            exampleList = {"jaamsim-commands"})
    private final StringInput topic;

    @Keyword(description = "Kafka topic to publish responses/events to.",
            exampleList = {"jaamsim-events"})
    private final StringInput responseTopic;

    @Keyword(description = "Kafka consumer group ID.",
            exampleList = {"jaamsim-controller"})
    private final StringInput groupId;

    @Keyword(description = "Poll interval in seconds for checking Kafka.",
            exampleList = {"1.0"})
    private final StringInput pollInterval;

    // =========================================================================
    // RUNTIME STATE
    // =========================================================================

    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;
    private volatile boolean running = false;
    private Thread consumerThread;

    // =========================================================================
    // INPUT REGISTRATION
    // =========================================================================

    {
        bootstrapServers = new StringInput("BootstrapServers", KEY_INPUTS, "localhost:9092");
        this.addInput(bootstrapServers);

        topic = new StringInput("Topic", KEY_INPUTS, "jaamsim-commands");
        this.addInput(topic);

        responseTopic = new StringInput("ResponseTopic", KEY_INPUTS, "jaamsim-events");
        this.addInput(responseTopic);

        groupId = new StringInput("GroupId", KEY_INPUTS, "jaamsim-controller");
        this.addInput(groupId);

        pollInterval = new StringInput("PollInterval", KEY_INPUTS, "1.0");
        this.addInput(pollInterval);

        // Deferred start: wait for inputs to be loaded from cfg, then start Kafka
        Thread deferredStart = new Thread(() -> {
            try {
                Thread.sleep(5000); // wait for cfg to finish loading
                if (!running
                        && bootstrapServers.getValue() != null && !bootstrapServers.getValue().isEmpty()
                        && topic.getValue() != null && !topic.getValue().isEmpty()
                        && groupId.getValue() != null && !groupId.getValue().isEmpty()) {
                    initConsumer();
                    initProducer();
                    running = true;
                    startConsumerThread();
                    logInfo("Kafka listener started (deferred)");
                }
            } catch (Exception e) {
                logError("Deferred start failed: %s", e.getMessage());
            }
        }, "KafkaCommandListener-DeferredStart");
        deferredStart.setDaemon(true);
        deferredStart.start();
    }

    // =========================================================================
    // LIFECYCLE
    // =========================================================================

    @Override
    public void earlyInit() {
        super.earlyInit();

        // If already running (started from updateForInput), skip
        if (running) return;

        try {
            initConsumer();
            initProducer();
            running = true;
            startConsumerThread();
            logInfo("Started - listening on topic '%s', responding on '%s'",
                    topic.getValue(), responseTopic.getValue());
        } catch (Exception e) {
            logError("Failed to start Kafka consumer: %s", e.getMessage());
        }
    }

    @Override
    public void startUp() {
        super.startUp();
        // Nothing here now - Kafka already running from earlyInit
    }

    // =========================================================================
    // KAFKA INIT
    // =========================================================================

    private void initConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.getValue());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId.getValue());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic.getValue()));
    }

    private void initProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.getValue());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(props);
    }

    // =========================================================================
    // CONSUMER THREAD
    // =========================================================================

    private void startConsumerThread() {
        consumerThread = new Thread(() -> {
            try {
                while (running) {
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            processCommand(record.value());
                        } catch (Exception e) {
                            logError("Failed to process command: %s - %s",
                                    record.value(), e.getMessage());
                            sendResponse("error", e.getMessage());
                        }
                    }
                }
            } catch (Exception e) {
                if (running) {
                    logError("Consumer thread error: %s", e.getMessage());
                }
            }
        }, "KafkaCommandListener-" + getName());
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    private void stopConsumerThread() {
        if (consumerThread != null) {
            try {
                consumerThread.join(3000);
            } catch (InterruptedException ignored) {}
            consumerThread = null;
        }
    }

    // =========================================================================
    // COMMAND PROCESSING
    // =========================================================================

    /**
     * Processes a JSON command string.
     *
     * Supported commands:
     *
     *   TOGGLE A BUTTON:
     *   {"entity":"ToggleButton1", "action":"TOGGLE"}
     *
     *   SET A BUTTON STATE:
     *   {"entity":"ToggleButton1", "action":"SET", "value":true}
     *
     *   EDIT ANY ENTITY INPUT:
     *   {"entity":"Server1", "action":"EDIT", "input":"ServiceTime", "value":"5 s"}
     *
     *   SIMULATION CONTROL:
     *   {"action":"SIM_START"}
     *   {"action":"SIM_PAUSE"}
     *   {"action":"SIM_STOP"}
     *   {"action":"SIM_RESUME"}
     *
     *   QUERY AN ENTITY:
     *   {"entity":"Server1", "action":"QUERY", "input":"ServiceTime"}
     *
     *   QUERY SIMULATION TIME:
     *   {"action":"QUERY_TIME"}
     */
    private void processCommand(String json) {
        String action = extractJsonString(json, "action");

        if (action == null || action.isEmpty()) {
            logWarn("Command missing 'action' field: %s", json);
            sendResponse("error", "Missing 'action' field");
            return;
        }

        logInfo("Received command: %s", json);

        switch (action.toUpperCase()) {

            // --- Simulation control (no entity needed) ---
            case "SIM_START":
                handleSimControl("START");
                break;
            case "SIM_PAUSE":
                handleSimControl("PAUSE");
                break;
            case "SIM_STOP":
                handleSimControl("STOP");
                break;
            case "SIM_RESUME":
                handleSimControl("RESUME");
                break;
            case "QUERY_TIME":
                handleQueryTime();
                break;

            // --- Entity-based commands ---
            default:
                processEntityCommand(json, action);
                break;
        }
    }

    private void processEntityCommand(String json, String action) {
        String entityName = extractJsonString(json, "entity");

        if (entityName == null || entityName.isEmpty()) {
            logWarn("Command missing 'entity' field: %s", json);
            sendResponse("error", "Missing 'entity' field");
            return;
        }

        Entity ent = getJaamSimModel().getNamedEntity(entityName);
        if (ent == null) {
            logWarn("Entity not found: '%s'", entityName);
            sendResponse("error", "Entity not found: " + entityName);
            return;
        }

        switch (action.toUpperCase()) {
            case "TOGGLE":
                handleToggle(ent, entityName);
                break;
            case "SET":
                boolean desired = extractJsonBoolean(json, "value");
                handleSet(ent, entityName, desired);
                break;
            case "EDIT":
                String inputName = extractJsonString(json, "input");
                String inputValue = extractJsonString(json, "value");
                handleEdit(ent, entityName, inputName, inputValue);
                break;
            case "QUERY":
                String queryInput = extractJsonString(json, "input");
                handleQuery(ent, entityName, queryInput);
                break;
            default:
                logWarn("Unknown action: '%s'", action);
                sendResponse("error", "Unknown action: " + action);
        }
    }

    // =========================================================================
    // ACTION HANDLERS
    // =========================================================================

    private void handleToggle(Entity ent, String entityName) {
        if (!(ent instanceof ToggleButton)) {
            logWarn("Entity '%s' is not a ToggleButton (type: %s)",
                    entityName, ent.getClass().getSimpleName());
            sendResponse("error", entityName + " is not a ToggleButton");
            return;
        }
        ToggleButton btn = (ToggleButton) ent;
        btn.handleMouseClicked((short) 1, new Vec3d(), false, false, false);

        boolean state = btn.isPressed(0);
        logInfo("Toggled '%s' -> %s", entityName, state ? "PRESSED" : "UNPRESSED");
        sendResponse("toggled",
                String.format("{\"entity\":\"%s\",\"state\":%b}", entityName, state));
    }

    private void handleSet(Entity ent, String entityName, boolean desired) {
        if (!(ent instanceof ToggleButton)) {
            logWarn("Entity '%s' is not a ToggleButton (type: %s)",
                    entityName, ent.getClass().getSimpleName());
            sendResponse("error", entityName + " is not a ToggleButton");
            return;
        }
        ToggleButton btn = (ToggleButton) ent;
        boolean current = btn.isPressed(0);

        if (current == desired) {
            logInfo("Entity '%s' already %s", entityName, desired ? "PRESSED" : "UNPRESSED");
            sendResponse("already_set",
                    String.format("{\"entity\":\"%s\",\"state\":%b}", entityName, desired));
            return;
        }

        btn.handleMouseClicked((short) 1, new Vec3d(), false, false, false);
        logInfo("Set '%s' -> %s", entityName, desired ? "PRESSED" : "UNPRESSED");
        sendResponse("set",
                String.format("{\"entity\":\"%s\",\"state\":%b}", entityName, desired));
    }

    private void handleEdit(Entity ent, String entityName, String inputName, String inputValue) {
        if (inputName == null || inputName.isEmpty()) {
            logWarn("EDIT command missing 'input' field");
            sendResponse("error", "Missing 'input' field for EDIT");
            return;
        }
        if (inputValue == null) {
            logWarn("EDIT command missing 'value' field");
            sendResponse("error", "Missing 'value' field for EDIT");
            return;
        }

        try {
            InputAgent.applyArgs(ent, inputName, inputValue);
            logInfo("Edited '%s'.%s = '%s'", entityName, inputName, inputValue);
            sendResponse("edited",
                    String.format("{\"entity\":\"%s\",\"input\":\"%s\",\"value\":\"%s\"}",
                            entityName, inputName, inputValue));
        } catch (Exception e) {
            logError("Failed to edit '%s'.%s: %s", entityName, inputName, e.getMessage());
            sendResponse("error",
                    String.format("Failed to edit %s.%s: %s", entityName, inputName, e.getMessage()));
        }
    }

    private void handleQuery(Entity ent, String entityName, String inputName) {
        if (inputName == null || inputName.isEmpty()) {
            logWarn("QUERY command missing 'input' field");
            sendResponse("error", "Missing 'input' field for QUERY");
            return;
        }

        Input<?> inp = ent.getInput(inputName);
        if (inp == null) {
            logWarn("Input '%s' not found on entity '%s'", inputName, entityName);
            sendResponse("error",
                    String.format("Input '%s' not found on '%s'", inputName, entityName));
            return;
        }

        String value = inp.getValueString();
        logInfo("Query '%s'.%s = '%s'", entityName, inputName, value);
        sendResponse("query_result",
                String.format("{\"entity\":\"%s\",\"input\":\"%s\",\"value\":\"%s\"}",
                        entityName, inputName, value));
    }

    private void handleSimControl(String control) {
        try {
            GUIFrame gui = GUIFrame.getInstance();
            if (gui == null) {
                logWarn("GUIFrame not available");
                sendResponse("error", "GUIFrame not available");
                return;
            }

            switch (control) {
                case "START":
                case "RESUME":
                    SwingUtilities.invokeLater(() -> {
                        JaamSimModel sim = GUIFrame.getJaamSimModel();
                        if (!sim.isRunning()) {
                            gui.invokeRunPause();
                        }
                    });
                    logInfo("Simulation %s requested", control);
                    sendResponse("sim_control", control);
                    break;

                case "PAUSE":
                    SwingUtilities.invokeLater(() -> {
                        JaamSimModel sim = GUIFrame.getJaamSimModel();
                        if (sim.isRunning()) {
                            gui.invokeRunPause();
                        }
                    });
                    logInfo("Simulation PAUSED");
                    sendResponse("sim_control", "PAUSED");
                    break;

                case "STOP":
                    SwingUtilities.invokeLater(() -> {
                        try {
                            JaamSimModel sim = GUIFrame.getJaamSimModel();
                            if (!sim.isRunning()) {
                                sim.resume();
                            }
                            GUIFrame.getInstance().stopSimulation();
                        } catch (Exception e) {
                            logError("Failed to stop simulation: %s", e.getMessage());
                        }
                    });
                    logInfo("Simulation STOPPED");
                    sendResponse("sim_control", "STOPPED");
                    break;

                default:
                    logWarn("Unknown control command: %s", control);
                    sendResponse("error", "Unknown control: " + control);
                    break;
            }
        } catch (Exception e) {
            logError("Sim control '%s' failed: %s", control, e.getMessage());
            sendResponse("error", "Sim control failed: " + e.getMessage());
        }
    }

    private void handleQueryTime() {
        try {
            double simTime = getSimTime();
            logInfo("Current sim time: %f", simTime);
            sendResponse("sim_time",
                    String.format("{\"simTime\":%f}", simTime));
        } catch (Exception e) {
            logError("Query time failed: %s", e.getMessage());
            sendResponse("error", "Query time failed: " + e.getMessage());
        }
    }

    // =========================================================================
    // KAFKA RESPONSE PRODUCER
    // =========================================================================

    private void sendResponse(String type, String payload) {
        if (producer == null || responseTopic.getValue().isEmpty()) return;
        try {
            String msg = String.format("{\"type\":\"%s\",\"payload\":%s}",
                    type, payload.startsWith("{") ? payload : "\"" + payload + "\"");
            producer.send(new ProducerRecord<>(responseTopic.getValue(), type, msg));
            producer.flush();
        } catch (Exception e) {
            logError("Failed to send response: %s", e.getMessage());
        }
    }

    // =========================================================================
    // MINIMAL JSON PARSING
    // =========================================================================

    private String extractJsonString(String json, String key) {
        String pattern = "\"" + key + "\"";
        int idx = json.indexOf(pattern);
        if (idx < 0) return null;

        int colonIdx = json.indexOf(':', idx + pattern.length());
        if (colonIdx < 0) return null;

        int startQuote = json.indexOf('"', colonIdx + 1);
        if (startQuote < 0) return null;

        int endQuote = json.indexOf('"', startQuote + 1);
        if (endQuote < 0) return null;

        return json.substring(startQuote + 1, endQuote);
    }

    private boolean extractJsonBoolean(String json, String key) {
        String pattern = "\"" + key + "\"";
        int idx = json.indexOf(pattern);
        if (idx < 0) return false;

        int colonIdx = json.indexOf(':', idx + pattern.length());
        if (colonIdx < 0) return false;

        String rest = json.substring(colonIdx + 1).trim();
        return rest.startsWith("true");
    }

    // =========================================================================
    // LOGGING
    // =========================================================================

    private void logInfo(String format, Object... args) {
        System.out.println("[KafkaCommandListener:" + getName() + "] INFO "
                + String.format(format, args));
    }

    private void logWarn(String format, Object... args) {
        System.err.println("[KafkaCommandListener:" + getName() + "] WARN "
                + String.format(format, args));
    }

    private void logError(String format, Object... args) {
        System.err.println("[KafkaCommandListener:" + getName() + "] ERROR "
                + String.format(format, args));
    }

    // =========================================================================
    // CLEANUP
    // =========================================================================

    private void closeConsumer() {
        if (consumer != null) {
            try { consumer.close(); } catch (Exception ignored) {}
            consumer = null;
        }
    }

    private void closeProducer() {
        if (producer != null) {
            try { producer.close(); } catch (Exception ignored) {}
            producer = null;
        }
    }

    @Override
    public void kill() {
        running = false;
        closeConsumer();
        closeProducer();
        stopConsumerThread();
        super.kill();
    }
}
