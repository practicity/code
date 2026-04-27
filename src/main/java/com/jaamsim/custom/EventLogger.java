package com.jaamsim.custom;

import com.jaamsim.basicsim.Entity;
import com.jaamsim.events.EventManager;
import com.jaamsim.events.EventTraceListener;
import com.jaamsim.events.ProcessTarget;
import com.jaamsim.input.StringInput;
import com.jaamsim.input.Keyword;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class EventLogger extends Entity implements EventTraceListener {

    @Keyword(description = "The file to write events to.",
            exampleList = {"'simulation_events.txt'"})
    private final StringInput fileName;

    private PrintWriter writer;
    private long eventCount = 0;
    private long filteredCount = 0;
    private long lastTick = 0;
    private long maxTickSeen = 0;

    private long entitiesGenerated = 0;
    private long entitiesDisposed = 0;
    private long queueAdds = 0;
    private long serverStarts = 0;
    private final Map<String, Long> entityFirstSeen = new HashMap<>();
    private final Map<String, Long> entityCompleted = new HashMap<>();
    private double totalTimeInSystem = 0.0;

    private static final String[] SKIP_PREFIXES = {
            "XY-Grid", "XYZ-Axis", "Clock", "Title",
            "Simulation.startUp", "MyEventLogger"
    };
    private static final String[] SKIP_CONTAINS = {
            ".Label.", "DisplayEntity", "ColladaModel",
            "OverlayClock", "OverlayText", "View1"
    };

    {
        fileName = new StringInput("FileName", "Key Inputs", "simulation_events.txt");
        this.addInput(fileName);
    }

    public EventLogger() {}

    @Override
    public void earlyInit() {
        super.earlyInit();
        eventCount = 0;
        filteredCount = 0;
        lastTick = 0;
        maxTickSeen = 0;
        entitiesGenerated = 0;
        entitiesDisposed = 0;
        queueAdds = 0;
        serverStarts = 0;
        totalTimeInSystem = 0.0;
        entityFirstSeen.clear();
        entityCompleted.clear();

        try {
            writer = new PrintWriter(new FileWriter(new File(fileName.getValue())));
            writer.println("╔═══════════════════════════════════════════════════════════════════════════════════╗");
            writer.println("║                         JAAMSIM EVENT TRACE LOG                                 ║");
            writer.println("╠═══════════════════════════════════════════════════════════════════════════════════╣");
            writer.println();
            writer.printf("%-8s %-10s %-5s %-45s %-12s%n",
                    "#", "Time(s)", "Pri", "Description", "State");
            writer.println("─".repeat(85));
            writer.flush();

            EventManager.current().setTraceListener(this);

        } catch (Exception e) {
            System.err.println("EventLogger: Could not open file: " + e.getMessage());
        }
    }

    private boolean shouldSkip(String desc) {
        for (String prefix : SKIP_PREFIXES) {
            if (desc.startsWith(prefix)) return true;
        }
        for (String contains : SKIP_CONTAINS) {
            if (desc.contains(contains)) return true;
        }
        return false;
    }

    private String formatTime(long tick) {
        double sec = tick / 1.0e6;
        return String.format("%.1f", sec);
    }

    private void trackStatistics(String desc, String state, long tick) {
        if (desc.contains("EntityGenerator") && state.equals("Event")
                && desc.contains("endStep")) {
            entitiesGenerated++;
        }
        if (desc.contains("Queue") && desc.contains("UpdateAllQueueUsers")) {
            queueAdds++;
        }
        if (desc.contains("Server") && desc.contains("endStep")
                && state.equals("Event")) {
            serverStarts++;
        }
        if (desc.contains("EntityGenerator") && desc.contains("endStep")
                && state.equals("Event")) {
            String entityName = "Entity_" + entitiesGenerated;
            entityFirstSeen.put(entityName, tick);
        }
        if (desc.contains("Server") && desc.contains("endStep")
                && state.equals("Event")) {
            String entityName = "Entity_" + serverStarts;
            if (entityFirstSeen.containsKey(entityName)) {
                long birthTick = entityFirstSeen.get(entityName);
                double timeInSystem = (tick - birthTick) / 1.0e6;
                totalTimeInSystem += timeInSystem;
                entitiesDisposed++;
                entityCompleted.put(entityName, tick);
            }
        }
    }

    private void log(long tick, int priority, String desc, String state) {
        if (writer == null) return;
        if (shouldSkip(desc)) return;

        filteredCount++;
        eventCount++;

        // Track max tick for proper delta and separators
        if (tick > maxTickSeen) {
            // Time has advanced - print separator
            if (maxTickSeen > 0) {
                long delta = tick - maxTickSeen;
                writer.printf("  ·········· [ +%s s ] ··········%n", formatTime(delta));
            }
            maxTickSeen = tick;
        }

        writer.printf("%-8d %-10s %-5d %-45s %-12s%n",
                filteredCount, formatTime(tick), priority, desc, state);
        writer.flush();

        trackStatistics(desc, state, tick);
        lastTick = tick;
    }

    @Override
    public void traceEvent(long tick, int priority, ProcessTarget t) {
        log(tick, priority, t.getDescription(), "Event");
    }

    @Override
    public void traceWait(long tick, int priority, ProcessTarget t) {
        log(tick, priority, t.getDescription(), "Wait");
    }

    @Override
    public void traceSchedProcess(long tick, int priority, ProcessTarget t) {
        log(tick, priority, t.getDescription(), "Scheduled");
    }

    @Override
    public void traceProcessStart(ProcessTarget t) {
        if (shouldSkip(t.getDescription())) return;
        if (writer == null) return;
        filteredCount++;
        eventCount++;
        writer.printf("%-8d %-10s %-5s %-45s %-12s%n",
                filteredCount, "", "", t.getDescription(), "▶ Start");
        writer.flush();
    }

    @Override
    public void traceProcessEnd() {
        eventCount++;
    }

    @Override
    public void traceInterrupt(long tick, int priority, ProcessTarget t) {
        log(tick, priority, t.getDescription(), "⚡ Interrupt");
    }

    @Override
    public void traceKill(long tick, int priority, ProcessTarget t) {
        log(tick, priority, t.getDescription(), "✖ Kill");
    }

    @Override
    public void traceWaitUntil() {
        eventCount++;
    }

    @Override
    public void traceSchedUntil(ProcessTarget t) {
        if (shouldSkip(t.getDescription())) return;
        log(0, 0, t.getDescription(), "SchedUntil");
    }

    @Override
    public void traceConditionalEval(ProcessTarget t) {
        eventCount++;
    }

    @Override
    public void traceConditionalEvalEnded(boolean wakeup, ProcessTarget t) {
        if (wakeup && !shouldSkip(t.getDescription())) {
            log(0, 0, t.getDescription(), "CondWake");
        }
        eventCount++;
    }

    @Override
    public void kill() {
        if (writer != null) {
            writer.println();
            writer.println("─".repeat(85));
            writer.println();
            writer.println("╔═══════════════════════════════════════════════════════════════════════════════════╗");
            writer.println("║                              SIMULATION SUMMARY                                 ║");
            writer.println("╠═══════════════════════════════════════════════════════════════════════════════════╣");
            writer.println();
            writer.printf("  Total raw events:          %,d%n", eventCount);
            writer.printf("  Filtered events logged:    %,d%n", filteredCount);
            writer.printf("  Events filtered out:       %,d%n", eventCount - filteredCount);
            writer.println();
            writer.println("  ── Entity Flow ──");
            writer.printf("  Entities generated:        %,d%n", entitiesGenerated);
            writer.printf("  Queue additions:           %,d%n", queueAdds);
            writer.printf("  Server completions:        %,d%n", serverStarts);
            writer.printf("  Entities completed:        %,d%n", entitiesDisposed);
            writer.println();

            if (entitiesDisposed > 0) {
                double avgTime = totalTimeInSystem / entitiesDisposed;
                writer.println("  ── Timing ──");
                writer.printf("  Avg time in system:        %s s%n", formatTime((long)(avgTime * 1e6)));
                writer.printf("  Total entity-seconds:      %s s%n", formatTime((long)(totalTimeInSystem * 1e6)));
            }

            double simDurationSec = maxTickSeen / 1.0e6;
            writer.println();
            writer.printf("  Simulation duration:       %s s (%.2f hr)%n",
                    formatTime(maxTickSeen), simDurationSec / 3600.0);
            writer.println();
            writer.println("╚═══════════════════════════════════════════════════════════════════════════════════╝");
            writer.close();
        }
        super.kill();
    }
}
