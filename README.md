**JaamSim Remote Control --- User Manual**

## What It Does

This Python script connects to your JaamSim simulation through **Kafka**. It lets you **control the simulation** and **modify entities in real-time** from outside JaamSim --- no need to touch the JaamSim GUI.

Your Java plugin (KafkaCommandListener) runs inside JaamSim and listens for commands on the Kafka topic jaamsim-commands. This Python script **sends** commands to that topic.

## Requirements

- Kafka running on localhost:9092

- JaamSim running with the Java Kafka plugin loaded

- Python with kafka-python installed (pip install kafka-python)

## Starting Up

python jaamsim_control.py

You\'ll see this menu:

============================================================

JaamSim Remote Control

============================================================

\-\-- Toggle Buttons \-\--

1 = Toggle ToggleButton1

2 = Toggle ToggleButton2

\-\-- Simulation Control \-\--

s = Start/Resume simulation

p = Pause simulation

x = Stop simulation

\-\-- Entity Control \-\--

e = Edit any entity input

qe = Query any entity input

q = Quit

## Commands

### 1 --- Toggle ToggleButton1 {#toggle-togglebutton1}

Flips ToggleButton1 between ON and OFF. If it\'s currently ON, it goes OFF, and vice versa.

Enter command: 1

-\> Sent: {\'entity\': \'ToggleButton1\', \'action\': \'TOGGLE\'}

**Use case:** ToggleButton1 might control a branch in your model --- e.g., enabling/disabling a conveyor path.

### 2 --- Toggle ToggleButton2 {#toggle-togglebutton2}

Same as above but for ToggleButton2.

Enter command: 2

-\> Sent: {\'entity\': \'ToggleButton2\', \'action\': \'TOGGLE\'}

### s --- Start / Resume Simulation {#s-start-resume-simulation}

Starts the simulation if it\'s stopped, or resumes it if it\'s paused.

Enter command: s

-\> Sent: {\'action\': \'SIM_START\'}

### p --- Pause Simulation {#p-pause-simulation}

Pauses the simulation. The sim time freezes but everything stays in place. Use s to resume.

Enter command: p

-\> Sent: {\'action\': \'SIM_PAUSE\'}

### x --- Stop Simulation {#x-stop-simulation}

Fully stops the simulation and resets it. You\'d need to press s to start from the beginning.

Enter command: x

-\> Sent: {\'action\': \'SIM_STOP\'}

### e --- Edit an Entity Input {#e-edit-an-entity-input}

This is the most powerful command. It lets you **change any input on any entity** in the running model.

**How it works:**

1.  You type the exact entity name (as it appears in JaamSim)

2.  You type the exact input/keyword name

3.  You type the new value

**Example 1 --- Change a server\'s processing time:**

Enter command: e

Entity name: Server1

Input name: ServiceTime

New value: 5 s

This changes Server1\'s service time to 5 seconds.

**Example 2 --- Change an entity\'s position:**

Enter command: e

Entity name: Server1

Input name: Position

New value: 1.0 2.0 0.0 m

**Example 3 --- Change a generator\'s arrival rate:**

Enter command: e

Entity name: EntityGenerator1

Input name: InterArrivalTime

New value: 10 s

**Example 4 --- Change a queue\'s max size:**

Enter command: e

Entity name: Queue1

Input name: MaxValidLength

New value: 20

**Tips:**

- Entity names and input names are **case-sensitive** --- they must match exactly what\'s in JaamSim

- Values must be in the same format JaamSim expects (including units like s, m, h)

- To find the exact input names, click on an entity in JaamSim and look at the **Input Editor** panel

### qe --- Query an Entity Input {#qe-query-an-entity-input}

Sends a request to read an entity\'s current input value.

Enter command: qe

Entity name: Server1

Input name: ServiceTime

-\> Sent: {\'entity\': \'Server1\', \'action\': \'QUERY\', \'input\': \'ServiceTime\'}

**⚠️ Important limitation:** The response is sent back on the jaamsim-events Kafka topic, but this script **does not listen** to that topic. So you won\'t see the answer in this script. The result will appear in JaamSim\'s console/log output instead.

### q --- Quit {#q-quit}

Closes the Kafka connection and exits the script.

Enter command: q

Bye!

## Typical Workflow

1.  **Open JaamSim** with your model and the Kafka plugin loaded

2.  **Start Kafka** (zookeeper + kafka-server-start)

3.  **Run the Python script**

4.  Press s to **start the simulation**

5.  Press 1 or 2 to **toggle buttons** as needed

6.  Use e to **tweak parameters** while the sim runs

7.  Press p to **pause** and inspect, s to **resume**

8.  Press x to **stop** and reset

9.  Press q to **exit**

## Troubleshooting

| **Problem**                                 | **Solution**                                                          |
|---------------------------------------------|-----------------------------------------------------------------------|
| NoBrokersAvailable                          | Kafka isn\'t running --- start Zookeeper and Kafka first              |
| Command sent but nothing happens in JaamSim | Make sure the Java plugin is loaded and listening on jaamsim-commands |
| e command doesn\'t change anything          | Double-check entity name and input name --- they\'re case-sensitive   |
| Unknown entity error in JaamSim logs        | The entity name you typed doesn\'t exist in the model                 |
