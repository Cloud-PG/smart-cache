# smart-cache
## Use the simulator utilities

### Installation

```bash
cd SmartCache/sim/Utilities
python3 setup.py install
```

### Use the simulator utilities

```bash
python3 -m utils -h
# Example to fast compile
python3 -m utils compile --fast 'true'
# Example to get the simulator executable path
python3 -m utils simPath
```

## Use the Probe module

### Installation

```bash
cd Probe
python3 setup.py install
```

### Open the dashboard

Open the result folder with the following command:

```bash
python3 -m probe.results dashboard result_folder
```
