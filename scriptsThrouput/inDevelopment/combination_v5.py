import subprocess
import itertools
import json
import os
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from datetime import datetime

#beachte die dest_folder anzupassen
#beachte --directory anzupassen 

# Eingaben des Benutzers
operation = input("Welche Operation: read, write, randread, randwrite: ").strip().lower()
userinput_bs = input("Gib mehrere Blocksizes ein, getrennt durch Leerzeichen: ")
userinput_numjobs = input("Gib verschiedene Numjobs-Werte ein, getrennt durch Leerzeichen: ")
userinput_iodepth = input("Gib verschiedene Iodepth-Werte ein, getrennt durch Leerzeichen: ")

bs = userinput_bs.split()
numjobs = userinput_numjobs.split()
iodepth = userinput_iodepth.split()

test_combinations = list(itertools.product(bs, numjobs, iodepth))

timestamp = datetime.now().strftime("%Y%m%d")
dest_folder = f"/root/fio_benchmark/justusresults/combination_results_{timestamp}"
os.makedirs(dest_folder, exist_ok=True)

all_results = {"read": [], "write": []} if operation == "all" else {operation: []}


# hier ist noch Potential zum Optimieren
def run_fio(bs, numjobs, iodepth):
    result_file = f"{dest_folder}/result_{bs}_{numjobs}_{iodepth}.json"
    fio_cmd = (
        f"fio --rw={operation if operation != 'all' else 'rw'} --ioengine=sync --filesize=4m:6m --nrfiles=10 --bs={bs} "
        f"--numjobs={numjobs} --iodepth={iodepth} --unlink=1 "
        f"--directory=/mnt/draidBenchmark/testbereichjustus --group_reporting=1 "
        f"--time_based=1 --runtime=30s --name=test_python --output-format=json "
        f"--output={result_file}"
    )
    subprocess.run(fio_cmd, shell=True)
    return result_file

# testdurchführung
for (bs, numjobs, iodepth) in test_combinations:
    result_file = run_fio(bs, numjobs, iodepth)
    with open(result_file, 'r') as f:
        result_data = json.load(f)
        if operation == "all":
            all_results["read"].append(result_data)
            all_results["write"].append(result_data)
        else:
            all_results[operation].append(result_data)

# Speichern aller Ergebnisse mit Zeitstempel
total_results_file = f"{dest_folder}/all_results_{operation}_{timestamp}.json"
with open(total_results_file, "w") as f:
    json.dump(all_results, f, indent=4)
print(f"Ergebnisse gespeichert in: {total_results_file}")

# Verarbeitung und Visualisierung
def parse_fio_output(json_data, op):
    read_bandwidths, write_bandwidths = [], []
    block_sizes, numjobs, iodepths = [], [], []

    def parse_bs(bs_str):
        size, unit = int(bs_str[:-1]), bs_str[-1].lower()
        return size * (1024 ** {'k': 1, 'm': 2, 'g': 3}.get(unit, 0))

    for entry in json_data.get(op, []):
        job = entry["jobs"][0]  # Erstes Job-Ergebnis

        # Read & Write Bandbreite auslesen
        read_bw = job.get("read", {}).get("bw_bytes", 0) / (1024 * 1024)  # MB/s
        write_bw = job.get("write", {}).get("bw_bytes", 0) / (1024 * 1024)  # MB/s

        # FIO-Global-Optionen auslesen
        global_opts = entry.get("global options", {})
        block_size_bytes = parse_bs(global_opts.get("bs", "4k"))
        numjob = int(global_opts.get("numjobs", 1))
        iodepth = int(global_opts.get("iodepth", 1))

        read_bandwidths.append(read_bw)
        write_bandwidths.append(write_bw)
        block_sizes.append(block_size_bytes)
        numjobs.append(numjob)
        iodepths.append(iodepth)

    return (np.array(read_bandwidths), np.array(write_bandwidths), 
            np.array(block_sizes), np.array(numjobs), np.array(iodepths))

with open(total_results_file, 'r') as f:
    json_data = json.load(f)

if operation == "all":
    read_bw, write_bw, block_sizes, numjobs, iodepths = parse_fio_output(json_data, "read")
    _, write_bw, _, _, _ = parse_fio_output(json_data, "write")  # Write-Bandbreite aktualisieren
else:
    read_bw, write_bw, block_sizes, numjobs, iodepths = parse_fio_output(json_data, operation)

# DataFrame für CSV
df = pd.DataFrame({
    'Block Size (Bytes)': block_sizes,
    'Num Jobs': numjobs,
    'IO Depth': iodepths,
    'Read Bandwidth (MB/s)': read_bw if read_bw.size > 0 else None,
    'Write Bandwidth (MB/s)': write_bw if write_bw.size > 0 else None
})

# CSV-Datei speichern
csv_file_path = f"{dest_folder}/fio_benchmark_{operation}_{timestamp}.csv"
df.to_csv(csv_file_path, index=False)
print(f"CSV-Datei gespeichert unter: {csv_file_path}")

# Balkendiagramm mit korrekter X-Achsen-Beschriftung
def plot_bar_chart(numjobs, block_sizes, iodepths, read_bandwidths, write_bandwidths):
    if len(numjobs) == 0 or len(block_sizes) == 0:
        print("Nicht genug Daten für das Diagramm. PNG-Datei wird nicht gespeichert.")
        return

    # Daten für das Diagramm vorbereiten
    data = []
    for i in range(len(numjobs)):
        data.append({
            'Label': f"NJ: {int(numjobs[i])}, BS: {block_sizes[i]}B, IO-D: {int(iodepths[i])}",  
            'Read Bandwidth (MB/s)': read_bandwidths[i] if read_bandwidths.size > 0 else 0,
            'Write Bandwidth (MB/s)': write_bandwidths[i] if write_bandwidths.size > 0 else 0
        })

    # In DataFrame konvertieren
    df_plot = pd.DataFrame(data)

    # Balkendiagramm erstellen
    fig, ax = plt.subplots(figsize=(20, 8))
    df_melted = df_plot.melt(id_vars=["Label"], 
                         value_vars=["Read Bandwidth (MB/s)", "Write Bandwidth (MB/s)"],
                         var_name="Operation", value_name="Bandwidth (MB/s)")

    # Balkendiagramm mit seaborn
    sns.barplot(data=df_melted, x="Label", y="Bandwidth (MB/s)", hue="Operation", dodge=True, ax=ax)

    # Achsentitel setzen
    ax.set_title(f"FIO Benchmark: {operation} Ergebnisse", fontsize=14)
    ax.set_xlabel("NumJobs, BlockSize, IO Depth", fontsize=12)
    ax.set_ylabel("Bandwidth (MB/s)", fontsize=12)
    ax.legend(title="Operation")

    # X-Achsen-Labels drehen + mehr Platz unten
    plt.xticks(rotation=45, fontsize=10, ha="right")
    plt.subplots_adjust(bottom=0.3)  # Mehr Platz für Labels
    plt.tight_layout()  

    # Diagramm speichern
    img_path = f"{dest_folder}/fio_benchmark_{operation}_{timestamp}_chart.png"
    plt.savefig(img_path, dpi=300)
    plt.close()

    print(f"Balkendiagramm gespeichert unter: {img_path}")

# Diagramm erstellen (falls Daten vorhanden sind)
plot_bar_chart(numjobs, block_sizes, iodepths, read_bw, write_bw)
