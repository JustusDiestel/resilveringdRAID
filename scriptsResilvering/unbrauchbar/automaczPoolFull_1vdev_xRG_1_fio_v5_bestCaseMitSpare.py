import subprocess
import time
from datetime import datetime

POOL_NAME = "mypool"
MOUNTPOINT = "/mnt/draidBenchmark"
FILL_LEVELS = [0]  # Bei Bedarf anpassen

def run_cmd(cmd, check=True):
    """Führt ein Shell-Kommando aus und gibt die Ausgabe zurück."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"[FEHLER] Befehl fehlgeschlagen: {cmd}")
        print(result.stderr)
        raise Exception("Fehler bei Kommandoausführung")
    return result.stdout.strip()

def get_valid_disk_paths():
    """Sucht alle gültigen Disks (LUNs) für dRAID."""
    cmd = r'''
for dev in /dev/sd*; do
    [[ "$dev" =~ [0-9] ]] && continue
    id=$(smartctl -i "$dev" 2>/dev/null | grep 'Logical Unit id' | awk '{print $4}')
    if [[ ${#id} -eq 18 ]]; then
        for prefix in /dev/disk/by-id/wwn-* /dev/disk/by-id/scsi-*; do
            if [[ -e "$prefix" ]] && [[ "$(readlink -f "$prefix")" == "$(readlink -f "$dev")" ]]; then
                echo "$prefix"
                break
            fi
        done
    fi
done
    '''
    result = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True)
    if result.returncode != 0:
        print("Fehler beim Abrufen der Disk-Pfade:")
        print(result.stderr)
        return []
    lines = result.stdout.strip().splitlines()
    print(f"[INFO] {len(lines)} gültige Disks gefunden.")
    return lines

def generate_rg_configs(dev_paths):
    """Erzeugt dRAID-Konfigurationen für Test."""
    total_disks = len(dev_paths)
    configs = []

    children = total_disks
    spares = 1  # Best Case: Spare vorhanden
    parity = 2
    min_data = 1
    max_data = children - spares - parity

    for data in range(min_data, max_data + 1):
        if (children - parity - spares) % data != 0:
            continue

        vdev_config = f"draid2:{data}d:{spares}s:{children}c"
        vdev_devs = " ".join(dev_paths)

        zpool_cmd = (
            f"zpool create -f -m {MOUNTPOINT} -o ashift=12 {POOL_NAME} \\\n"
            f"  {vdev_config} {vdev_devs}"
        )

        configs.append({
            "vdevs": 1,
            "children": children,
            "spares": spares,
            "parity": parity,
            "data": data,
            "zfs_syntax": vdev_config,
            "zpool_create_cmd": zpool_cmd,
            "used_disks": dev_paths
        })

    configs.sort(key=lambda x: x["data"])
    return configs

def create_pool(pool_cmd):
    """Erstellt ZFS-Pool."""
    print("[INFO] Erstelle Pool...")
    run_cmd(pool_cmd)
    print("[INFO] Deaktiviere Kompression...")
    run_cmd(f"zfs set compression=off {POOL_NAME}")

def fill_pool(level, num_vdevs):
    """Füllt den Pool mit Dummy-Daten via fio."""
    print(f"[INFO] Fülle Pool zu {int(level * 100)}% mit fio...")

    output = run_cmd(f"zfs list -Hp -o available {POOL_NAME}")
    available_bytes = int(output.strip())
    fill_size_bytes = int(available_bytes * level)
    fill_size_gib = fill_size_bytes // (1024 ** 3)

    print(f"[INFO] Zielgröße gesamt: {fill_size_gib} GiB")

    if fill_size_gib == 0:
        print("[INFO] Kein Füllbedarf, überspringe fio.")
        return

    per_file_gib = max(1, fill_size_gib // num_vdevs)
    filenames = [f"{MOUNTPOINT}/fillfile_{i}" for i in range(num_vdevs)]
    fio_filename_str = ":".join(filenames)

    fio_cmd = (
        f"fio --name=filljob "
        f"--rw=write "
        f"--bs=2M "
        f"--numjobs={num_vdevs} "
        f"--iodepth=64 "
        f"--size={per_file_gib}G "
        f"--filename={fio_filename_str} "
        f"--ioengine=libaio "
        f"--group_reporting"
    )

    print(f"[INFO] Starte fio mit {num_vdevs} Jobs, je {per_file_gib} GiB...")
    process = subprocess.Popen(fio_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        for line in process.stdout:
            print(line.strip())
        process.wait()
    except KeyboardInterrupt:
        process.kill()
        print("[ABBRUCH] Füllen wurde manuell abgebrochen.")
        raise

    if process.returncode != 0:
        raise Exception("fio ist mit Fehlern beendet.")

def clear_fill():
    """Löscht Dummy-Dateien."""
    print("[INFO] Entferne Dummy-Dateien...")
    run_cmd(f"rm -f {MOUNTPOINT}/fillfile_*", check=False)

def simulate_resilver(pool_name, used_disks):
    """Simuliert einen Disk-Ausfall und misst Resilver-Zeit."""
    failed_path = used_disks[0]

    print(f"[INFO] Nehme Disk offline (simulierter Ausfall): {failed_path}")
    run_cmd(f"zpool offline {pool_name} {failed_path}")
    time.sleep(0.5)

    print("[INFO] Warte auf Resilvering in Spare-Felder...")
    start_time = time.time()
    while True:
        status = run_cmd(f"zpool status {pool_name}", check=False)
        if "resilver" in status and "in progress" in status:
            time.sleep(2)
        else:
            break
    end_time = time.time()
    duration = end_time - start_time

    print(f"[INFO] Pool ist nun DEGRADED, Resilver abgeschlossen.")
    return duration, status

def delete_pool(pool_name):
    """Löscht Pool und beendet Prozesse."""
    print("[INFO] Lösche Pool...")
    run_cmd("pkill -9 fio", check=False)
    run_cmd(f"fuser -k {MOUNTPOINT}", check=False)
    run_cmd(f"umount -f {MOUNTPOINT}", check=False)
    run_cmd(f"zpool destroy {pool_name}", check=False)

def main():
    dev_paths = get_valid_disk_paths()
    if len(dev_paths) < 5:
        print("[FEHLER] Nicht genug gültige Disks gefunden!")
        return

    configs = generate_rg_configs(dev_paths)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    logfile = f"resilver_bestcase_Fill:{FILL_LEVELS[0]}_{timestamp}.log"

    for i, cfg in enumerate(configs):
        print(f"\n[CONFIG {i+1}/{len(configs)}] {cfg['zfs_syntax']}")
        for level in FILL_LEVELS:
            try:
                print(f"\n[TEST] {int(level*100)}% Füllstand")
                create_pool(cfg["zpool_create_cmd"])
                fill_pool(level, cfg["vdevs"])
                duration, status = simulate_resilver(POOL_NAME, cfg["used_disks"])
                clear_fill()
                delete_pool(POOL_NAME)

                with open(logfile, "a") as f:
                    f.write(f"--- Konfiguration: {cfg['zfs_syntax']} | Fill: {int(level*100)}% ---\n")
                    f.write(f"VDEVs: {cfg['vdevs']}, Data: {cfg['data']}, Children: {cfg['children']}\n")
                    f.write(f"Resilver-Zeit: {duration:.2f} Sekunden\n")
                    f.write(status + "\n\n")

            except Exception as e:
                print(f"[FEHLER] Test fehlgeschlagen: {e}")
                try:
                    clear_fill()
                    delete_pool(POOL_NAME)
                except:
                    pass
                continue

    print(f"\n✅ Alle Tests abgeschlossen. Ergebnisse gespeichert in: {logfile}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[ABBRUCH] Manuell gestoppt. Aufräumen...")
        try:
            clear_fill()
            delete_pool(POOL_NAME)
        except:
            print("[WARNUNG] Konnte nicht sauber aufräumen.")
